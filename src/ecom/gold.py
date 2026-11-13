"""Gold load: dimensions rebuilt, facts reloaded for the orders that changed since the last run."""
from __future__ import annotations

from pyspark.sql import functions as F

from ecom.silver import read_watermark, set_watermark

ORDERS_GROUP = "gold.orders_group"      # fact_orders + fact_order_items + fact_payments move together
REVIEWS = "gold.fact_reviews"
GROUP_SOURCES = ["orders", "order_items", "payments"]

FAR = "TIMESTAMP'9999-12-31 00:00:00'"
OLD = "TIMESTAMP'1900-01-01 00:00:00'"


# ---------------------------------------------------------------- what changed
def upper_bound(spark, catalog: str, tables: list[str]) -> str | None:
    """Newest _processed_ts across the given Silver tables, as text. Fixed once at the start of a run,
    so rows that Silver writes while Gold is running are left for the next run."""
    maxes = [spark.table(f"{catalog}.silver.{t}").agg(F.max("_processed_ts")).first()[0] for t in tables]
    maxes = [m for m in maxes if m is not None]
    return str(max(maxes)) if maxes else None


def _in_window(df, lo: str, hi: str):
    return df.where((F.col("_processed_ts") > F.lit(lo).cast("timestamp"))
                    & (F.col("_processed_ts") <= F.lit(hi).cast("timestamp")))


def changed_order_ids(spark, catalog: str, lo: str, hi: str):
    """Orders to reload: changed in Silver inside (lo, hi], plus orders still pointing at an Unknown member."""
    changed = [_in_window(spark.table(f"{catalog}.silver.{t}"), lo, hi).select("order_id")
               for t in GROUP_SOURCES]
    retry_items = (spark.table(f"{catalog}.gold.fact_order_items")
                   .where("customer_key = -1 OR product_key = -1 OR seller_key = -1").select("order_id"))
    retry_orders = spark.table(f"{catalog}.gold.fact_orders").where("customer_key = -1").select("order_id")
    result = changed[0]
    for part in changed[1:] + [retry_items, retry_orders]:
        result = result.union(part)
    return result.where(F.col("order_id").isNotNull()).distinct()


# ---------------------------------------------------------------- SQL (catalog is filled in per run)
def dimension_sql(c: str) -> dict[str, str]:
    return {
        "dim_seller": f"""
            INSERT OVERWRITE {c}.gold.dim_seller
            SELECT xxhash64(seller_id), seller_id, seller_name, city, state, seller_since, seller_rating
            FROM {c}.silver.sellers
            UNION ALL SELECT -1, 'UNKNOWN', 'Unknown', NULL, NULL, NULL, NULL""",
        "dim_product": f"""
            INSERT OVERWRITE {c}.gold.dim_product
            SELECT xxhash64(product_id, CAST(valid_from AS STRING)), product_id, product_name, category, brand,
                   unit_price, cost_price, is_active, seller_id, valid_from, valid_to, valid_to = {FAR}
            FROM (SELECT *,
                    CASE WHEN row_number() OVER (PARTITION BY product_id ORDER BY updated_at) = 1
                         THEN {OLD} ELSE updated_at END AS valid_from,
                    coalesce(lead(updated_at) OVER (PARTITION BY product_id ORDER BY updated_at), {FAR}) AS valid_to
                  FROM {c}.silver.products)
            UNION ALL SELECT -1, 'UNKNOWN', 'Unknown', NULL, NULL, NULL, NULL, NULL, NULL, {OLD}, {FAR}, true""",
        "dim_customer": f"""
            INSERT OVERWRITE {c}.gold.dim_customer
            SELECT xxhash64(customer_id, CAST(valid_from AS STRING)), customer_id, customer_name, email, phone,
                   city, state, signup_date, valid_from, valid_to, valid_to = {FAR}
            FROM (SELECT *,
                    CASE WHEN row_number() OVER (PARTITION BY customer_id ORDER BY updated_at) = 1
                         THEN {OLD} ELSE updated_at END AS valid_from,
                    coalesce(lead(updated_at) OVER (PARTITION BY customer_id ORDER BY updated_at), {FAR}) AS valid_to
                  FROM {c}.silver.customers)
            UNION ALL SELECT -1, 'UNKNOWN', 'Unknown', NULL, NULL, NULL, NULL, NULL, {OLD}, {FAR}, true""",
    }


def fact_sql(c: str) -> dict[str, str]:
    ch = f"(SELECT order_id FROM {c}.control.gold_changed_orders)"
    return {
        "fact_order_items": f"""
            MERGE INTO {c}.gold.fact_order_items t USING (
              SELECT i.order_id, i.order_item_id,
                     CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS date_key,
                     o.order_purchase_ts AS order_ts,
                     coalesce(cu.customer_key, -1) AS customer_key,
                     coalesce(p.product_key, -1)   AS product_key,
                     coalesce(s.seller_key, -1)    AS seller_key,
                     o.order_status, o.channel, i.quantity, i.unit_price, i.discount_amount,
                     i.freight_value, i.line_amount, i.updated_at, current_timestamp() AS _loaded_ts
              FROM {c}.silver.order_items i
              JOIN {c}.silver.orders o ON o.order_id = i.order_id
              LEFT JOIN {c}.gold.dim_customer cu ON cu.customer_id = o.customer_id
                   AND o.order_purchase_ts >= cu.valid_from AND o.order_purchase_ts < cu.valid_to
              LEFT JOIN {c}.gold.dim_product p ON p.product_id = i.product_id
                   AND o.order_purchase_ts >= p.valid_from AND o.order_purchase_ts < p.valid_to
              LEFT JOIN {c}.gold.dim_seller s ON s.seller_id = i.seller_id
              WHERE i.order_id IN {ch}
            ) s ON t.order_id = s.order_id AND t.order_item_id = s.order_item_id
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""",
        "fact_orders": f"""
            MERGE INTO {c}.gold.fact_orders t USING (
              SELECT o.order_id,
                     CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS date_key,
                     coalesce(cu.customer_key, -1) AS customer_key, o.order_status, o.channel,
                     o.order_purchase_ts AS purchase_ts, o.order_approved_ts AS approved_ts,
                     o.order_shipped_ts AS shipped_ts, o.order_delivered_ts AS delivered_ts,
                     o.estimated_delivery_date,
                     CAST(coalesce(i.items_count, 0) AS BIGINT) AS items_count,
                     CAST(coalesce(i.items_amount, 0) AS DECIMAL(18,2)) AS items_amount,
                     CAST(coalesce(i.freight_amount, 0) AS DECIMAL(18,2)) AS freight_amount,
                     CAST(coalesce(p.paid_amount, 0) AS DECIMAL(18,2)) AS paid_amount,
                     datediff(o.order_delivered_ts, o.order_purchase_ts) AS delivery_days,
                     o.order_delivered_ts IS NOT NULL AS is_delivered,
                     coalesce(to_date(o.order_delivered_ts) > o.estimated_delivery_date, false) AS is_late,
                     o.updated_at, current_timestamp() AS _loaded_ts
              FROM {c}.silver.orders o
              LEFT JOIN {c}.gold.dim_customer cu ON cu.customer_id = o.customer_id
                   AND o.order_purchase_ts >= cu.valid_from AND o.order_purchase_ts < cu.valid_to
              LEFT JOIN (SELECT order_id, sum(quantity) AS items_count, sum(line_amount) AS items_amount,
                                sum(freight_value) AS freight_amount
                         FROM {c}.silver.order_items WHERE order_id IN {ch} GROUP BY order_id) i
                   ON i.order_id = o.order_id
              LEFT JOIN (SELECT order_id, sum(payment_value) AS paid_amount
                         FROM {c}.silver.payments WHERE order_id IN {ch} GROUP BY order_id) p
                   ON p.order_id = o.order_id
              WHERE o.order_id IN {ch}
            ) s ON t.order_id = s.order_id
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""",
        "fact_payments": f"""
            MERGE INTO {c}.gold.fact_payments t USING (
              SELECT p.order_id, p.payment_sequential,
                     CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS date_key,
                     p.payment_type, p.payment_installments, p.payment_value, p.updated_at,
                     current_timestamp() AS _loaded_ts
              FROM {c}.silver.payments p
              LEFT JOIN {c}.silver.orders o ON o.order_id = p.order_id
              WHERE p.order_id IN {ch}
            ) s ON t.order_id = s.order_id AND t.payment_sequential = s.payment_sequential
            WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""",
    }


def reviews_sql(c: str, lo: str, hi: str) -> str:
    return f"""
        MERGE INTO {c}.gold.fact_reviews t USING (
          SELECT review_id, order_id, CAST(date_format(review_ts, 'yyyyMMdd') AS INT) AS date_key,
                 review_score, review_comment IS NOT NULL AS has_comment, review_comment, review_ts,
                 current_timestamp() AS _loaded_ts
          FROM {c}.silver.reviews
          WHERE _processed_ts > TIMESTAMP'{lo}' AND _processed_ts <= TIMESTAMP'{hi}'
        ) s ON t.review_id = s.review_id
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"""


# ---------------------------------------------------------------- run
def _merge(spark, sql: str) -> int:
    return int(spark.sql(sql).first()["num_affected_rows"])


def _version(spark, table: str) -> int:
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])


def run_gold(spark, catalog: str, run_id: str, audit) -> None:
    failures: list[tuple] = []

    def step(name: str, work, rows_read: int | None = None) -> bool:
        """Run one Gold table with an audit row. Returns True on success."""
        table = f"gold.{name}"
        audit.start("gold", table)
        try:
            written = work()
            audit.finish("gold", table, "SUCCESS", rows_read, written, 0, _version(spark, f"{catalog}.{table}"))
            print(f"[{name}] read={rows_read} written={written}")
            return True
        except Exception as exc:  # noqa: BLE001 - one failed table must not stop the others
            audit.finish("gold", table, "FAILED", error=f"{type(exc).__name__}: {exc}")
            failures.append((name, str(exc)[:300]))
            print(f"[{name}] FAILED: {exc}")
            return False

    # 1. Dimensions: rebuilt fully, before the facts so that late dimension rows are found.
    for name, sql in dimension_sql(catalog).items():
        step(name, lambda sql=sql, name=name: (spark.sql(sql), spark.table(f"{catalog}.gold.{name}").count())[1])

    # 2. Orders group: reload every order that changed (or still points to an Unknown member).
    lo = read_watermark(spark, catalog, ORDERS_GROUP)
    hi = max(upper_bound(spark, catalog, GROUP_SOURCES) or lo, lo)
    changed = changed_order_ids(spark, catalog, lo, hi)
    changed.write.mode("overwrite").saveAsTable(f"{catalog}.control.gold_changed_orders")
    n_changed = spark.table(f"{catalog}.control.gold_changed_orders").count()
    print(f"orders to reload: {n_changed} (window {lo} -> {hi})")

    if n_changed:
        results = [step(name, lambda sql=sql: _merge(spark, sql), n_changed)
                   for name, sql in fact_sql(catalog).items()]
        if all(results):
            set_watermark(spark, catalog, ORDERS_GROUP, hi, run_id)

    # 3. Reviews: own watermark.
    r_lo = read_watermark(spark, catalog, REVIEWS)
    r_hi = max(upper_bound(spark, catalog, ["reviews"]) or r_lo, r_lo)
    if r_hi > r_lo:
        if step("fact_reviews", lambda: _merge(spark, reviews_sql(catalog, r_lo, r_hi))):
            set_watermark(spark, catalog, REVIEWS, r_hi, run_id)
    else:
        print("[fact_reviews] nothing new")

    if failures:
        raise RuntimeError(f"Gold failed for {len(failures)} table(s): {failures}")