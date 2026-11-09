"""Silver column specs: how each Bronze column becomes a typed, clean Silver column.

One entry per table. The Silver engine is generic; everything table-specific lives here.
try_to_timestamp gives NULL for a bad value instead of failing; the data quality rules then catch the NULL.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SilverSpec:
    source_table: str
    expressions: dict[str, str]   # Silver column name -> Spark SQL expression over the Bronze columns


ORDERS = SilverSpec(
    source_table="orders",
    expressions={
        "order_id": "nullif(trim(order_id), '')",
        "customer_id": "nullif(trim(customer_id), '')",
        "order_status": "lower(trim(order_status))",
        "order_purchase_ts": "try_to_timestamp(order_purchase_ts)",
        "order_approved_ts": "try_to_timestamp(order_approved_ts)",
        "order_shipped_ts": "try_to_timestamp(order_shipped_ts)",
        "order_delivered_ts": "try_to_timestamp(order_delivered_ts)",
        "estimated_delivery_date": "to_date(try_to_timestamp(estimated_delivery_date))",
        "order_date": "to_date(try_to_timestamp(order_purchase_ts))",
        "channel": "nullif(trim(channel), '')",
        "updated_at": "try_to_timestamp(updated_at)",
    },
)

ORDER_ITEMS = SilverSpec(
    source_table="order_items",
    expressions={
        "order_id": "nullif(trim(order_id), '')",
        "order_item_id": "try_cast(order_item_id AS INT)",
        "product_id": "nullif(trim(product_id), '')",
        "seller_id": "nullif(trim(seller_id), '')",
        "quantity": "try_cast(quantity AS INT)",
        "unit_price": "try_cast(unit_price AS DECIMAL(12,2))",
        "discount_amount": "try_cast(discount_amount AS DECIMAL(12,2))",
        "freight_value": "try_cast(freight_value AS DECIMAL(12,2))",
        "line_amount": ("CAST(try_cast(unit_price AS DECIMAL(12,2)) * try_cast(quantity AS INT)"
                        " - try_cast(discount_amount AS DECIMAL(12,2)) AS DECIMAL(14,2))"),
        "updated_at": "try_to_timestamp(updated_at)",
    },
)

PAYMENTS = SilverSpec(
    source_table="payments",
    expressions={
        "order_id": "nullif(trim(order_id), '')",
        "payment_sequential": "try_cast(payment_sequential AS INT)",
        "payment_type": "lower(trim(payment_type))",
        "payment_installments": "try_cast(payment_installments AS INT)",
        "payment_value": "try_cast(payment_value AS DECIMAL(12,2))",
        "updated_at": "try_to_timestamp(updated_at)",
    },
)

REVIEWS = SilverSpec(
    source_table="reviews",
    expressions={
        "review_id": "nullif(trim(review_id), '')",
        "order_id": "nullif(trim(order_id), '')",
        "review_score": "try_cast(review_score AS INT)",
        "review_comment": "nullif(trim(review_comment), '')",
        "review_ts": "try_to_timestamp(review_ts)",
    },
)

CUSTOMERS = SilverSpec(
    source_table="customers",
    expressions={
        "customer_id": "nullif(trim(customer_id), '')",
        "customer_name": "nullif(trim(customer_name), '')",
        "email": "nullif(lower(trim(email)), '')",
        "phone": "nullif(trim(phone), '')",
        "city": "nullif(trim(city), '')",
        "state": "nullif(trim(state), '')",
        "signup_date": "to_date(try_to_timestamp(signup_date))",
        "updated_at": "try_to_timestamp(updated_at)",
    },
)

PRODUCTS = SilverSpec(
    source_table="products",
    expressions={
        "product_id": "nullif(trim(product_id), '')",
        "product_name": "nullif(trim(product_name), '')",
        "category": "nullif(trim(category), '')",
        "brand": "nullif(trim(brand), '')",
        "unit_price": "try_cast(unit_price AS DECIMAL(12,2))",
        "cost_price": "try_cast(cost_price AS DECIMAL(12,2))",
        "is_active": "try_cast(lower(trim(is_active)) AS BOOLEAN)",
        "seller_id": "nullif(trim(seller_id), '')",
        "updated_at": "try_to_timestamp(updated_at)",
    },
)

SELLERS = SilverSpec(
    source_table="sellers",
    expressions={
        "seller_id": "nullif(trim(seller_id), '')",
        "seller_name": "nullif(trim(seller_name), '')",
        "city": "nullif(trim(city), '')",
        "state": "nullif(trim(state), '')",
        "seller_since": "to_date(try_to_timestamp(seller_since))",
        "seller_rating": "try_cast(seller_rating AS DECIMAL(3,1))",
        "updated_at": "try_to_timestamp(updated_at)",
    },
)

SPECS = {spec.source_table: spec
         for spec in (ORDERS, ORDER_ITEMS, PAYMENTS, REVIEWS, CUSTOMERS, PRODUCTS, SELLERS)}


def get_spec(table: str) -> SilverSpec:
    if table not in SPECS:
        raise KeyError(f"No Silver spec defined for '{table}'. Known: {sorted(SPECS)}")
    return SPECS[table]