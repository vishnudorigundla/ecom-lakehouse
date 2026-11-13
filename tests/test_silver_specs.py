from ecom.silver_specs import (
    CUSTOMERS,
    ORDER_ITEMS,
    ORDERS,
    PAYMENTS,
    PRODUCTS,
    REVIEWS,
    SELLERS,
    SilverSpec,
)

ALL_SPECS = [ORDERS, ORDER_ITEMS, PAYMENTS, REVIEWS, CUSTOMERS, PRODUCTS, SELLERS]


def test_every_spec_is_well_formed():
    for spec in ALL_SPECS:
        assert isinstance(spec, SilverSpec)
        assert spec.source_table
        assert spec.expressions
        assert all(isinstance(e, str) and e.strip() for e in spec.expressions.values())


def test_source_tables_are_unique():
    names = [s.source_table for s in ALL_SPECS]
    assert len(names) == len(set(names))


def test_key_columns_are_cleaned():
    """Business keys are trimmed and blank becomes NULL, so blank keys reach quarantine, not the MERGE."""
    keys = [
        (ORDERS, ["order_id"]),
        (ORDER_ITEMS, ["order_id", "product_id"]),
        (PAYMENTS, ["order_id"]),
        (REVIEWS, ["review_id"]),
        (CUSTOMERS, ["customer_id"]),
        (PRODUCTS, ["product_id"]),
        (SELLERS, ["seller_id"]),
    ]
    for spec, columns in keys:
        for column in columns:
            assert "nullif" in spec.expressions[column].lower(), f"{spec.source_table}.{column}"


def test_money_columns_are_decimal_not_float():
    money = [
        (ORDER_ITEMS, ["unit_price", "discount_amount", "freight_value", "line_amount"]),
        (PAYMENTS, ["payment_value"]),
        (PRODUCTS, ["unit_price", "cost_price"]),
    ]
    for spec, columns in money:
        for column in columns:
            expr = spec.expressions[column].upper()
            assert "DECIMAL" in expr, f"{spec.source_table}.{column}"
            assert "DOUBLE" not in expr and "FLOAT" not in expr, f"{spec.source_table}.{column}"
