import pytest

from scripts.count_customer_orders import count_customer_order, parse_line


def test_parse_line_should_succeed():
    line = "44,8602,37.19"
    customer_id, order_amount = parse_line(line)
    assert customer_id == 44
    assert order_amount == 37.19


def test_count_customer_order_should_succeed(spark_context_fixture):
    result = count_customer_order(
        spark_context=spark_context_fixture,
        file_path="tests/test_data/test_customer_orders.csv"
    )
    assert result == [(4, 83.55), (35, 110.85), (44, 118.95), (47, 68.1)]