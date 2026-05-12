import pytest

from connector.duckdb_connector import _normalize_type


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("INTEGER", "INT"),
        ("DECIMAL(10,2)", "DECIMAL"),
        ("DECIMAL", "DECIMAL"),
        ("TIMESTAMP", "TIMESTAMP"),
        ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP"),
        ("VARCHAR", "VARCHAR"),
        ("BOOLEAN", "BOOLEAN"),
        ("BIGINT", "BIGINT"),
    ],
)
def test_normalize_type(raw, expected):
    assert _normalize_type(raw) == expected
