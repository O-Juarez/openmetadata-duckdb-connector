"""Tests for pure-function helpers in connector.duckdb_profiler."""
import pytest

pytest.importorskip("metadata.generated.schema.api.data.createTableProfile")

from connector.duckdb_profiler import (  # noqa: E402
    _build_column_aggregates,
    _is_numeric,
    _is_string,
    _is_temporal,
    _matches_any,
    _quote_ident,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("INT", True),
        ("BIGINT", True),
        ("DECIMAL(10,2)", True),
        ("DOUBLE", True),
        ("VARCHAR", False),
        ("DATE", False),
    ],
)
def test_is_numeric(raw, expected):
    assert _is_numeric(raw) is expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("DATE", True),
        ("TIMESTAMP", True),
        ("TIMESTAMP WITH TIME ZONE", True),
        ("TIME", True),
        ("VARCHAR", False),
        ("INT", False),
    ],
)
def test_is_temporal(raw, expected):
    assert _is_temporal(raw) is expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("VARCHAR", True),
        ("CHAR", True),
        ("TEXT", True),
        ("INT", False),
        ("DATE", False),
    ],
)
def test_is_string(raw, expected):
    assert _is_string(raw) is expected


def test_quote_ident_escapes_internal_quotes():
    assert _quote_ident('foo"bar') == '"foo""bar"'


def test_quote_ident_simple():
    assert _quote_ident("col_with_hyphen-and-dot.x") == '"col_with_hyphen-and-dot.x"'


def test_matches_any_handles_none():
    assert _matches_any("anything", None) is False
    assert _matches_any("anything", []) is False


def test_matches_any_uses_fullmatch():
    assert _matches_any("dim_users", ["dim_.*"]) is True
    assert _matches_any("fact_users", ["dim_.*"]) is False
    # fullmatch — partial regex doesn't count
    assert _matches_any("dim_users_extra", ["dim_users"]) is False


def test_build_column_aggregates_numeric_includes_stats():
    parts = _build_column_aggregates(0, "amount", "DECIMAL(10,2)")
    joined = " ".join(parts)
    assert "COUNT(\"amount\")" in joined
    assert "MIN(\"amount\")" in joined
    assert "MAX(\"amount\")" in joined
    assert "AVG(\"amount\"::DOUBLE)" in joined
    assert "STDDEV" in joined


def test_build_column_aggregates_string_includes_lengths():
    parts = _build_column_aggregates(0, "name", "VARCHAR")
    joined = " ".join(parts)
    assert "MIN(LENGTH(\"name\"))" in joined
    assert "MAX(LENGTH(\"name\"))" in joined
    assert "STDDEV" not in joined  # no numeric aggregates for strings


def test_build_column_aggregates_temporal_includes_min_max_only():
    parts = _build_column_aggregates(0, "created_at", "TIMESTAMP")
    joined = " ".join(parts)
    assert "MIN(\"created_at\")" in joined
    assert "MAX(\"created_at\")" in joined
    assert "AVG" not in joined
