"""
test_connection_masking.py — Database passwords never reach the terminal (security review I4).

README: "When a connection fails, the error shows the URL with the password
masked". Every place that prints a connection string goes through
loader._mask_password(): SQLAlchemy's URL rendering with hide_password,
and a fallback that masks up to the last "@" of the authority (a password
holding a raw "@") and every `password=` query value. The credentials
here are made up.
"""

from __future__ import annotations

import pytest

from datadelta.loader import _mask_password, load_file, source_label


SECRET = "s3cr3tXYZ"


@pytest.mark.parametrize("url, kept", [
    (f"postgresql://alice:{SECRET}@db.internal:5432/shop::orders",
     ["postgresql://alice:***@db.internal:5432/shop", "::orders"]),
    (f"postgresql://alice:{SECRET[:4]}@{SECRET[4:]}@db/shop::orders",          # a raw "@" in the password
     ["alice", "db/shop", "::orders"]),
    (f"postgresql://alice@db/shop?password={SECRET}::orders",                 # password as a query parameter
     ["alice@db/shop", "::orders"]),
    (f"postgresql://alice:pw@db/shop?sslmode=require&PASSWORD={SECRET}::orders",
     ["sslmode=require", "::orders"]),
    (f"mssql+pyodbc://u:{SECRET}@host/db?driver=ODBC+Driver+18::t",
     ["u:***@host/db", "driver=ODBC+Driver+18", "::t"]),
    (f"postgresql://alice:{SECRET}:x@db/shop::orders",                        # a ":" in the password
     ["alice", "db/shop"]),
    (f"postgresql://alice:{SECRET[:4]}/{SECRET[4:]}@db/shop::orders",         # a raw "/" in the password
     ["alice", "db/shop"]),
    (f"postgresql://alice:{SECRET[:4]}?{SECRET[4:]}@db/shop::orders",         # a raw "?" in the password
     ["alice", "db/shop"]),
    (f"postgresql://alice:{SECRET[:4]}@{SECRET[4:]}/x@db/shop::orders",       # "@" and "/" together
     ["alice", "db/shop"]),
])
def test_mask_password_hides_every_form_of_password(url, kept):
    masked = _mask_password(url)
    assert SECRET not in masked
    for piece in (SECRET[:4], SECRET[4:]):
        assert piece not in masked
    assert "***" in masked
    for text in kept:
        assert text in masked
    assert source_label(url) == masked


@pytest.mark.parametrize("source", ["sqlite:////abs/path/shop.db::orders", "data/orders.csv", "plain.xlsx"])
def test_mask_password_leaves_sources_without_a_password_alone(source):
    assert _mask_password(source) == source


def test_missing_table_suffix_error_masks_the_password():
    with pytest.raises(ValueError) as info:
        load_file(f"postgresql://alice:{SECRET}@127.0.0.1:1/shop")
    assert SECRET not in str(info.value)
    assert "Got: postgresql://alice:***@127.0.0.1:1/shop" in str(info.value)


def test_unknown_prefix_error_masks_the_password():
    """'postgres://' is not a known prefix, so the URL is taken for a path."""
    with pytest.raises(FileNotFoundError) as info:
        load_file(f"postgres://alice:{SECRET}@db/shop::orders")
    assert SECRET not in str(info.value)
    assert "***" in str(info.value)


@pytest.mark.parametrize("url", [
    "postgres://alice:S3cretPw@db::orders",                    # unknown prefix, no /db path
    "postgresql+psycopg2://alice:S3cretPw@db::orders",         # driver suffix not in SQL_PREFIXES, no /db path
])
def test_source_label_masks_any_url_even_without_a_known_prefix(url):
    """Path(source).name is the whole authority when the URL has no path."""
    label = source_label(url)
    assert "S3cretPw" not in label
    assert "***" in label
    assert "db" in label


@pytest.mark.parametrize("command", [["diff", "{url}", "{csv}"], ["clean", "{url}", "--dry-run"]])
@pytest.mark.parametrize("url", [
    f"postgresql://alice:{SECRET}@127.0.0.1:1/shop",
    f"postgresql://alice:{SECRET[:4]}@{SECRET[4:]}@127.0.0.1:1/shop::orders",
    f"postgres://alice:{SECRET}@db/shop::orders",
])
def test_diff_and_clean_never_print_the_password(cli, write_csv, command, url):
    import pandas as pd
    csv = write_csv("x.csv", pd.DataFrame({"a": [1, 2]}))
    result = cli(*[part.format(url=url, csv=csv) for part in command])
    assert result.exit_code == 1
    output = result.stdout + result.stderr
    assert "Error" in output
    for piece in (SECRET, SECRET[:4], SECRET[4:]):
        assert piece not in output


def test_init_never_prints_the_password(cli, fake_anthropic):
    url = f"postgresql://alice:{SECRET}@127.0.0.1:1/shop::orders"
    result = cli("init", "--from", url, "--business", "We run a web shop.")
    assert result.exit_code == 1
    assert "Loading sample file: postgresql://alice:***@127.0.0.1:1/shop::orders" in result.stderr
    assert SECRET not in result.stdout + result.stderr
    assert fake_anthropic.calls == []
