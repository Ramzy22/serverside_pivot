"""Live Oracle Free parity against the independent record-by-record reference."""

import os
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

import pytest

from pivot_engine.backends.ibis_backend import IbisBackend
from pivot_engine.core.exec import execute
from pivot_engine.core.ir import MeasureFilter, MeasureRef, PivotQuery, Predicate, SortKey, Window

from ..oracle.ir_adapter import oracle_rows


TABLE = "SalesLedger"
BASE = PivotQuery(
    dataset=TABLE,
    rows=("Region", "Channel"),
    measures=(
        MeasureRef("net", "sum", "Amount"),
        MeasureRef("mean", "avg", "Amount"),
        MeasureRef("orders", "count", None),
        MeasureRef("priced", "count", "Amount"),
        MeasureRef("buyers", "count_distinct", "Buyer"),
    ),
)
QUERIES = {
    "hierarchy-and-decimal-aggregates": BASE,
    "quarter-pivot": replace(BASE, measures=BASE.measures[:1], columns=("Quarter",),
                             column_members=(("Q1",), ("Q2",), ("Q3",), ("Q4",))),
    "filtered-and-sorted": replace(BASE, filters=(Predicate("Channel", "=", "retail", False),
                                                 Predicate("Amount", ">", 0)),
                                   sort=(SortKey("net", desc=True),)),
    "measure-filter-null-group": replace(BASE, rows=("Channel",),
                                         measure_filters=(MeasureFilter(1, "orders", ">=", 10),)),
    "empty-varchar-is-null": replace(BASE, filters=(Predicate("Channel", "is_null"),)),
    "blank-or-null-channel": replace(BASE, filters=(Predicate("Channel", "is_empty"),)),
    "nonblank-channel": replace(BASE, filters=(Predicate("Channel", "is_not_empty"),)),
    "middle-page": replace(BASE, sort=(SortKey("net", desc=True),), window=Window(2, 4)),
    "past-last-page": replace(BASE, window=Window(999, 3)),
    "no-sales-match": replace(BASE, filters=(Predicate("Region", "=", "Atlantis"),)),
    "min-and-max": replace(BASE, measures=(MeasureRef("low", "min", "Amount"),
                                          MeasureRef("high", "max", "Amount"))),
    "leaf-partials": replace(BASE, measures=(BASE.measures[0], BASE.measures[2])),
    "all-record-totals": replace(BASE, totals="all_data",
                                 measure_filters=(MeasureFilter(2, "orders", ">=", 12),)),
    "collapsed-hierarchy": replace(BASE, expanded=frozenset({("EMEA",)})),
    "null-membership-filter": replace(BASE, filters=(Predicate("Channel", "in", (None, "Direct")),)),
    "zero-size-page": replace(BASE, window=Window(0, 0)),
    "subtotal-after-children": replace(BASE, subtotals="after"),
}


def sales():
    """Posted sales, refunds and pending prices across four regions and quarters."""
    records = []
    for i in range(192):
        cents = (1879 + 613 * i) % 75000
        if i % 11 == 0:
            cents = -cents
        records.append({
            "Region": ("EMEA", "APAC", "Americas", "Nordics")[i % 4],
            "Channel": "   " if i % 48 == 7 else ("Retail", "Direct", "", "retail", "Partner", None)[(i // 4) % 6],
            "Quarter": f"Q{1 + i // 48}",
            "Buyer": None if i % 13 == 0 else f"C{i % 23:03d}",
            "Amount": None if i % 17 == 0 else Decimal(cents).scaleb(-2),
        })
    return records


@pytest.fixture(scope="module")
def ledger():
    uri = os.environ.get("PIVOT_TEST_ORACLE_URI")
    if not uri:
        pytest.skip("Start the live database with the command in tests/oracle_db/README.md")
    assert uri.startswith("oracle://"), "The live suite requires an Oracle URI"
    # Each xdist worker owns its table, including during overlapping runs.
    name = f"Sales_{uuid4().hex[:20]}"
    with IbisBackend(connection_uri=uri) as backend:
        con = backend.con
        assert con.name == "oracle"
        records = sales()
        with con.begin() as cur:
            cur.execute(f'''CREATE TABLE "{name}" (
                "Region" VARCHAR2(16) NOT NULL, "Channel" VARCHAR2(16),
                "Quarter" VARCHAR2(2) NOT NULL, "Buyer" VARCHAR2(16), "Amount" NUMBER(12,2)
            )''')
        try:
            with con.begin() as cur:
                cur.executemany(
                    f'INSERT INTO "{name}" ("Region","Channel","Quarter","Buyer","Amount") '
                    'VALUES (:Region,:Channel,:Quarter,:Buyer,:Amount)', records,
                )
                cur.execute(f'SELECT COUNT(*), COUNT("Channel") FROM "{name}"')
                assert cur.fetchone() == (192, 128)
            # Oracle stores zero-length VARCHAR2 values as NULL. Derive the expected
            # input from the seed, independently of any rows returned by the database.
            expected = [dict(row, Channel=row["Channel"] or None) for row in records]
            yield con, name, expected
        finally:
            with con.begin() as cur:
                cur.execute(f'DROP TABLE "{name}" PURGE')


@pytest.mark.parametrize("query", QUERIES.values(), ids=QUERIES.keys())
def test_oracle_database_matches_reference(ledger, query):
    con, name, records = ledger
    query = replace(query, dataset=name)
    frame = execute(query, con.table, grand_total=True)
    whole = oracle_rows(replace(query, window=Window()), records)
    offset, limit = query.window.offset, query.window.limit
    expected = whole[offset:None if limit is None else offset + limit]

    actual = list(frame.records())
    assert [row["path"] for row in actual] == [path for path, _ in expected]
    for row, (_, values) in zip(actual, expected, strict=True):
        assert_values(row["values"], values)
    assert frame.total == len(whole)
    grand = next(values for path, values in whole if path == ())
    assert_values(frame.grand_total_record()["values"], grand)


def assert_values(actual, expected):
    assert actual.keys() == expected.keys()
    for key, value in expected.items():
        if key == "mean" and value is not None:
            # Ibis exposes AVG(NUMBER(12,2)) at the input scale in Arrow.
            # Allow rounding a fractional cent only for this measure.
            assert abs(Decimal(str(actual[key])) - value) <= Decimal("0.005"), key
        else:
            assert actual[key] == value, key
