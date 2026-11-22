"""Authority unit behaviour: scope predicate, guard, context, gate (REQ-SEC-001..007)."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import re
import types
from decimal import Decimal

import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
import pytest

from pivot_engine.core.authority import scope as S
from pivot_engine.runtime.dataset_authorization import RowScopeGrant


def G(table="t", fp=None, **fields):
    fields = {k: tuple(v) for k, v in fields.items()}
    return RowScopeGrant(table=table, subject="u", field_values=fields,
                         scope_fingerprint=fp or repr(sorted(fields.items())))


def OPEN(table="t"):
    return types.SimpleNamespace(table=table, field_values={})


def _con():
    con = ibis.duckdb.connect()
    con.create_table("t", pa.table({
        "tenant": ["A", "a", "B", None],
        "n": [1, 2, 3, None],
        "ok": [True, False, True, None],
        "amt": pa.array([Decimal("1.50"), Decimal("2.00"), None, Decimal("3.25")], pa.decimal128(10, 2)),
        "f": [0.5, 1.0, 2.0, None],
    }))
    return con


def rows(expr, col="tenant"):
    return expr.order_by(expr.n.asc(nulls_first=False)).to_pyarrow().column(col).to_pylist()


# ------------------------------------------------------------ names & grants


@pytest.mark.parametrize("name, bare", [
    ('"Main"."Sales"', "sales"), ("db.t", "t"), ("  T ", "t"), (None, ""), ("", ""), ('"x"', "x"), ("a.b.c", "c"),
])
def test_bare_table_names(name, bare):
    assert S._bare(name) == bare


def test_grant_fields_and_protection():
    assert S._grant_fields(types.SimpleNamespace(field_values=["x"])) == {}
    assert S._grant_fields(None) == {}
    assert S._grant_fields(G(tenant=["A"])) == {"tenant": ("A",)}
    assert S.is_protecting(G(tenant=["A"])) is True
    assert S.is_protecting(OPEN()) is False
    ctx = S.ScopeContext(grants={"t": G(tenant=["A"]), "u": None})
    assert ctx.grant_for('"DB"."T"') is ctx.grants["t"]
    assert ctx.grant_for("zz") is None
    assert ctx.active is True
    assert S.ScopeContext(grants={"u": OPEN()}).active is False
    assert S.current_scope() is S._EMPTY_SCOPE
    assert S.current_scope().active is False


# ------------------------------------------------------------ exact coercion


@pytest.mark.parametrize("value, dtype, expected", [
    (None, dt.string, None), ("A", dt.string, "A"), (7, dt.string, "7"),
    (5, dt.int64, 5), (" +5 ", dt.int64, 5), ("-3", dt.int32, -3), (Decimal("4"), dt.int64, 4),
    ("1.25", dt.Decimal(10, 2), Decimal("1.25")), (2, dt.Decimal(10, 2), Decimal("2")),
    ("0.5", dt.float64, 0.5), (1, dt.float64, 1.0),
    (True, dt.boolean, True), (False, dt.boolean, False),
    ("2026-01-01", dt.date, "2026-01-01"),
])
def test_coerce_exact(value, dtype, expected):
    got = S._coerce_exact(value, dtype, "col")
    assert got == expected
    assert type(got) is type(expected)


@pytest.mark.parametrize("value, dtype, message", [
    (True, dt.int64, "Scope value for 'col' is not an integer."),
    ("1.5", dt.int64, "Scope value for 'col' is not an exact integer."),
    (Decimal("1.5"), dt.int64, "Scope value for 'col' is not an exact integer."),
    (1.0, dt.int64, "Scope value for 'col' is not an exact integer."),
    ("x", dt.Decimal(10, 2), "Scope value for 'col' is invalid."),
    ("x", dt.float64, "Scope value for 'col' is invalid."),
    ("true", dt.boolean, "Scope value for 'col' is not a boolean."),
    (1, dt.boolean, "Scope value for 'col' is not a boolean."),
])
def test_coerce_exact_rejects(value, dtype, message):
    with pytest.raises(S.ScopeValueError, match="^" + re.escape(message) + "$"):
        S._coerce_exact(value, dtype, "col")


# ------------------------------------------------------------ predicate


def test_predicate_semantics_are_exact():
    t = S.unguarded_table(_con(), "t")
    assert S.compile_scope_predicate(t, OPEN()) is None
    assert rows(t.filter(S.compile_scope_predicate(t, G(tenant=["A"])))) == ["A"]
    assert rows(t.filter(S.compile_scope_predicate(t, G(tenant=["A", "B"])))) == ["A", "B"]
    assert rows(t.filter(S.compile_scope_predicate(t, G(tenant=["a", None])))) == ["a", None]
    assert rows(t.filter(S.compile_scope_predicate(t, G(tenant=[None])))) == [None]
    assert rows(t.filter(S.compile_scope_predicate(t, G(tenant=["A", "B"], n=["3", 1])))) == ["A", "B"]
    assert rows(t.filter(S.compile_scope_predicate(t, G(tenant=["A", "B"], n=[3])))) == ["B"]
    assert rows(t.filter(S.compile_scope_predicate(t, G(amt=["1.5"]))), "amt") == [Decimal("1.50")]
    assert rows(t.filter(S.compile_scope_predicate(t, G(ok=[True])))) == ["A", "B"]


def test_single_value_uses_equality_and_many_use_isin():
    t = S.unguarded_table(_con(), "t")
    assert type(S.compile_scope_predicate(t, G(tenant=["A"])).op()).__name__ == "Equals"
    assert type(S.compile_scope_predicate(t, G(tenant=["A", "B"])).op()).__name__ == "InValues"


def test_predicate_denials():
    t = S.unguarded_table(_con(), "t")
    with pytest.raises(S.ScopeColumnError,
                       match=r"^Row policy column 'org' is not present in the dataset; access denied\.$"):
        S.compile_scope_predicate(t, G(tenant=["A"], org=["x"]))
    with pytest.raises(S.UnscopedAccessError, match=r"^Row grant is empty; access denied\.$"):
        S.compile_scope_predicate(t, G(tenant=[]))
    with pytest.raises(S.ScopeValueError, match="'n'"):
        S.compile_scope_predicate(t, G(n=["x"]))


def test_apply_scope():
    t = S.unguarded_table(_con(), "t")
    assert S.apply_scope("t", t) is t
    with pytest.raises(S.UnscopedAccessError, match=r"^Dataset 't' is protected and no row grant is active"):
        S.apply_scope('"main"."T"', t, protected={"t"})
    with S.scoped_execution(G(tenant=["B"])):
        assert rows(S.apply_scope("main.t", t)) == ["B"]
        assert S.apply_scope("other", t) is t
    with S.scoped_execution({"t": OPEN()}):
        with pytest.raises(S.UnscopedAccessError):
            S.apply_scope("t", t, protected={"t"})


# ------------------------------------------------------------ guard


def test_guard_install_is_idempotent_and_patches_the_connection():
    con = _con()
    raw_table = con.table
    assert S.install_scope_guard(None) is None
    assert S.install_scope_guard(object()) is None
    assert S.connection_guard(None) is None
    assert S.connection_guard(con) is None
    assert S.protected_tables(con) == frozenset()
    guard = S.install_scope_guard(con)
    assert S.install_scope_guard(con) is guard
    assert S.connection_guard(con) is guard
    assert guard.raw_table == raw_table
    assert con.table == guard.table
    assert con.sql == guard.sql
    assert con.raw_sql == guard.raw_sql
    assert con.table("t").count().to_pyarrow().as_py() == 4  # unprotected: pass-through
    assert con.sql("select count(*) as n from t").to_pyarrow().column(0).to_pylist() == [4]


def test_guard_without_sql_methods_only_patches_table():
    class Bare:
        def table(self, name):
            return name

    b = Bare()
    guard = S.install_scope_guard(b)
    assert guard._raw_sql is None
    assert guard._raw_raw_sql is None
    assert "sql" not in vars(b)
    assert "raw_sql" not in vars(b)
    assert b.table("x") == "x"


def test_guard_denies_raw_sql_naming_a_protected_table_only():
    con = _con()
    con.create_table("tx", pa.table({"x": [1]}))
    con.create_table("t_2", pa.table({"x": [2]}))
    with S.scoped_execution(G(tenant=["A"]), connections=[con]):
        assert S.protected_tables(con) == frozenset({"t"})
        assert rows(con.table("t")) == ["A"]
        for sql in ("select * from t", 'SELECT * FROM "T"', "select 1 from main.t where 1=1"):
            with pytest.raises(S.UnscopedAccessError,
                               match="^Raw SQL over a protected dataset cannot be proven scoped"):
                con.sql(sql)
            with pytest.raises(S.UnscopedAccessError):
                con.raw_sql(sql)
        assert con.sql("select x from tx").to_pyarrow().column(0).to_pylist() == [1]
        assert con.sql("select x from t_2").to_pyarrow().column(0).to_pylist() == [2]
        assert con.raw_sql("select 1 as one").fetchall() == [(1,)]
        assert S.unguarded_table(con, "t").count().to_pyarrow().as_py() == 4


def test_unguarded_table_on_plain_connection():
    assert S.unguarded_table(_con(), "t").count().to_pyarrow().as_py() == 4


# ------------------------------------------------------------ connections_of


class Con:
    def __init__(self, name):
        self.name = name

    def table(self, _):  # pragma: no cover - identity only
        return None

    def __repr__(self):
        return self.name


def _holder(con=None, backends=None):
    catalog = types.SimpleNamespace(_registered_backends=backends or {})
    return types.SimpleNamespace(con=con, builder=types.SimpleNamespace(catalog=catalog))


def test_connections_of_walks_controller_planner_backend_and_catalogs():
    a, b, c, d, e = (Con(x) for x in "abcde")
    controller = _holder(a, {"x": b, "y": types.SimpleNamespace(con=d)})
    controller.planner = _holder(c, {"z": a})  # duplicate a
    controller.backend = _holder(e)
    owner = types.SimpleNamespace(controller=controller)
    assert S.connections_of(owner) == [a, b, d, c, e]
    assert S.connections_of(controller) == [a, b, d, c, e]  # no .controller: the owner is the controller


def test_connections_of_skips_missing_pieces():
    a, b = Con("a"), Con("b")
    controller = types.SimpleNamespace(con=object(), planner=None, backend=_holder(a, {"k": b}))
    assert S.connections_of(controller) == [a, b]
    assert S.connections_of(types.SimpleNamespace()) == []
    assert S.connections_of(None) == []


# ------------------------------------------------------------ context


def test_prepare_scope_forms():
    g = G(tenant=["A"])
    assert dict(S._prepare_scope(None, (), ()).grants) == {}
    assert dict(S._prepare_scope(g, (), ()).grants) == {"t": g}
    mapping = S._prepare_scope({'"Main"."T"': g, "u": None}, (), ("p1",))
    assert dict(mapping.grants) == {"t": g}
    assert mapping.proven_paths == frozenset({"p1"})


def test_nested_scopes_merge_and_cannot_replace_a_protecting_grant():
    a, b = G(tenant=["A"], fp="fa"), G(tenant=["B"], fp="fb")
    same = G(tenant=["A"], fp="fa")
    other_table = G(table="u", tenant=["B"], fp="fb")
    with S.scoped_execution(a, proven_paths=("p1",)):
        with S.scoped_execution(same, proven_paths=("p2",)) as inner:
            assert inner.grant_for("t") is same
            assert inner.proven_paths == frozenset({"p1", "p2"})
        with S.scoped_execution(other_table) as inner:
            assert inner.grant_for("t") is a
            assert inner.grant_for("u") is other_table
        with S.scoped_execution(a):
            pass
        with pytest.raises(S.UnscopedAccessError,
                           match=r"^Nested execution attempted to replace an active row grant\.$"):
            with S.scoped_execution(b):
                pass
        assert S.current_scope().grant_for("t") is a
    assert S.current_scope() is S._EMPTY_SCOPE
    with S.scoped_execution(OPEN()):
        with S.scoped_execution(b) as inner:
            assert inner.grant_for("t") is b


def test_scope_installs_guards_protecting_only_protected_tables():
    con = _con()
    with S.scoped_execution({"t": G(tenant=["A"]), "u": OPEN("u")}, connections=[con, None, object()]):
        assert S.protected_tables(con) == frozenset({"t"})


def test_scope_travels_across_thread_hops():
    seen = []
    with S.scoped_execution(G(tenant=["A"], fp="fa")):
        bound = S.wrap_with_scope(lambda x: (x, S.current_scope().grant_for("t").scope_fingerprint))
        with concurrent.futures.ThreadPoolExecutor(1) as pool:
            seen.append(S.submit_with_scope(
                pool, lambda k: (k, S.current_scope().grant_for("t").scope_fingerprint), k=9).result())
            seen.append(pool.submit(bound, 1).result())

            async def hop():
                return await S.run_in_executor_scoped(
                    asyncio.get_running_loop(), pool, lambda y: (y, S.current_scope().active), 2)

            seen.append(asyncio.run(hop()))
    assert seen == [(9, "fa"), (1, "fa"), (2, True)]
    assert bound.__name__ == "<lambda>"


def test_require_proven_path():
    S.require_proven_path("anything")  # no protecting grant: allowed
    with S.scoped_execution(G(tenant=["A"]), proven_paths=("ok",)):
        S.require_proven_path("ok")
        with pytest.raises(S.PathNotProvenError, match=r"^Execution path 'pruned' is not proven tenant-safe"):
            S.require_proven_path("pruned")
    with S.scoped_execution(OPEN()):
        S.require_proven_path("pruned")


def test_row_scoped_sync_and_async():
    con = _con()

    class Adapter:
        controller = types.SimpleNamespace(con=con, planner=None, backend=None)

        @S.row_scoped
        def sync(self, x, row_scope=None):
            return x, S.current_scope().grant_for("t")

        @S.row_scoped
        async def asynchronous(self, x, row_scope=None):
            return x, S.current_scope().grant_for("t")

    g = G(tenant=["A"])
    assert Adapter().sync(2) == (2, None)
    assert S.protected_tables(con) == frozenset()
    assert Adapter().sync(1, row_scope=g) == (1, g)
    assert S.protected_tables(con) == frozenset({"t"})
    assert asyncio.run(Adapter().asynchronous(3, row_scope=g)) == (3, g)
    assert Adapter.sync.__name__ == "sync"
    assert Adapter.asynchronous.__name__ == "asynchronous"
    assert S.current_scope() is S._EMPTY_SCOPE


def test_bind_request_scope_binds_for_the_rest_of_the_context():
    a, b = G(tenant=["A"], fp="fa"), G(tenant=["B"], fp="fb")
    con = _con()

    def handler():
        ctx = S.bind_request_scope(a, connections=[con])
        assert S.current_scope() is ctx
        assert S.protected_tables(con) == frozenset({"t"})
        with pytest.raises(S.UnscopedAccessError):
            S.bind_request_scope(b)
        return S.current_scope().grant_for("t")

    assert contextvars.copy_context().run(handler) is a
    assert S.current_scope() is S._EMPTY_SCOPE


# ------------------------------------------------------------ raw-SQL where


def _quote(name):
    return f'"{name}"'


def test_scope_where_sql():
    assert S.scope_where_sql(OPEN(), _quote) == ("", [])
    assert S.scope_where_sql(G(tenant=["A"]), _quote) == (' WHERE ("tenant" IN (?))', ["A"])
    assert S.scope_where_sql(G(tenant=["A", None, "B"], n=[1]), _quote) == (
        ' WHERE ("n" IN (?)) AND ("tenant" IN (?, ?) OR "tenant" IS NULL)', [1, "A", "B"])
    assert S.scope_where_sql(G(tenant=[None]), _quote) == (' WHERE ("tenant" IS NULL)', [])
    with pytest.raises(S.UnscopedAccessError, match=r"^Row grant is empty; access denied\.$"):
        S.scope_where_sql(G(tenant=[]), _quote)


def test_error_hierarchy_is_permission_error():
    for cls in (S.UnscopedAccessError, S.ScopeColumnError, S.ScopeValueError, S.PathNotProvenError):
        assert issubclass(cls, S.ScopeError)
        assert issubclass(cls, PermissionError)
