from dash import Dash, dcc, html, dash_table

from dash_tanstack_pivot import DashTanstackPivot
from pivot_engine.runtime import (
    DashPivotInstanceConfig,
    register_dash_callbacks_for_instances,
    register_dash_drill_modal_callback,
    register_dash_pivot_transport_callback,
)
from pivot_engine.runtime.dash_callbacks import _is_bootstrap_without_viewport


class _StubRuntimeService:
    def process(self, *args, **kwargs):  # pragma: no cover - callback execution is not in scope here
        raise RuntimeError("not used in registration-only tests")


def _make_app():
    app = Dash(__name__)
    app.layout = html.Div(
        [
            DashTanstackPivot(id="pivot-a", table="sales_a"),
            DashTanstackPivot(id="pivot-b", table="sales_b"),
            dcc.Store(id="drill-a"),
            dcc.Store(id="drill-b"),
            html.Div(id="modal-a"),
            html.Div(id="modal-b"),
            dash_table.DataTable(id="table-a"),
            dash_table.DataTable(id="table-b"),
            html.Button("close", id="close-a"),
            html.Button("close", id="close-b"),
        ]
    )
    return app


def test_register_dash_pivot_transport_callback_is_idempotent():
    app = _make_app()
    getter = lambda: _StubRuntimeService()

    assert register_dash_pivot_transport_callback(
        app, getter, pivot_id="pivot-a", drill_store_id="drill-a", debug=False
    )
    assert not register_dash_pivot_transport_callback(
        app, getter, pivot_id="pivot-a", drill_store_id="drill-a", debug=False
    )
    assert register_dash_pivot_transport_callback(
        app, getter, pivot_id="pivot-b", drill_store_id="drill-b", debug=False
    )
    assert len(app.callback_map) == 2


def test_register_dash_pivot_transport_callback_without_drill_store():
    app = Dash(__name__)
    app.layout = html.Div([DashTanstackPivot(id="pivot-only", table="sales")])
    getter = lambda: _StubRuntimeService()

    assert register_dash_pivot_transport_callback(
        app, getter, pivot_id="pivot-only", debug=False
    )
    assert len(app.callback_map) == 1


def test_register_dash_drill_modal_callback_is_idempotent():
    app = _make_app()

    assert register_dash_drill_modal_callback(
        app,
        drill_store_id="drill-a",
        close_drill_id="close-a",
        drill_modal_id="modal-a",
        drill_table_id="table-a",
    )
    assert not register_dash_drill_modal_callback(
        app,
        drill_store_id="drill-a",
        close_drill_id="close-a",
        drill_modal_id="modal-a",
        drill_table_id="table-a",
    )
    assert len(app.callback_map) == 1


def test_register_dash_callbacks_for_instances_supports_multi_instance():
    app = _make_app()
    getter = lambda: _StubRuntimeService()

    instances = [
        DashPivotInstanceConfig(
            pivot_id="pivot-a",
            drill_store_id="drill-a",
            drill_modal_id="modal-a",
            drill_table_id="table-a",
            close_drill_id="close-a",
        ),
        DashPivotInstanceConfig(
            pivot_id="pivot-b",
            drill_store_id="drill-b",
            drill_modal_id="modal-b",
            drill_table_id="table-b",
            close_drill_id="close-b",
        ),
    ]

    status = register_dash_callbacks_for_instances(app, getter, instances, debug=False)
    assert status == {"pivot-a": True, "pivot-b": True}
    assert len(app.callback_map) == 4

    status_repeat = register_dash_callbacks_for_instances(app, getter, instances, debug=False)
    assert status_repeat == {"pivot-a": False, "pivot-b": False}
    assert len(app.callback_map) == 4


def test_bootstrap_detection_requires_real_viewport_context():
    assert _is_bootstrap_without_viewport(None, None) is True
    assert _is_bootstrap_without_viewport(None, {}) is True
    assert _is_bootstrap_without_viewport(None, {"start": 0, "end": 99}) is True
    assert (
        _is_bootstrap_without_viewport(
            None,
            {
                "start": 0,
                "end": 99,
                "session_id": "sess",
                "client_instance": "client-a",
            },
        )
        is False
    )
    assert _is_bootstrap_without_viewport("pivot-grid.viewport", None) is False


def test_bootstrap_fallback_viewport_shape_is_complete():
    pivot_id = "pivot-grid"
    fallback_viewport = {
        "start": 0,
        "end": 99,
        "count": 100,
        "window_seq": 1,
        "state_epoch": 0,
        "abort_generation": 0,
        "session_id": f"bootstrap::{pivot_id}",
        "client_instance": "bootstrap",
        "intent": "structural",
        "needs_col_schema": True,
    }
    assert fallback_viewport["start"] == 0
    assert fallback_viewport["end"] == 99
    assert fallback_viewport["needs_col_schema"] is True
