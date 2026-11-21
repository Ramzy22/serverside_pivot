"""Query Authority: the only way a protected dataset is read (REQ-SEC-*)."""

from .scope import (
    PathNotProvenError,
    ScopeColumnError,
    ScopeContext,
    ScopeValueError,
    UnscopedAccessError,
    compile_scope_predicate,
    current_scope,
    install_scope_guard,
    require_proven_path,
    row_scoped,
    scoped_execution,
    submit_with_scope,
    wrap_with_scope,
)

__all__ = [
    "PathNotProvenError",
    "ScopeColumnError",
    "ScopeContext",
    "ScopeValueError",
    "UnscopedAccessError",
    "compile_scope_predicate",
    "current_scope",
    "install_scope_guard",
    "require_proven_path",
    "row_scoped",
    "scoped_execution",
    "submit_with_scope",
    "wrap_with_scope",
]
