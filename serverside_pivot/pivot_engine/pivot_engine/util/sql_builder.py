"""Small helpers for safely constructing the identifier parts of SQL."""

import re

identifier_re = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def safe_ident(name: str) -> str:
    if not isinstance(name, str) or not identifier_re.fullmatch(name):
        raise ValueError(f"Unsafe identifier: {name}")
    return name


def quote_ident(name: str) -> str:
    """Quote one SQL identifier without changing its literal name.

    SQL parameters cannot represent identifiers. Doubling an embedded quote is
    the portable way to keep user- or schema-provided names inside a quoted
    identifier instead of letting them become executable SQL.
    """
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return '"' + name.replace('"', '""') + '"'
