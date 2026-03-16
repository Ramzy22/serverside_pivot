"""
SQL builder utilities to help generate safe SQL fragments.
(Real implementation should use parameterized queries; this is a simple helper.)
"""
import re
identifier_re = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

def safe_ident(name: str) -> str:
    if not identifier_re.match(name):
        raise ValueError(f"Unsafe identifier: {name}")
    return name
