"""Semantic pivot core (golden architecture).

Layers, each importable only through its public module:

* ``core.authority`` — dataset access, row scope, column grants, plan verification
* ``core.ir`` / ``core.registry`` — the language-neutral query meaning
* ``core.logical`` / ``core.physical`` — planning
* ``core.exec`` — the single execution boundary
"""
