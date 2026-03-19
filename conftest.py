# conftest.py — repo root
# The actual pivot_engine package lives at pivot_engine/pivot_engine/
# Adding the outer pivot_engine/ dir to sys.path makes imports like
# `from pivot_engine.controller import PivotController` resolve correctly
# when pytest is run from the repo root.
import sys
import os

# The pivot_engine lives in dash_tanstack_pivot/pivot_engine/
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "dash_tanstack_pivot", "pivot_engine"))
