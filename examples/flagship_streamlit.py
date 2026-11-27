"""Flagship Streamlit showcase for serverside-pivot.

Start the runtime service first, then run:
``streamlit run examples/flagship_streamlit.py --server.port 8502``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
_PACKAGE_ROOT = _ROOT / "serverside_pivot"
if str(_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_ROOT))

from streamlit_pivot import pivot_workspace

from examples.flagship_demo_data import DEMO_ROW_COUNT, workspace_config


st.set_page_config(page_title="serverside-pivot · Streamlit", page_icon="◆", layout="wide", initial_sidebar_state="collapsed")

# This counts actual script invocations, including host-triggered reruns.
# Qualification observations read the server-produced value in the outer page.
if os.environ.get("PIVOT_FLAGSHIP_PARITY_DATA") == "1" or os.environ.get("PIVOT_FLAGSHIP_QUALIFICATION") == "1":
    st.session_state["_pivot_qualification_script_runs"] = st.session_state.get("_pivot_qualification_script_runs", 0) + 1
    st.markdown(f'<output data-pivot-script-runs="{st.session_state["_pivot_qualification_script_runs"]}">Script runs: {st.session_state["_pivot_qualification_script_runs"]}</output>', unsafe_allow_html=True)

st.markdown(
    """
    <style>
      .stApp { background: #07111F; color: #E5EEF9; }
      .block-container { max-width: 1540px; padding-top: .7rem; padding-bottom: 3rem; }
      [data-testid="stHeader"] { background: rgba(7,17,31,.72); }
      .flagship-eyebrow { color:#5EEAD4; letter-spacing:.1em; font-size:.72rem; font-weight:800; text-transform:uppercase; }
      .flagship-title { color:#F8FAFC; font-size:clamp(1.15rem,2vw,1.65rem); font-weight:800; letter-spacing:-.055em; line-height:1.03; max-width:820px; margin:.4rem 0; }
      .flagship-copy { color:#AABBD0; max-width:780px; line-height:1.4; font-size:.78rem; margin:0; }
      .flagship-header { display:flex; flex-wrap:wrap; align-items:center; justify-content:space-between; gap:8px; padding:4px 0 12px; }
      .flagship-stats { display:flex; gap:12px; font-size:12px; color:#90A6BF; }
      .flagship-stats strong { color:#5EEAD4; margin-right:5px; }
      .flagship-card { border:1px solid #1E3852; border-radius:16px; padding:18px; min-height:145px; background:linear-gradient(145deg,#0D1C2F,#091523); }
      .flagship-card-kicker { color:#5EEAD4; font-family:ui-monospace,monospace; font-size:.7rem; font-weight:800; letter-spacing:.08em; }
      .flagship-card h3 { color:#F8FAFC; font-size:1rem; margin:.7rem 0 .4rem; }
      .flagship-card p { color:#9DB0C6; font-size:.82rem; line-height:1.55; margin:0; }
      .flagship-note { border:1px solid #1E3852; border-radius:14px; padding:15px 17px; color:#9DB0C6; background:#0B1726; font-size:.82rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(f"""<header class="flagship-header">
<div><span class="flagship-eyebrow">serverside-pivot / Streamlit</span>
<h1 class="flagship-title">A real-time analytical workspace in Streamlit.</h1>
<p class="flagship-copy">Explore revenue, shape the model, analyze trends, and export a saved view.</p></div>
<div class="flagship-stats"><span><strong>{DEMO_ROW_COUNT // 1000}k</strong> source rows</span><span><strong>V1</strong> direct transport</span></div>
</header>""", unsafe_allow_html=True)

st.markdown('<div class="flagship-note"><strong style="color:#E5EEF9">Live workspace</strong> &nbsp; Open the fields panel, rearrange the model, switch themes, add a chart, or export the result. The service below is an explicit local V1 endpoint; in production, put it behind your own authentication and row-level-security policy.</div>', unsafe_allow_html=True)

endpoint = os.environ.get("PIVOT_RUNTIME_ENDPOINT", "http://127.0.0.1:8765/v1/runtime")
pivot_workspace(
    runtime_endpoint=endpoint,
    key="flagship-streamlit-pivot",
    **workspace_config("streamlit"),
)

st.markdown("<br/>", unsafe_allow_html=True)
cards = st.columns(4)
card_content = [
    ("01 / LAYOUT", "Compose", "Drag dimensions, add filters, configure measures, formulas, custom dimensions, and reports."),
    ("02 / SCALE", "Navigate", "Rows and columns virtualize independently; the browser requests only the viewport it needs."),
    ("03 / ANALYZE", "Investigate", "Use totals, selection, formatting, sparklines, drill detail, and optional chart workspaces."),
    ("04 / DELIVER", "Share", "Persist a view, copy a selection, or trigger a lazy XLSX export without making every user pay the bundle cost."),
]
for column, (kicker, title, copy) in zip(cards, card_content):
    column.markdown(f'<div class="flagship-card"><div class="flagship-card-kicker">{kicker}</div><h3>{title}</h3><p>{copy}</p></div>', unsafe_allow_html=True)

st.markdown("<br/>", unsafe_allow_html=True)
st.markdown('<div class="flagship-note"><strong>Why this host</strong> · Streamlit keeps Python application state while viewport requests travel directly to the runtime.<br/><strong>Publishing checklist</strong> · Saved views · Virtual scrolling · Charts · XLSX exports</div>', unsafe_allow_html=True)

with st.expander("How this integration is wired"):
    st.code(
        """from streamlit_pivot import pivot_workspace

pivot_workspace(
    "revenue_operations",
    runtime_endpoint="/v1/runtime",
    row_fields=["region", "segment", "product"],
    col_fields=["quarter"],
    server_side=True,
)""",
        language="python",
    )
