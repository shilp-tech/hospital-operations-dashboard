"""
Hospital Operations Dashboard — main entry point.

Initialises the Dash app, registers callbacks, and exposes the WSGI
`server` object consumed by Gunicorn in production.

Run locally:
    python app/app.py

Production (Gunicorn):
    gunicorn --bind 0.0.0.0:8050 app.app:server
"""

import os
import sys
from pathlib import Path

# Allow imports from project root whether we're run from root or from app/
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import dash
import dash_bootstrap_components as dbc

from app.layouts import build_layout
from app.callbacks import register_callbacks

# ── App initialisation ───────────────────────────────────────────────────────

app = dash.Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.DARKLY,          # Bootstrap dark theme
        dbc.icons.BOOTSTRAP,        # Bootstrap icons
        # Google Fonts — Inter for body, JetBrains Mono for numbers
        "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap",
    ],
    suppress_callback_exceptions=True,
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"},
        {"name": "description", "content": "Hospital Operations Dashboard — CMS quality metrics"},
    ],
    title="Hospital Operations Dashboard",
)

# Required by Gunicorn
server = app.server

app.layout = build_layout()
register_callbacks(app)

# ── Custom CSS injected at runtime ───────────────────────────────────────────

app.index_string = """
<!DOCTYPE html>
<html>
<head>
{%metas%}
<title>{%title%}</title>
{%favicon%}
{%css%}
<style>
  /* ── Typography ── */
  body, .dash-table-container { font-family: 'Inter', sans-serif; }

  /* ── Page background slightly darker than Darkly default ── */
  body { background-color: #0f1117 !important; }
  .main-content { background-color: #0f1117; min-height: 100vh; }

  /* ── KPI cards ── */
  .kpi-card {
    background: linear-gradient(135deg, #1a1f2e 0%, #1e2535 100%);
    border: 1px solid #2d3748;
    border-radius: 12px;
    padding: 20px;
    text-align: center;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
  }
  .kpi-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 8px 25px rgba(99, 179, 237, 0.15);
  }
  .kpi-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 2.2rem;
    font-weight: 600;
    color: #63b3ed;
    line-height: 1.1;
  }
  .kpi-label {
    font-size: 0.78rem;
    font-weight: 500;
    color: #a0aec0;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-top: 6px;
  }
  .kpi-delta {
    font-size: 0.75rem;
    color: #68d391;
    margin-top: 4px;
  }

  /* ── Chart cards ── */
  .chart-card {
    background: #1a1f2e;
    border: 1px solid #2d3748;
    border-radius: 12px;
    padding: 16px;
    margin-bottom: 20px;
  }
  .chart-title {
    font-size: 0.85rem;
    font-weight: 600;
    color: #e2e8f0;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 12px;
    padding-bottom: 8px;
    border-bottom: 1px solid #2d3748;
  }

  /* ── Sidebar / filter panel ── */
  .filter-panel {
    background: #1a1f2e;
    border: 1px solid #2d3748;
    border-radius: 12px;
    padding: 20px;
    position: sticky;
    top: 20px;
  }
  .filter-title {
    font-size: 0.8rem;
    font-weight: 600;
    color: #a0aec0;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin-bottom: 16px;
  }
  .filter-label {
    font-size: 0.78rem;
    color: #a0aec0;
    font-weight: 500;
    margin-bottom: 4px;
    margin-top: 12px;
  }

  /* ── Dropdown dark overrides ── */
  .Select-control, .Select-menu-outer {
    background-color: #252d3d !important;
    border-color: #3d4d6a !important;
    color: #e2e8f0 !important;
  }
  .Select-value-label, .Select-placeholder, .Select-option {
    color: #e2e8f0 !important;
  }
  .Select-option.is-focused { background-color: #2d3748 !important; }
  .Select-option.is-selected { background-color: #3182ce !important; }

  /* ── Data table dark theme ── */
  .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner th {
    background-color: #1a2744 !important;
    color: #90cdf4 !important;
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    border-bottom: 2px solid #3182ce !important;
  }
  .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner td {
    background-color: #1a1f2e !important;
    color: #e2e8f0 !important;
    border-color: #2d3748 !important;
    font-size: 0.82rem;
  }
  .dash-table-container .dash-spreadsheet-container .dash-spreadsheet-inner tr:hover td {
    background-color: #252d3d !important;
  }

  /* ── Header ── */
  .dashboard-header {
    background: linear-gradient(135deg, #1a2744 0%, #0f1117 100%);
    border-bottom: 1px solid #2d3748;
    padding: 18px 24px;
    margin-bottom: 24px;
  }
  .header-title {
    font-size: 1.4rem;
    font-weight: 700;
    color: #90cdf4;
    letter-spacing: -0.02em;
  }
  .header-subtitle {
    font-size: 0.78rem;
    color: #718096;
    margin-top: 2px;
  }

  /* ── Status badge ── */
  .status-badge {
    font-size: 0.7rem;
    padding: 3px 10px;
    border-radius: 20px;
    font-weight: 500;
  }

  /* ── Scrollbar ── */
  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: #1a1f2e; }
  ::-webkit-scrollbar-thumb { background: #3d4d6a; border-radius: 3px; }

  /* ── Loading spinner override ── */
  ._dash-loading-callback { background: rgba(15,17,23,0.85) !important; }
</style>
</head>
<body>
{%app_entry%}
<footer>
{%config%}
{%scripts%}
{%renderer%}
</footer>
</body>
</html>
"""

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8050))
    debug = os.environ.get("DASH_DEBUG", "false").lower() == "true"
    app.run(debug=debug, host="0.0.0.0", port=port)
