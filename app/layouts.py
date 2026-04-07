"""
Dashboard layout — all UI structure defined here.

Callbacks are wired separately in callbacks.py to keep this file
purely declarative and easy to read/adjust.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
from sqlalchemy import create_engine, text


# ── DB helper (read-only, used only to populate filter dropdowns) ─────────────

def _get_engine():
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        return None
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    try:
        return create_engine(db_url, pool_pre_ping=True)
    except Exception:
        return None


def _load_filter_options() -> dict[str, list]:
    """Fetch distinct values for dropdowns. Returns empty lists on DB error."""
    engine = _get_engine()
    if engine is None:
        return {"states": [], "types": [], "ownerships": []}
    try:
        with engine.connect() as conn:
            states = [
                r[0] for r in conn.execute(
                    text("SELECT DISTINCT state FROM hospitals WHERE state IS NOT NULL ORDER BY state")
                )
            ]
            types = [
                r[0] for r in conn.execute(
                    text("SELECT DISTINCT hospital_type FROM hospitals WHERE hospital_type IS NOT NULL ORDER BY hospital_type")
                )
            ]
            ownerships = [
                r[0] for r in conn.execute(
                    text("SELECT DISTINCT ownership FROM hospitals WHERE ownership IS NOT NULL ORDER BY ownership")
                )
            ]
        return {"states": states, "types": types, "ownerships": ownerships}
    except Exception:
        return {"states": [], "types": [], "ownerships": []}


# ── Reusable component builders ───────────────────────────────────────────────

def _kpi_card(card_id: str, label: str, icon: str, accent_color: str = "#63b3ed") -> dbc.Col:
    return dbc.Col(
        html.Div(
            [
                html.Div(
                    html.I(className=f"bi {icon}", style={"fontSize": "1.4rem", "color": accent_color}),
                    style={"marginBottom": "8px"},
                ),
                html.Div(id=card_id, className="kpi-value", style={"color": accent_color},
                         children="—"),
                html.Div(label, className="kpi-label"),
            ],
            className="kpi-card",
        ),
        xs=6, sm=4, md=2,
    )


def _chart_card(title: str, graph_id: str, height: int = 360) -> html.Div:
    return html.Div(
        [
            html.Div(title, className="chart-title"),
            dcc.Graph(
                id=graph_id,
                config={"displayModeBar": False, "responsive": True},
                style={"height": f"{height}px"},
            ),
        ],
        className="chart-card",
    )


def _dropdown(
    dropdown_id: str,
    placeholder: str,
    options: list[str],
    multi: bool = True,
) -> dcc.Dropdown:
    return dcc.Dropdown(
        id=dropdown_id,
        options=[{"label": v, "value": v} for v in options],
        placeholder=placeholder,
        multi=multi,
        clearable=True,
        style={
            "backgroundColor": "#252d3d",
            "color": "#e2e8f0",
            "border": "1px solid #3d4d6a",
            "borderRadius": "6px",
            "fontSize": "0.82rem",
        },
    )


# ── Filter sidebar ────────────────────────────────────────────────────────────

def _filter_panel(opts: dict) -> html.Div:
    return html.Div(
        [
            html.Div(
                [
                    html.I(className="bi bi-funnel-fill me-2", style={"color": "#63b3ed"}),
                    "Filters",
                ],
                className="filter-title",
            ),

            html.Div("State", className="filter-label"),
            _dropdown("filter-state", "All States", opts["states"], multi=True),

            html.Div("Hospital Type", className="filter-label"),
            _dropdown("filter-type", "All Types", opts["types"], multi=True),

            html.Div("CMS Star Rating", className="filter-label"),
            dcc.RangeSlider(
                id="filter-rating",
                min=1, max=5, step=1,
                value=[1, 5],
                marks={i: {"label": f"★{i}", "style": {"color": "#a0aec0", "fontSize": "0.72rem"}}
                       for i in range(1, 6)},
                tooltip={"placement": "bottom", "always_visible": False},
            ),

            html.Div(style={"marginTop": "12px"}),
            html.Div("Ownership Type", className="filter-label"),
            _dropdown("filter-ownership", "All Ownership Types", opts["ownerships"], multi=True),

            html.Hr(style={"borderColor": "#2d3748", "margin": "20px 0"}),

            dbc.Button(
                [html.I(className="bi bi-arrow-counterclockwise me-2"), "Reset Filters"],
                id="btn-reset",
                color="secondary",
                outline=True,
                size="sm",
                className="w-100",
                style={"fontSize": "0.78rem"},
            ),

            html.Div(
                id="record-count",
                style={"fontSize": "0.72rem", "color": "#718096", "marginTop": "12px", "textAlign": "center"},
            ),
        ],
        className="filter-panel",
    )


# ── Header ────────────────────────────────────────────────────────────────────

def _header() -> html.Div:
    return html.Div(
        dbc.Container(
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Div(
                                [
                                    html.I(className="bi bi-hospital me-3",
                                           style={"fontSize": "1.6rem", "color": "#63b3ed"}),
                                    html.Span("Hospital Operations Dashboard",
                                              className="header-title"),
                                ],
                                style={"display": "flex", "alignItems": "center"},
                            ),
                            html.Div(
                                "CMS Hospital Compare · Quality & Performance Analytics",
                                className="header-subtitle",
                            ),
                        ],
                        md=8,
                    ),
                    dbc.Col(
                        [
                            dbc.Badge(
                                "LIVE DATA",
                                color="success",
                                className="status-badge me-2",
                            ),
                            html.Span(
                                "Source: CMS data.cms.gov",
                                style={"fontSize": "0.72rem", "color": "#718096"},
                            ),
                        ],
                        md=4,
                        className="d-flex align-items-center justify-content-end",
                    ),
                ],
                align="center",
            ),
            fluid=True,
        ),
        className="dashboard-header",
    )


# ── KPI row ───────────────────────────────────────────────────────────────────

def _kpi_row() -> dbc.Row:
    cards = [
        ("kpi-total-hospitals",     "Total Hospitals",              "bi-building-fill-cross",     "#63b3ed"),
        ("kpi-avg-rating",          "Avg CMS Star Rating",          "bi-star-fill",               "#fbd38d"),
        ("kpi-pct-high-performers", "High Performers (4-5★)",       "bi-award-fill",              "#68d391"),
        ("kpi-pct-er",              "With Emergency Services",       "bi-heart-pulse-fill",        "#fc8181"),
        ("kpi-rated-pct",           "Hospitals Rated",               "bi-clipboard2-pulse-fill",   "#b794f4"),
        ("kpi-states-covered",      "States Covered",                "bi-geo-alt-fill",            "#76e4f7"),
    ]
    return dbc.Row(
        [_kpi_card(cid, label, icon, color) for cid, label, icon, color in cards],
        className="g-3 mb-4",
    )


# ── Data table ────────────────────────────────────────────────────────────────

def _data_table() -> html.Div:
    return html.Div(
        [
            html.Div("Top Hospitals — Sortable Detail View", className="chart-title"),
            dash_table.DataTable(
                id="hospital-table",
                columns=[
                    {"name": "Provider ID",   "id": "provider_id"},
                    {"name": "Hospital Name", "id": "name"},
                    {"name": "State",         "id": "state"},
                    {"name": "City",          "id": "city"},
                    {"name": "Type",          "id": "hospital_type"},
                    {"name": "Ownership",     "id": "ownership"},
                    {"name": "★ Rating",      "id": "overall_rating"},
                    {"name": "Mortality",     "id": "mortality_national_comparison"},
                    {"name": "Safety",        "id": "safety_national_comparison"},
                    {"name": "Readmission",   "id": "readmission_national_comparison"},
                    {"name": "Pt. Experience","id": "patient_experience_national_comparison"},
                    {"name": "Emergency",     "id": "emergency_services"},
                ],
                data=[],
                sort_action="native",
                sort_mode="single",
                filter_action="native",
                page_action="native",
                page_size=20,
                style_table={"overflowX": "auto"},
                style_header={
                    "backgroundColor": "#1a2744",
                    "color": "#90cdf4",
                    "fontWeight": "600",
                    "fontSize": "0.73rem",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.06em",
                    "border": "1px solid #2d3748",
                },
                style_cell={
                    "backgroundColor": "#1a1f2e",
                    "color": "#e2e8f0",
                    "border": "1px solid #2d3748",
                    "fontSize": "0.8rem",
                    "padding": "10px 12px",
                    "textAlign": "left",
                    "whiteSpace": "normal",
                    "maxWidth": "220px",
                    "overflow": "hidden",
                    "textOverflow": "ellipsis",
                },
                style_data_conditional=[
                    {
                        "if": {"row_index": "odd"},
                        "backgroundColor": "#16202e",
                    },
                    {
                        "if": {"filter_query": '{overall_rating} = 5'},
                        "color": "#fbd38d",
                    },
                    {
                        "if": {"filter_query": '{overall_rating} = 1'},
                        "color": "#fc8181",
                    },
                ],
            ),
        ],
        className="chart-card",
    )


# ── Main layout builder ───────────────────────────────────────────────────────

def build_layout() -> html.Div:
    opts = _load_filter_options()

    return html.Div(
        [
            _header(),

            dbc.Container(
                [
                    dbc.Row(
                        [
                            # ── Sidebar ──────────────────────────────────────
                            dbc.Col(
                                _filter_panel(opts),
                                xs=12, md=3, lg=2,
                                className="mb-4",
                            ),

                            # ── Main content ──────────────────────────────────
                            dbc.Col(
                                [
                                    # KPI row
                                    _kpi_row(),

                                    # Row 1: Top states + Rating distribution
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                _chart_card(
                                                    "Top 10 States by Hospital Count",
                                                    "chart-top-states",
                                                    height=340,
                                                ),
                                                md=7,
                                            ),
                                            dbc.Col(
                                                _chart_card(
                                                    "CMS Star Rating Distribution",
                                                    "chart-rating-dist",
                                                    height=340,
                                                ),
                                                md=5,
                                            ),
                                        ],
                                        className="g-3",
                                    ),

                                    # Row 2: Hospital type + Ownership
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                _chart_card(
                                                    "Hospital Type Breakdown",
                                                    "chart-hospital-type",
                                                    height=340,
                                                ),
                                                md=6,
                                            ),
                                            dbc.Col(
                                                _chart_card(
                                                    "Ownership Type Distribution",
                                                    "chart-ownership",
                                                    height=340,
                                                ),
                                                md=6,
                                            ),
                                        ],
                                        className="g-3",
                                    ),

                                    # Row 3: Scatter plot full width
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                _chart_card(
                                                    "Patient Experience vs. Readmission Performance  (colored by CMS Star Rating)",
                                                    "chart-scatter",
                                                    height=420,
                                                ),
                                                md=12,
                                            ),
                                        ],
                                        className="g-3",
                                    ),

                                    # Data table
                                    _data_table(),

                                    # Footer
                                    html.Div(
                                        [
                                            html.Hr(style={"borderColor": "#2d3748"}),
                                            html.P(
                                                [
                                                    "Built by ",
                                                    html.Strong("Shilp Patel", style={"color": "#90cdf4"}),
                                                    " · M.S. Advanced Data Analytics, University of North Texas  ·  ",
                                                    html.A("data.cms.gov", href="https://data.cms.gov",
                                                           target="_blank", style={"color": "#63b3ed"}),
                                                ],
                                                style={"fontSize": "0.75rem", "color": "#718096",
                                                       "textAlign": "center", "padding": "12px 0"},
                                            ),
                                        ]
                                    ),
                                ],
                                xs=12, md=9, lg=10,
                            ),
                        ],
                    ),
                ],
                fluid=True,
                style={"padding": "0 20px 40px"},
            ),
        ],
        className="main-content",
    )
