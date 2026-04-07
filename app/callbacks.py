"""
Dash callbacks — all interactivity lives here.

Single master callback pattern: all filter inputs → all chart outputs.
This avoids multiple DB round-trips and keeps state consistent across charts.

Additional callbacks:
  - Reset button clears all filters
  - Record count badge updates in sidebar
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output, State, callback, no_update
from sqlalchemy import create_engine, text

# ── DB connection ──────────────────────────────────────────────────────────────

_engine = None


def _get_engine():
    global _engine
    if _engine is not None:
        return _engine
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        return None
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    try:
        _engine = create_engine(db_url, pool_pre_ping=True, pool_size=3, max_overflow=5)
        return _engine
    except Exception:
        return None


# ── Plotly theme constants ─────────────────────────────────────────────────────

PLOTLY_TEMPLATE = "plotly_dark"
BG_COLOR = "#1a1f2e"
PAPER_COLOR = "#1a1f2e"
GRID_COLOR = "#2d3748"
ACCENT_COLORS = ["#63b3ed", "#68d391", "#fbd38d", "#fc8181", "#b794f4", "#76e4f7", "#f6ad55"]

STAR_PALETTE = {
    1: "#fc8181",
    2: "#f6ad55",
    3: "#fbd38d",
    4: "#68d391",
    5: "#48bb78",
}

_EMPTY_FIG = go.Figure(
    layout=go.Layout(
        template=PLOTLY_TEMPLATE,
        paper_bgcolor=BG_COLOR,
        plot_bgcolor=BG_COLOR,
        annotations=[{
            "text": "No data — run ETL pipeline or check DATABASE_URL",
            "xref": "paper", "yref": "paper",
            "x": 0.5, "y": 0.5,
            "showarrow": False,
            "font": {"size": 14, "color": "#718096"},
        }],
    )
)


def _base_layout(title: str = "") -> dict:
    """Common layout kwargs applied to all figures."""
    return {
        "template": PLOTLY_TEMPLATE,
        "paper_bgcolor": BG_COLOR,
        "plot_bgcolor": BG_COLOR,
        "font": {"family": "Inter, sans-serif", "color": "#e2e8f0"},
        "title": {"text": title, "font": {"size": 13, "color": "#a0aec0"}} if title else None,
        "margin": {"t": 30, "b": 40, "l": 50, "r": 20},
        "showlegend": False,
    }


# ── Data fetching ──────────────────────────────────────────────────────────────

def _fetch_hospitals(
    states: list | None,
    types: list | None,
    rating_range: list | None,
    ownerships: list | None,
) -> pd.DataFrame:
    """Return filtered hospitals DataFrame. Returns empty DF on error."""
    engine = _get_engine()
    if engine is None:
        return pd.DataFrame()

    conditions = ["1=1"]
    params: dict[str, Any] = {}

    if states:
        conditions.append("state = ANY(:states)")
        params["states"] = states
    if types:
        conditions.append("hospital_type = ANY(:types)")
        params["types"] = types
    if ownerships:
        conditions.append("ownership = ANY(:ownerships)")
        params["ownerships"] = ownerships
    if rating_range and len(rating_range) == 2:
        conditions.append("overall_rating BETWEEN :r_min AND :r_max")
        params["r_min"] = rating_range[0]
        params["r_max"] = rating_range[1]

    query = f"""
        SELECT
            provider_id, name, state, city, hospital_type, ownership,
            emergency_services, overall_rating,
            mortality_national_comparison, safety_national_comparison,
            readmission_national_comparison, patient_experience_national_comparison,
            effectiveness_national_comparison, timeliness_national_comparison,
            efficient_use_national_comparison
        FROM hospitals
        WHERE {" AND ".join(conditions)}
        ORDER BY overall_rating DESC NULLS LAST, name
        LIMIT 5000
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn, params=params)
    except Exception as exc:
        print(f"[callback] DB error: {exc}")
        return pd.DataFrame()


# ── Score derivation ───────────────────────────────────────────────────────────

_COMPARISON_SCORE = {
    "Above the national average": 3.0,
    "Same as the national average": 2.0,
    "Below the national average": 1.0,
}


def _comparison_score(series: pd.Series) -> pd.Series:
    return series.map(_COMPARISON_SCORE)


# ── Chart builders ─────────────────────────────────────────────────────────────

def _chart_top_states(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return _EMPTY_FIG
    counts = (
        df.groupby("state")
        .size()
        .nlargest(10)
        .reset_index(name="count")
        .sort_values("count", ascending=True)   # ascending so largest is at top of hbar
    )
    fig = px.bar(
        counts,
        x="count",
        y="state",
        orientation="h",
        text="count",
        color="count",
        color_continuous_scale=[[0, "#2d3748"], [1, "#63b3ed"]],
    )
    fig.update_traces(
        textfont_size=11,
        textposition="outside",
        marker_line_width=0,
    )
    fig.update_layout(
        **_base_layout(),
        coloraxis_showscale=False,
        xaxis_title="",
        yaxis_title="",
        xaxis_showgrid=False,
    )
    return fig


def _chart_rating_dist(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return _EMPTY_FIG
    rated = df[df["overall_rating"].notna()].copy()
    rated["overall_rating"] = rated["overall_rating"].astype(int)
    counts = rated["overall_rating"].value_counts().sort_index()

    colors = [STAR_PALETTE.get(r, "#63b3ed") for r in counts.index]
    labels = [f"{'★' * r}{'☆' * (5 - r)}" for r in counts.index]

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=counts.values,
            hole=0.52,
            marker={"colors": colors, "line": {"color": "#1a1f2e", "width": 2}},
            textinfo="percent",
            textfont_size=12,
            hovertemplate="<b>%{label}</b><br>Count: %{value:,}<br>%{percent}<extra></extra>",
            sort=False,
        )
    )
    fig.update_layout(
        **_base_layout(),
        legend={
            "orientation": "v",
            "x": 1.02, "y": 0.5,
            "font": {"size": 11, "color": "#a0aec0"},
        },
        annotations=[{
            "text": f"<b>{len(rated):,}</b><br><span style='font-size:10px'>Rated</span>",
            "x": 0.5, "y": 0.5,
            "font_size": 16,
            "showarrow": False,
            "font_color": "#e2e8f0",
        }],
    )
    return fig


def _chart_hospital_type(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return _EMPTY_FIG
    counts = (
        df.groupby("hospital_type")
        .agg(count=("provider_id", "count"), avg_rating=("overall_rating", "mean"))
        .reset_index()
        .sort_values("count", ascending=False)
    )
    # Truncate long names for display
    counts["label"] = counts["hospital_type"].str.replace(r"\s+Hospitals?", "", regex=True).str.strip()

    fig = px.bar(
        counts,
        x="label",
        y="count",
        text="count",
        color="avg_rating",
        color_continuous_scale=[[0, "#fc8181"], [0.5, "#fbd38d"], [1, "#68d391"]],
        custom_data=["hospital_type", "avg_rating"],
    )
    fig.update_traces(
        textfont_size=11,
        textposition="outside",
        marker_line_width=0,
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>"
            "Count: %{y:,}<br>"
            "Avg Rating: %{customdata[1]:.2f}★<extra></extra>"
        ),
    )
    fig.update_layout(
        **_base_layout(),
        coloraxis_colorbar={
            "title": "Avg ★",
            "tickfont": {"size": 10, "color": "#a0aec0"},
            "title_font": {"size": 10, "color": "#a0aec0"},
            "thickness": 12,
            "len": 0.6,
        },
        xaxis_title="",
        yaxis_title="Count",
        xaxis_tickangle=-20,
    )
    return fig


def _chart_ownership(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return _EMPTY_FIG
    counts = (
        df.groupby("ownership")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    # Shorten labels
    short = {
        "Voluntary non-profit - Private": "Non-profit (Private)",
        "Voluntary non-profit - Church": "Non-profit (Church)",
        "Voluntary non-profit - Other": "Non-profit (Other)",
        "Government - State": "Gov (State)",
        "Government - Local": "Gov (Local)",
        "Government - Federal": "Gov (Federal)",
        "Proprietary": "For-profit",
    }
    counts["label"] = counts["ownership"].map(short).fillna(counts["ownership"])

    fig = px.bar(
        counts,
        x="count",
        y="label",
        orientation="h",
        text="count",
        color_discrete_sequence=ACCENT_COLORS,
        color="label",
    )
    fig.update_traces(
        textfont_size=11,
        textposition="outside",
        marker_line_width=0,
    )
    fig.update_layout(
        **_base_layout(),
        xaxis_title="",
        yaxis_title="",
        xaxis_showgrid=False,
        yaxis={"categoryorder": "total ascending"},
    )
    return fig


def _chart_scatter(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return _EMPTY_FIG

    plot_df = df.copy()
    # Derive numeric scores from categorical fields
    plot_df["pt_exp_score"] = _comparison_score(plot_df["patient_experience_national_comparison"])
    plot_df["readmission_score"] = _comparison_score(plot_df["readmission_national_comparison"])
    plot_df["rating_str"] = plot_df["overall_rating"].apply(
        lambda r: f"{'★' * int(r)}{'☆' * (5 - int(r))}" if pd.notna(r) else "Unrated"
    )

    # Drop rows missing both axes
    plot_df = plot_df.dropna(subset=["pt_exp_score", "readmission_score"])

    if plot_df.empty:
        return _EMPTY_FIG

    # Add jitter so overlapping points separate
    import numpy as np
    rng = np.random.default_rng(42)
    plot_df["pt_exp_jitter"] = plot_df["pt_exp_score"] + rng.uniform(-0.12, 0.12, len(plot_df))
    plot_df["readm_jitter"] = plot_df["readmission_score"] + rng.uniform(-0.12, 0.12, len(plot_df))

    color_map = {
        f"{'★' * r}{'☆' * (5-r)}": STAR_PALETTE[r] for r in range(1, 6)
    }
    color_map["Unrated"] = "#718096"

    fig = px.scatter(
        plot_df,
        x="pt_exp_jitter",
        y="readm_jitter",
        color="rating_str",
        color_discrete_map=color_map,
        hover_name="name",
        custom_data=["state", "hospital_type", "overall_rating",
                     "patient_experience_national_comparison",
                     "readmission_national_comparison"],
        opacity=0.75,
        size_max=8,
    )
    fig.update_traces(
        marker={"size": 7, "line": {"width": 0.5, "color": "#1a1f2e"}},
        hovertemplate=(
            "<b>%{hovertext}</b><br>"
            "State: %{customdata[0]}<br>"
            "Type: %{customdata[1]}<br>"
            "Rating: %{customdata[2]}★<br>"
            "Patient Experience: %{customdata[3]}<br>"
            "Readmission: %{customdata[4]}<extra></extra>"
        ),
    )
    fig.update_layout(
        **_base_layout(),
        legend={
            "title": "Star Rating",
            "font": {"size": 11, "color": "#a0aec0"},
            "bgcolor": "rgba(26,31,46,0.8)",
            "bordercolor": "#2d3748",
            "borderwidth": 1,
        },
        xaxis={
            "title": "Patient Experience Score  (1=Below Avg · 2=Same · 3=Above Avg)",
            "tickvals": [1, 2, 3],
            "ticktext": ["Below Avg", "Same", "Above Avg"],
            "gridcolor": GRID_COLOR,
            "range": [0.6, 3.4],
        },
        yaxis={
            "title": "Readmission Performance  (1=Worse · 3=Better)",
            "tickvals": [1, 2, 3],
            "ticktext": ["Below Avg", "Same", "Above Avg"],
            "gridcolor": GRID_COLOR,
            "range": [0.6, 3.4],
        },
    )
    return fig


def _build_table_data(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    top = df.nlargest(200, "overall_rating", keep="all").head(200)
    # Shorten comparison strings for table display
    shorten_map = {
        "Above the national average": "↑ Above Avg",
        "Same as the national average": "→ Same",
        "Below the national average": "↓ Below Avg",
    }
    for col in ["mortality_national_comparison", "safety_national_comparison",
                "readmission_national_comparison", "patient_experience_national_comparison"]:
        if col in top.columns:
            top[col] = top[col].map(shorten_map).fillna(top[col])
    top["emergency_services"] = top["emergency_services"].map(
        {True: "Yes", False: "No", None: "—"}
    ).fillna("—")
    return top.to_dict("records")


# ── KPI builders ───────────────────────────────────────────────────────────────

def _kpi_values(df: pd.DataFrame) -> dict[str, str]:
    if df.empty:
        return {k: "—" for k in [
            "total", "avg_rating", "pct_high", "pct_er", "rated_pct", "states"
        ]}

    total = len(df)
    rated = df["overall_rating"].notna()
    avg_rating = df.loc[rated, "overall_rating"].mean()
    pct_high = (df.loc[rated, "overall_rating"] >= 4).sum() / rated.sum() * 100 if rated.sum() else 0
    pct_er = (df["emergency_services"] == True).sum() / total * 100
    rated_pct = rated.sum() / total * 100
    states = df["state"].nunique()

    return {
        "total": f"{total:,}",
        "avg_rating": f"{avg_rating:.2f} ★" if not pd.isna(avg_rating) else "—",
        "pct_high": f"{pct_high:.1f}%",
        "pct_er": f"{pct_er:.1f}%",
        "rated_pct": f"{rated_pct:.1f}%",
        "states": str(states),
    }


# ── Callback registration ──────────────────────────────────────────────────────

def register_callbacks(app):

    # ── Master data callback ────────────────────────────────────────────────────
    @app.callback(
        # KPIs
        Output("kpi-total-hospitals",     "children"),
        Output("kpi-avg-rating",          "children"),
        Output("kpi-pct-high-performers", "children"),
        Output("kpi-pct-er",              "children"),
        Output("kpi-rated-pct",           "children"),
        Output("kpi-states-covered",      "children"),
        # Charts
        Output("chart-top-states",        "figure"),
        Output("chart-rating-dist",       "figure"),
        Output("chart-hospital-type",     "figure"),
        Output("chart-ownership",         "figure"),
        Output("chart-scatter",           "figure"),
        # Table
        Output("hospital-table",          "data"),
        # Record count badge
        Output("record-count",            "children"),
        # Inputs
        Input("filter-state",             "value"),
        Input("filter-type",              "value"),
        Input("filter-rating",            "value"),
        Input("filter-ownership",         "value"),
    )
    def update_dashboard(states, types, rating_range, ownerships):
        df = _fetch_hospitals(states, types, rating_range, ownerships)

        kpis = _kpi_values(df)
        record_msg = f"{len(df):,} hospitals match current filters" if not df.empty else "No data loaded"

        return (
            kpis["total"],
            kpis["avg_rating"],
            kpis["pct_high"],
            kpis["pct_er"],
            kpis["rated_pct"],
            kpis["states"],
            _chart_top_states(df),
            _chart_rating_dist(df),
            _chart_hospital_type(df),
            _chart_ownership(df),
            _chart_scatter(df),
            _build_table_data(df),
            record_msg,
        )

    # ── Reset button ────────────────────────────────────────────────────────────
    @app.callback(
        Output("filter-state",     "value"),
        Output("filter-type",      "value"),
        Output("filter-rating",    "value"),
        Output("filter-ownership", "value"),
        Input("btn-reset",         "n_clicks"),
        prevent_initial_call=True,
    )
    def reset_filters(n_clicks):
        return None, None, [1, 5], None
