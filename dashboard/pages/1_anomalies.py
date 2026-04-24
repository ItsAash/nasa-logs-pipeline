"""
pages/1_🚨_Anomalies.py
-----------------------
Real-time anomaly detection feed.
Auto-refreshes every 30 seconds. Sorted CRITICAL → LOW.
"""

import streamlit as st
import pandas as pd
import streamlit.components.v1 as components

st.set_page_config(
    page_title="Anomalies · NASA Log Monitor",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded",
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils.theme import (inject_css, section, page_title, empty_state,
                          bar_chart, pie_chart,
                          C_CYAN, C_RED, C_AMBER, C_GREEN, C_TEXT2, C_TEXT3,
                          C_BORDER, C_SURFACE, SEVERITY_COLORS, render_sidebar_nav,
                          )
from utils.db import fetch_anomalies

inject_css()
render_sidebar_nav("anomalies")

# ── Auto-refresh every 30 seconds ────────────────────────────────────────────
components.html(
    """
    <script>
    window.setTimeout(function () {
        window.parent.location.reload();
    }, 30000);
    </script>
    """,
    height=0,
)

page_title("🚨", "Real-Time Anomalies",
           "auto-refresh · every 30s")
st.markdown("<div style='height:0.9rem'></div>", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner(""):
    df = fetch_anomalies(limit=1000)

if df.empty:
    empty_state("⚡ No anomalies in the detection stream yet")
    st.stop()

# ── Filters ───────────────────────────────────────────────────────────────────
section("Filters")
f1, f2, f3 = st.columns([1, 1, 2])

all_types = sorted(df["anomaly_type"].dropna().unique().tolist()) if "anomaly_type" in df.columns else []
all_sevs  = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
avail_sevs = [s for s in all_sevs if s in df["severity"].dropna().unique().tolist()] if "severity" in df.columns else all_sevs

with f1:
    sel_severity = st.multiselect(
        "Severity",
        options=all_sevs,
        default=avail_sevs,
        key="sev_filter",
    )

with f2:
    sel_type = st.multiselect(
        "Anomaly Type",
        options=all_types,
        default=all_types,
        key="type_filter",
    )

with f3:
    search_ip = st.text_input(
        "Search IP / Host",
        placeholder="e.g. 199.72.81.55",
        key="ip_search",
    )

# Apply filters
filtered = df.copy()
if sel_severity and "severity" in filtered.columns:
    filtered = filtered[filtered["severity"].isin(sel_severity)]
if sel_type and "anomaly_type" in filtered.columns:
    filtered = filtered[filtered["anomaly_type"].isin(sel_type)]
if search_ip and "host" in filtered.columns:
    filtered = filtered[filtered["host"].str.contains(search_ip, case=False, na=False)]

# ── Summary Metrics Row ───────────────────────────────────────────────────────
section("Summary")
m1, m2, m3, m4, m5 = st.columns(5)

sev_counts = filtered["severity"].value_counts() if "severity" in filtered.columns else pd.Series()

def _metric(col, label, value, color=C_CYAN):
    with col:
        st.markdown(f"""
        <div class="kpi-tile" style="padding:0.85rem 1rem;">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value" style="color:{color};font-size:1.7rem;">{value}</div>
        </div>
        """, unsafe_allow_html=True)

_metric(m1, "Showing",   f"{len(filtered):,}",                      C_CYAN)
_metric(m2, "Critical",  f"{sev_counts.get('CRITICAL', 0):,}",      C_RED)
_metric(m3, "High",      f"{sev_counts.get('HIGH', 0):,}",          "#FF6B35")
_metric(m4, "Medium",    f"{sev_counts.get('MEDIUM', 0):,}",        C_AMBER)
_metric(m5, "Low",       f"{sev_counts.get('LOW', 0):,}",           C_GREEN)

st.markdown("<div style='height:0.75rem'></div>", unsafe_allow_html=True)

# ── Charts Row ────────────────────────────────────────────────────────────────
chart_left, chart_right = st.columns([1.4, 1], gap="large")

with chart_left:
    section("Top Anomalous IPs")
    if "host" in filtered.columns:
        top_ips = (
            filtered["host"].value_counts()
            .head(15)
            .reset_index()
            .rename(columns={"index": "host", "host": "count", "count": "count"})
        )
        # value_counts().reset_index() gives columns: [host, count] in pandas 2.x
        if top_ips.shape[1] == 2:
            top_ips.columns = ["host", "count"]
        fig = bar_chart(top_ips, x="host", y="count",
                        title="", horizontal=True, color=C_CYAN)
        fig.update_layout(height=320, margin=dict(l=130, r=20, t=10, b=40))
        st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
    else:
        empty_state("host column not found")

with chart_right:
    section("Severity Distribution")
    if "severity" in filtered.columns and not filtered.empty:
        sev_df = (
            filtered["severity"].value_counts()
            .reset_index()
        )
        sev_df.columns = ["severity", "count"]
        fig2 = pie_chart(sev_df, names_col="severity", values_col="count", title="")
        fig2.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig2, width="stretch", config={"displayModeBar": False})
    else:
        empty_state("No severity data")

# ── Anomaly Type Breakdown ────────────────────────────────────────────────────
if "anomaly_type" in filtered.columns:
    section("Anomaly Type Breakdown")
    type_counts = (
        filtered.groupby("anomaly_type")["severity"].value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    # Ensure severity columns exist
    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        if sev not in type_counts.columns:
            type_counts[sev] = 0

    import plotly.graph_objects as go
    fig3 = go.Figure()
    sev_color_map = {"CRITICAL": C_RED, "HIGH": "#FF6B35", "MEDIUM": C_AMBER, "LOW": C_GREEN}
    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        if sev in type_counts.columns:
            fig3.add_trace(go.Bar(
                name=sev, x=type_counts["anomaly_type"], y=type_counts[sev],
                marker_color=sev_color_map[sev],
                hovertemplate=f"<b>{sev}</b>: %{{y:,}}<extra></extra>",
            ))
    fig3.update_layout(
        barmode="stack",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
        margin=dict(l=40, r=10, t=10, b=60), height=220,
        xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                   tickfont=dict(family="JetBrains Mono", size=10, color=C_TEXT3)),
        yaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                   tickfont=dict(family="JetBrains Mono", size=10, color=C_TEXT3)),
        legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h",
                    yanchor="bottom", y=1.02, xanchor="right", x=1,
                    font=dict(size=10, color=C_TEXT2)),
        hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                        font=dict(family="JetBrains Mono", size=11)),
    )
    st.plotly_chart(fig3, width="stretch", config={"displayModeBar": False})

# ── Anomaly Table ─────────────────────────────────────────────────────────────
section(f"Anomaly Feed — {len(filtered):,} records")

display_cols = [c for c in ["window_start", "host", "anomaly_type", "severity", "rate_multiplier", "global_z_score"]
                if c in filtered.columns]
table_df = filtered[display_cols].copy() if display_cols else filtered.copy()

# Severity ordering
sev_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
if "severity" in table_df.columns:
    table_df["_rank"] = table_df["severity"].map(sev_order).fillna(99)
    table_df = table_df.sort_values("_rank").drop(columns=["_rank"])

# Color-code severity with pandas Styler
def style_severity(val):
    color = SEVERITY_COLORS.get(str(val).upper(), C_TEXT2)
    return f"color: {color}; font-weight: 600; font-family: JetBrains Mono, monospace; font-size: 11px;"

if "severity" in table_df.columns:
    styled_df = (
        table_df.style
        .applymap(style_severity, subset=["severity"])
        .set_properties(**{
            "font-family": "JetBrains Mono, monospace",
            "font-size": "11px",
            "color": C_TEXT2,
        })
    )
    st.dataframe(styled_df, width="stretch", hide_index=True, height=500)
else:
    st.dataframe(table_df, width="stretch", hide_index=True, height=500)

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="margin-top:1.5rem;padding-top:0.75rem;border-top:1px solid {C_BORDER};
            font-family:'JetBrains Mono',monospace;font-size:0.65rem;color:{C_TEXT3};">
    Source: <code>/Volumes/workspace/default/logs_volume/output/anomalies</code>
    &nbsp;·&nbsp; Cache TTL: 30s &nbsp;·&nbsp; Auto-refreshing every 30s
</div>
""", unsafe_allow_html=True)
