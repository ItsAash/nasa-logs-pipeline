"""
pages/3_🌐_IP_Intelligence.py
------------------------------
IP behaviour analysis: top IPs, error rate heatmap, first/last seen timeline.
Reads from: gold/ip_behaviour
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="IP Intelligence · NASA Log Monitor",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded",
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils.theme import (inject_css, section, page_title, empty_state,
                          bar_chart, C_CYAN, C_RED, C_AMBER, C_GREEN, C_PURPLE,
                          C_TEXT2, C_TEXT3, C_BORDER, C_SURFACE, C_BG, render_sidebar_nav,
                          )
from utils.db import fetch_ip_behaviour

inject_css()
render_sidebar_nav("ip_intelligence")

page_title("🌐", "IP Intelligence", "ip_behaviour · profiled host analysis")
st.markdown("<div style='height:0.9rem'></div>", unsafe_allow_html=True)

# ── Load data ─────────────────────────────────────────────────────────────────
col_controls, _ = st.columns([2, 3])
with col_controls:
    top_n = st.slider("IPs to display", min_value=10, max_value=100, value=20, step=10, key="ip_top_n")

if st.button("↺  Refresh", key="ip_refresh"):
    st.cache_data.clear()

with st.spinner(""):
    df = fetch_ip_behaviour(limit=500)

if df.empty:
    empty_state("No IP behaviour data available")
    st.stop()

# ── Summary Metrics ───────────────────────────────────────────────────────────
section("Fleet Summary")
m1, m2, m3, m4 = st.columns(4)

total_ips   = len(df)
avg_reqs    = df["total_requests"].mean() if "total_requests" in df.columns else 0
avg_err_rt  = df["error_rate_pct"].mean() if "error_rate_pct" in df.columns else 0
top_ip_req  = df["total_requests"].max() if "total_requests" in df.columns else 0
max_endpoint_breadth = df["unique_endpoints_hit"].max() if "unique_endpoints_hit" in df.columns else 0

def _kpi(col, label, value, color=C_CYAN):
    with col:
        st.markdown(f"""
        <div class="kpi-tile" style="padding:0.9rem 1rem;">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value" style="color:{color};font-size:1.65rem;">{value}</div>
        </div>
        """, unsafe_allow_html=True)

_kpi(m1, "Unique IPs Tracked", f"{total_ips:,}", C_CYAN)
_kpi(m2, "Avg Requests / IP",  f"{avg_reqs:,.0f}", C_GREEN)
_kpi(m3, "Avg Error Rate",     f"{avg_err_rt:.2f}%",
     C_RED if avg_err_rt > 10 else C_AMBER if avg_err_rt > 5 else C_GREEN)
_kpi(m4, "Max Endpoint Breadth",   f"{int(max_endpoint_breadth):,}", C_PURPLE)

st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

# ── Top IPs by Traffic ────────────────────────────────────────────────────────
section("Top IPs by Request Volume")

top_df = df.head(top_n).copy() if "total_requests" in df.columns else df.head(top_n)

if "total_requests" in top_df.columns and "host" in top_df.columns:
    left, right = st.columns([1.4, 1], gap="large")

    with left:
        fig_top = go.Figure()
        fig_top.add_trace(go.Bar(
            x=top_df["total_requests"],
            y=top_df["host"],
            orientation="h",
            marker=dict(
                color=top_df["total_requests"],
                colorscale=[[0, "#1C2B4A"], [0.5, C_CYAN], [1, C_PURPLE]],
                showscale=False,
                line=dict(width=0),
            ),
            hovertemplate="<b>%{y}</b><br>Requests: %{x:,}<extra></extra>",
        ))
        fig_top.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
            height=max(320, top_n * 22),
            margin=dict(l=130, r=20, t=10, b=40),
            xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                       tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
            yaxis=dict(autorange="reversed", linecolor=C_BORDER,
                       tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
            hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                            font=dict(family="JetBrains Mono", size=11)),
        )
        st.plotly_chart(fig_top, width="stretch", config={"displayModeBar": False})

    with right:
        # Error rate for top IPs
        if "error_rate_pct" in top_df.columns:
            top_err = top_df.sort_values("error_rate_pct", ascending=False).head(top_n)
            err_pct = top_err["error_rate_pct"]

            colors = [C_RED if v > 10 else C_AMBER if v > 5 else C_GREEN for v in err_pct]

            fig_err = go.Figure(go.Bar(
                x=err_pct,
                y=top_err["host"],
                orientation="h",
                marker=dict(color=colors, line=dict(width=0)),
                hovertemplate="<b>%{y}</b><br>Error Rate: %{x:.2f}%<extra></extra>",
            ))
            fig_err.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
                title=dict(text="Error Rate by IP (%)", font=dict(size=11, color=C_TEXT2), x=0),
                height=max(320, top_n * 22),
                margin=dict(l=130, r=20, t=30, b=40),
                xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER, ticksuffix="%",
                           tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
                yaxis=dict(autorange="reversed", linecolor=C_BORDER,
                           tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
                hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                                font=dict(family="JetBrains Mono", size=11)),
            )
            st.plotly_chart(fig_err, width="stretch", config={"displayModeBar": False})
        else:
            empty_state("error_rate_pct column not found")
else:
    empty_state("Required columns (host, total_requests) not found")

# ── Error Rate Heatmap ────────────────────────────────────────────────────────
section("Error Rate Heatmap — Request Volume vs Error Rate")

if "error_rate_pct" in df.columns and "total_requests" in df.columns:
    fig_heat = px.density_heatmap(
        df,
        x="total_requests",
        y="error_rate_pct",
        nbinsx=20,
        nbinsy=20,
        color_continuous_scale=["#0D1321", "#1C2B4A", C_AMBER, C_RED],
    )
    fig_heat.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
        height=280,
        margin=dict(l=60, r=20, t=10, b=60),
        xaxis=dict(linecolor=C_BORDER, title="Total Requests",
                   tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
        yaxis=dict(linecolor=C_BORDER, title="Error Rate (%)",
                   tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
        coloraxis_colorbar=dict(
            title="Hosts",
            tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3),
        ),
    )
    st.plotly_chart(fig_heat, width="stretch", config={"displayModeBar": False})
else:
    empty_state("Cannot build heatmap — need total_requests and error_rate_pct columns")

# ── First Seen / Last Seen Table ──────────────────────────────────────────────
section("IP Lifecycle — First Seen vs Last Seen")

fs_cols = [c for c in ["host", "total_requests", "error_rate_pct", "first_seen", "last_seen", "unique_endpoints_hit"]
           if c in df.columns]

if fs_cols:
    fs_df = df[fs_cols].head(top_n).copy()

    # Format error_rate as percentage if present
    if "error_rate_pct" in fs_df.columns:
        fs_df["error_rate_pct"] = fs_df["error_rate_pct"].apply(
            lambda v: f"{float(v):.1f}%" if pd.notna(v) else "—"
        )

    # Colour-code error rate
    def style_row(row):
        styles = []
        for col in fs_df.columns:
            if col == "host":
                styles.append(f"font-family:JetBrains Mono,monospace;font-size:11px;color:{C_CYAN}")
            elif col == "total_requests":
                styles.append(f"font-family:JetBrains Mono,monospace;font-size:11px;color:{C_TEXT2}")
            elif col == "error_rate_pct":
                val = str(row.get("error_rate_pct", "0%")).replace("%", "")
                try:
                    v = float(val)
                    color = C_RED if v > 10 else C_AMBER if v > 5 else C_GREEN
                except ValueError:
                    color = C_TEXT2
                styles.append(f"font-family:JetBrains Mono,monospace;font-size:11px;color:{color};font-weight:600")
            else:
                styles.append(f"font-family:JetBrains Mono,monospace;font-size:11px;color:{C_TEXT3}")
        return styles

    styled = fs_df.style.apply(style_row, axis=1)
    st.dataframe(styled, width="stretch", hide_index=True, height=420)
else:
    empty_state("Lifecycle columns not available (need first_seen, last_seen)")

# ── Requests vs Errors Scatter ────────────────────────────────────────────────
section("Requests vs Error Rate — Anomaly Scatter")

if "total_requests" in df.columns and "error_rate_pct" in df.columns and "host" in df.columns:
    scatter_df = df.head(200).copy()
    scatter_df["error_pct"] = scatter_df["error_rate_pct"]

    # Classify risk quadrants
    def classify(row):
        if row["total_requests"] > scatter_df["total_requests"].quantile(0.75) and row["error_pct"] > 5:
            return "HIGH VOLUME + HIGH ERROR"
        elif row["total_requests"] > scatter_df["total_requests"].quantile(0.75):
            return "HIGH VOLUME"
        elif row["error_pct"] > 10:
            return "HIGH ERROR"
        else:
            return "NORMAL"

    scatter_df["risk"] = scatter_df.apply(classify, axis=1)
    risk_colors = {
        "HIGH VOLUME + HIGH ERROR": C_RED,
        "HIGH VOLUME":              C_AMBER,
        "HIGH ERROR":               "#FF6B35",
        "NORMAL":                   C_CYAN,
    }

    fig_scatter = go.Figure()
    for risk, grp in scatter_df.groupby("risk"):
        fig_scatter.add_trace(go.Scatter(
            x=grp["total_requests"], y=grp["error_pct"],
            mode="markers",
            name=risk,
            text=grp["host"],
            marker=dict(
                color=risk_colors.get(risk, C_CYAN),
                size=7, opacity=0.75,
                line=dict(width=0.5, color=C_BG),
            ),
            hovertemplate="<b>%{text}</b><br>Requests: %{x:,}<br>Error Rate: %{y:.1f}%<extra></extra>",
        ))

    fig_scatter.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
        height=300, margin=dict(l=50, r=20, t=10, b=50),
        xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                   title="Total Requests",
                   tickfont=dict(family="JetBrains Mono", size=10, color=C_TEXT3)),
        yaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                   title="Error Rate (%)", ticksuffix="%",
                   tickfont=dict(family="JetBrains Mono", size=10, color=C_TEXT3)),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10, color=C_TEXT2)),
        hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                        font=dict(family="JetBrains Mono", size=11)),
    )
    st.plotly_chart(fig_scatter, width="stretch", config={"displayModeBar": False})
else:
    empty_state("Scatter plot requires total_requests and error_rate_pct columns")

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="margin-top:1.5rem;padding-top:0.75rem;border-top:1px solid {C_BORDER};
            font-family:'JetBrains Mono',monospace;font-size:0.65rem;color:{C_TEXT3};">
    Source: <code>/Volumes/workspace/default/logs_volume/gold/ip_behaviour</code>
    &nbsp;·&nbsp; Cache TTL: 120s
</div>
""", unsafe_allow_html=True)
