"""
pages/2_📈_Traffic.py
---------------------
Traffic overview: requests over time, error rate trend, rolling average.
Reads from: gold/hourly_stats
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

st.set_page_config(
    page_title="Traffic · NASA Log Monitor",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils.theme import (inject_css, section, page_title, empty_state,
                          C_CYAN, C_RED, C_AMBER, C_GREEN, C_PURPLE,
                          C_TEXT2, C_TEXT3, C_BORDER, C_SURFACE, render_sidebar_nav,
                          )
from utils.db import fetch_hourly_stats

inject_css()
render_sidebar_nav("traffic")

page_title("📈", "Traffic Overview", "hourly_stats · Spark windowed aggregation")
st.markdown("<div style='height:0.9rem'></div>", unsafe_allow_html=True)

# ── Time Range Selector ───────────────────────────────────────────────────────
section("Time Range")
t1, t2, _ = st.columns([1, 1, 3])
with t1:
    window = st.selectbox(
        "Lookback window",
        options=[24, 48, 72, 168, 336, 720],
        format_func=lambda h: f"Last {h}h" if h < 168 else f"Last {h//24}d",
        index=2,
        key="traffic_window",
    )
with t2:
    st.markdown("<div style='height:1.6rem'></div>", unsafe_allow_html=True)
    if st.button("↺  Refresh", key="traffic_refresh"):
        st.cache_data.clear()

# ── Load data ─────────────────────────────────────────────────────────────────
with st.spinner(""):
    df = fetch_hourly_stats(hours=window)

if df.empty:
    empty_state("No hourly stats data available")
    st.stop()

# Ensure time column is sorted ascending for charts
time_col = "window_start" if "window_start" in df.columns else df.columns[0]
df = df.sort_values(time_col)

# ── Top-Level Snapshot ────────────────────────────────────────────────────────
section("Snapshot")
mc = st.columns(5)

def _safe(df, col, agg="sum"):
    if col not in df.columns:
        return 0
    return df[col].sum() if agg == "sum" else df[col].mean()

total_req  = int(_safe(df, "total_requests"))
total_err  = int(_safe(df, "errors_404")) + int(_safe(df, "errors_500"))
avg_err_rt = float(df["error_rate"].mean() * 100) if "error_rate" in df.columns else 0
peak_req   = int(df["total_requests"].max()) if "total_requests" in df.columns else 0
avg_success = float(df["success_rate"].mean() * 100) if "success_rate" in df.columns else 0

def _kpi(col, label, value, sub="", color=C_CYAN):
    with col:
        st.markdown(f"""
        <div class="kpi-tile" style="padding:0.9rem 1rem;">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value" style="color:{color};font-size:1.65rem;">{value}</div>
            <div class="kpi-sub">{sub}</div>
        </div>
        """, unsafe_allow_html=True)

_kpi(mc[0], "Total Requests",   f"{total_req:,}",         f"in last {window}h",           C_CYAN)
_kpi(mc[1], "Total Errors",     f"{total_err:,}",          "all HTTP error responses",     C_RED   if total_err > 10000 else C_AMBER)
_kpi(mc[2], "Avg Error Rate",   f"{avg_err_rt:.2f}%",      "mean across all windows",      C_RED   if avg_err_rt > 10 else C_AMBER if avg_err_rt > 5 else C_GREEN)
_kpi(mc[3], "Peak Requests/hr", f"{peak_req:,}",           "highest single-hour spike",    C_PURPLE)
_kpi(mc[4], "Avg Success Rate", f"{avg_success:.2f}%",     "mean across all windows",      C_GREEN)

st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

# ── Request Volume Chart ──────────────────────────────────────────────────────
section("Request Volume Over Time")

req_cols = [c for c in ["total_requests", "rolling_3hr_avg_requests"] if c in df.columns]
if req_cols:
    fig = go.Figure()
    colors = [C_CYAN, C_AMBER]
    for i, col in enumerate(req_cols):
        fig.add_trace(go.Scatter(
            x=df[time_col], y=df[col],
            name=col.replace("_", " ").title(),
            mode="lines",
            line=dict(color=colors[i], width=1.8 if i == 0 else 1.4,
                      dash="solid" if i == 0 else "dot"),
            fill="tozeroy" if i == 0 else "none",
            fillcolor="rgba(0,212,255,0.05)" if i == 0 else None,
            hovertemplate=f"<b>%{{y:,.0f}}</b> requests<br>%{{x}}<extra>{col}</extra>",
        ))
    # Add peak annotation
    if "total_requests" in df.columns:
        peak_idx = df["total_requests"].idxmax()
        peak_time = df.loc[peak_idx, time_col]
        peak_val  = df.loc[peak_idx, "total_requests"]
        fig.add_annotation(
            x=peak_time, y=peak_val,
            text=f"Peak: {peak_val:,}",
            showarrow=True, arrowhead=2, arrowcolor=C_AMBER,
            font=dict(family="JetBrains Mono", size=10, color=C_AMBER),
            bgcolor=C_SURFACE, bordercolor=C_AMBER, borderwidth=1,
            ax=0, ay=-36,
        )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
        height=280, margin=dict(l=50, r=20, t=10, b=50),
        xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                   tickfont=dict(family="JetBrains Mono", size=10, color=C_TEXT3),
                   title="Time Window"),
        yaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                   tickfont=dict(family="JetBrains Mono", size=10, color=C_TEXT3),
                   title="Requests"),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10, color=C_TEXT2),
                    orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                        font=dict(family="JetBrains Mono", size=11)),
    )
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
else:
    empty_state("total_requests column not found in hourly_stats")

# ── Error Rate + Success Rate ─────────────────────────────────────────────────
left, right = st.columns(2, gap="large")

with left:
    section("Error Rate Trend")
    if "error_rate" in df.columns:
        err_pct = df["error_rate"] * 100
        # Colour-grade the line based on severity
        high_mask = err_pct > 10

        fig_err = go.Figure()
        # Background danger zone
        fig_err.add_hrect(
            y0=10, y1=max(err_pct.max(), 15),
            fillcolor="rgba(255,45,85,0.04)",
            line_width=0, annotation_text="High Risk Zone",
            annotation_font=dict(color=C_RED, size=9),
            annotation_position="top left",
        )
        fig_err.add_hline(
            y=5, line_dash="dot", line_color=C_AMBER,
            line_width=1, opacity=0.4,
            annotation_text="5% threshold",
            annotation_font=dict(color=C_AMBER, size=9),
        )
        fig_err.add_trace(go.Scatter(
            x=df[time_col], y=err_pct,
            mode="lines",
            line=dict(color=C_RED, width=1.8),
            fill="tozeroy", fillcolor="rgba(255,45,85,0.06)",
            hovertemplate="Error Rate: <b>%{y:.2f}%</b><br>%{x}<extra></extra>",
        ))
        fig_err.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
            height=240, margin=dict(l=50, r=20, t=10, b=50),
            xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                       tickfont=dict(family="JetBrains Mono", size=10, color=C_TEXT3)),
            yaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER, ticksuffix="%",
                       tickfont=dict(family="JetBrains Mono", size=10, color=C_TEXT3),
                       title="Error %"),
            hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                            font=dict(family="JetBrains Mono", size=11)),
        )
        st.plotly_chart(fig_err, width="stretch", config={"displayModeBar": False})
    else:
        empty_state("error_rate column not found")

with right:
    section("Success Rate Trend")
    if "success_rate" in df.columns:
        fig_hosts = go.Figure(go.Scatter(
            x=df[time_col], y=df["success_rate"] * 100,
            mode="lines",
            line=dict(color=C_GREEN, width=1.8),
            fill="tozeroy", fillcolor="rgba(0,229,160,0.05)",
            hovertemplate="Success Rate: <b>%{y:.2f}%</b><br>%{x}<extra></extra>",
        ))
        fig_hosts.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
            height=240, margin=dict(l=50, r=20, t=10, b=50),
            xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                       tickfont=dict(family="JetBrains Mono", size=10, color=C_TEXT3)),
            yaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER, ticksuffix="%",
                       tickfont=dict(family="JetBrains Mono", size=10, color=C_TEXT3),
                       title="Success %"),
            hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                            font=dict(family="JetBrains Mono", size=11)),
        )
        st.plotly_chart(fig_hosts, width="stretch", config={"displayModeBar": False})
    else:
        empty_state("success_rate column not found")

# ── Bandwidth / Total Bytes ───────────────────────────────────────────────────
if "total_bytes" in df.columns:
    section("Bandwidth (Total Bytes Transferred)")
    df["total_bytes_mb"] = df["total_bytes"] / (1024 * 1024)
    fig_bw = go.Figure(go.Bar(
        x=df[time_col], y=df["total_bytes_mb"],
        marker=dict(color=C_PURPLE, opacity=0.8, line=dict(width=0)),
        hovertemplate="Bandwidth: <b>%{y:.1f} MB</b><br>%{x}<extra></extra>",
    ))
    fig_bw.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
        height=200, margin=dict(l=50, r=20, t=10, b=50),
        xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                   tickfont=dict(family="JetBrains Mono", size=10, color=C_TEXT3)),
        yaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                   tickfont=dict(family="JetBrains Mono", size=10, color=C_TEXT3),
                   title="MB"),
        hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                        font=dict(family="JetBrains Mono", size=11)),
    )
    st.plotly_chart(fig_bw, width="stretch", config={"displayModeBar": False})

# ── Raw Hourly Stats Table ────────────────────────────────────────────────────
with st.expander("📋 Raw Hourly Stats Table", expanded=False):
    show_df = df.drop(columns=["total_bytes_mb"], errors="ignore")
    st.dataframe(
        show_df.sort_values(time_col, ascending=False),
        width="stretch",
        hide_index=True,
        height=380,
    )

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="margin-top:1.5rem;padding-top:0.75rem;border-top:1px solid {C_BORDER};
            font-family:'JetBrains Mono',monospace;font-size:0.65rem;color:{C_TEXT3};">
    Source: <code>/Volumes/workspace/default/logs_volume/gold/hourly_stats</code>
    &nbsp;·&nbsp; Cache TTL: 120s
</div>
""", unsafe_allow_html=True)
