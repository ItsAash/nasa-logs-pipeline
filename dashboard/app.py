"""
app.py — NASA Log Anomaly Detection Dashboard
Command Center Overview Page
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timezone

st.set_page_config(
    page_title="NASA Log Anomaly Detection",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Must come after set_page_config
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from utils.theme import inject_css, section, empty_state, C_CYAN, C_RED, C_AMBER, C_GREEN, C_TEXT2, C_TEXT3, C_BORDER
from utils.theme import render_sidebar_nav
from utils.db import fetch_kpi_summary, fetch_anomalies, fetch_hourly_stats
import plotly.graph_objects as go

inject_css()
render_sidebar_nav("overview")

# ── Page Header ───────────────────────────────────────────────────────────────
now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
st.markdown(f"""
<div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:1.5rem;">
    <div>
        <div style="font-family:'JetBrains Mono',monospace; font-size:0.65rem;
                    color:{C_TEXT3}; letter-spacing:0.14em; text-transform:uppercase;
                    margin-bottom:0.4rem;">
            <span style="display:inline-block;width:7px;height:7px;background:{C_GREEN};
                         border-radius:50%;margin-right:6px;vertical-align:middle;
                         box-shadow:0 0 6px {C_GREEN};
                         animation:blink 2s ease-in-out infinite;"></span>
            LIVE · NASA Kennedy Space Center — HTTP Log Pipeline
        </div>
        <h1 style="font-family:'JetBrains Mono',monospace !important; font-size:1.6rem !important;
                   font-weight:700 !important; color:#E8EAF0 !important; margin:0 !important;
                   letter-spacing:-0.01em;">
            Command Center
        </h1>
        <div style="font-size:0.78rem; color:{C_TEXT2}; margin-top:0.3rem;">
            Kafka → Spark Structured Streaming → Delta Lake · Anomaly Detection
        </div>
    </div>
    <div style="text-align:right;">
        <div style="font-family:'JetBrains Mono',monospace; font-size:0.72rem;
                    color:{C_TEXT3}; margin-bottom:0.2rem;">Last Refreshed</div>
        <div style="font-family:'JetBrains Mono',monospace; font-size:0.85rem;
                    color:{C_TEXT2};">{now}</div>
    </div>
</div>
""", unsafe_allow_html=True)

st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

# ── KPI Fetch ─────────────────────────────────────────────────────────────────
with st.spinner("Loading KPIs..."):
    try:
        kpis = fetch_kpi_summary()
    except Exception as e:
        st.error(f"KPI fetch failed: {e}")
        kpis = {}

total_anomalies  = kpis.get("total_anomalies", 0)
critical_count   = kpis.get("critical_count", 0)
high_count       = kpis.get("high_count", 0)
error_rate       = kpis.get("latest_error_rate", 0.0)
total_requests   = kpis.get("latest_total_requests", 0)
unique_ips       = kpis.get("unique_ips", 0)

error_pct = f"{(error_rate or 0) * 100:.1f}%"
error_color = C_RED if (error_rate or 0) > 0.1 else C_AMBER if (error_rate or 0) > 0.05 else C_GREEN
crit_color  = C_RED if (critical_count or 0) > 0 else C_GREEN

# ── KPI Tiles ─────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)

def kpi_tile(col, label, value, sub, accent_color=C_CYAN):
    with col:
        st.markdown(f"""
        <div class="kpi-tile">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value" style="color:{accent_color}">{value}</div>
            <div class="kpi-sub">{sub}</div>
        </div>
        """, unsafe_allow_html=True)

kpi_tile(c1, "Total Anomalies", f"{total_anomalies:,}", "all time detected", C_CYAN)
kpi_tile(c2, "Critical Alerts", f"{critical_count:,}", "require immediate action", crit_color)
kpi_tile(c3, "High Severity", f"{high_count:,}", "elevated risk", C_AMBER)
kpi_tile(c4, "Error Rate", error_pct, f"{total_requests:,} req/hr last window", error_color)
kpi_tile(c5, "Tracked IPs", f"{unique_ips:,}", "unique hosts profiled", C_GREEN)

st.markdown("<div style='height:1.5rem'></div>", unsafe_allow_html=True)

# ── Recent Anomalies + Traffic Overview ───────────────────────────────────────
col_left, col_right = st.columns([1.1, 1], gap="large")

with col_left:
    section("Recent Anomalies")
    with st.spinner(""):
        adf = fetch_anomalies(limit=15)

    if adf.empty:
        empty_state("No anomalies detected yet")
    else:
        display_cols = [
            c for c in [
                "window_start",
                "host",
                "anomaly_type",
                "severity",
                "rate_multiplier",
                "global_z_score",
            ]
            if c in adf.columns
        ]
        render_df = adf[display_cols].head(15).copy() if display_cols else adf.head(15)

        # Style severity column as HTML
        if "severity" in render_df.columns:
            sev_colors = {"CRITICAL": C_RED, "HIGH": "#FF6B35", "MEDIUM": C_AMBER, "LOW": C_GREEN}
            styled = render_df.style.apply(
                lambda row: [
                    f"color: {sev_colors.get(str(row.get('severity', '')).upper(), C_TEXT2)}"
                    if col == "severity" else "" for col in render_df.columns
                ],
                axis=1
            ).set_properties(**{
                "background-color": "#0D1321",
                "color": "#E8EAF0",
                "font-family": "JetBrains Mono, monospace",
                "font-size": "11px",
            }).set_table_styles([
                {"selector": "thead th",
                 "props": [("background-color", "#111827"), ("color", "#7B8DB0"),
                           ("font-size", "10px"), ("text-transform", "uppercase"),
                           ("letter-spacing", "0.08em"), ("border-bottom", f"1px solid {C_BORDER}")]},
                {"selector": "tr:hover td",
                 "props": [("background-color", "#111827")]},
            ])
            st.dataframe(render_df, width="stretch", hide_index=True, height=340)
        else:
            st.dataframe(render_df, width="stretch", hide_index=True, height=340)

with col_right:
    section("Traffic (Last 48 Hours)")
    with st.spinner(""):
        hdf = fetch_hourly_stats(hours=48)

    if hdf.empty:
        empty_state("No hourly stats available")
    else:
        time_col = "window_start" if "window_start" in hdf.columns else hdf.columns[0]
        plot_cols = [c for c in ["total_requests", "rolling_3hr_avg_requests"] if c in hdf.columns]

        if plot_cols:
            fig = go.Figure()
            colors_line = [C_CYAN, C_AMBER]
            for i, col in enumerate(plot_cols):
                fig.add_trace(go.Scatter(
                    x=hdf[time_col], y=hdf[col],
                    name=col.replace("_", " ").title(),
                    mode="lines",
                    line=dict(color=colors_line[i % 2], width=1.8),
                    fill="tozeroy" if i == 0 else "none",
                    fillcolor="rgba(0,212,255,0.05)" if i == 0 else None,
                    hovertemplate=f"<b>{col}</b>: %{{y:,.0f}}<br>%{{x}}<extra></extra>",
                ))
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
                margin=dict(l=40, r=10, t=10, b=40), height=175,
                xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                           tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
                yaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                           tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
                legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
                hoverlabel=dict(bgcolor="#0D1321", bordercolor=C_BORDER,
                                font=dict(family="JetBrains Mono", size=11)),
            )
            st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
        else:
            empty_state("traffic_requests column not found")

    # Error rate sparkline
    if not hdf.empty and "error_rate" in hdf.columns:
        section("Error Rate Trend")
        fig2 = go.Figure(go.Scatter(
            x=hdf[time_col], y=hdf["error_rate"] * 100,
            mode="lines",
            line=dict(color=C_RED, width=1.6),
            fill="tozeroy", fillcolor="rgba(255,45,85,0.07)",
            hovertemplate="Error Rate: <b>%{y:.1f}%</b><br>%{x}<extra></extra>",
        ))
        fig2.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
            margin=dict(l=40, r=10, t=10, b=40), height=145,
            xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                       tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
            yaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER, ticksuffix="%",
                       tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
            hoverlabel=dict(bgcolor="#0D1321", bordercolor=C_BORDER,
                            font=dict(family="JetBrains Mono", size=11)),
        )
        st.plotly_chart(fig2, width="stretch", config={"displayModeBar": False})

# ── Pipeline Status Cards ─────────────────────────────────────────────────────
section("Pipeline Data Sources")
sources = [
    ("Anomalies Stream",  "output/anomalies",  "CRITICAL" in (adf.get("severity", pd.Series()) if not adf.empty and "severity" in adf.columns else pd.Series()).values, "Real-time streaming output"),
    ("Hourly Stats",      "gold/hourly_stats",  not hdf.empty, "Spark windowed aggregation"),
    ("IP Behaviour",      "gold/ip_behaviour",  True,          "IP profile gold table"),
    ("Endpoint Stats",    "gold/endpoint_stats", True,          "Endpoint analytics gold table"),
]

cols = st.columns(4)
for i, (name, path, has_data, desc) in enumerate(sources):
    status_color = C_GREEN
    status_label = "CONNECTED"
    with cols[i]:
        st.markdown(f"""
        <div class="kpi-tile" style="padding:1rem 1.2rem;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem;">
                <div style="font-family:'JetBrains Mono',monospace;font-size:0.8rem;
                            font-weight:600;color:#E8EAF0;">{name}</div>
                <span style="font-family:'JetBrains Mono',monospace;font-size:0.6rem;
                             font-weight:600;letter-spacing:0.08em;padding:0.1em 0.5em;
                             border-radius:3px;background:rgba(0,229,160,0.12);
                             color:{status_color};border:1px solid rgba(0,229,160,0.3);">
                    {status_label}
                </span>
            </div>
            <div style="font-family:'JetBrains Mono',monospace;font-size:0.65rem;
                        color:{C_TEXT3};margin-bottom:0.3rem;">/Volumes/workspace/default/logs_volume/{path}</div>
            <div style="font-size:0.72rem;color:{C_TEXT2};">{desc}</div>
        </div>
        """, unsafe_allow_html=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="margin-top:2rem;padding-top:1rem;border-top:1px solid {C_BORDER};
            display:flex;justify-content:space-between;align-items:center;">
    <div style="font-family:'JetBrains Mono',monospace;font-size:0.65rem;color:{C_TEXT3};">
        NASA Log Anomaly Detection Pipeline · Kafka + Spark Structured Streaming + Delta Lake
    </div>
    <div style="font-family:'JetBrains Mono',monospace;font-size:0.65rem;color:{C_TEXT3};">
        Navigate using the sidebar →
    </div>
</div>
""", unsafe_allow_html=True)
