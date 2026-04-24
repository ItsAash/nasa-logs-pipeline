"""
pages/4_📡_Endpoints.py
------------------------
Endpoint analytics: top endpoints, 404-heavy paths, avg bytes per endpoint.
Reads from: gold/endpoint_stats
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

st.set_page_config(
    page_title="Endpoints · NASA Log Monitor",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from utils.theme import (inject_css, section, page_title, empty_state,
                          C_CYAN, C_RED, C_AMBER, C_GREEN, C_PURPLE,
                          C_TEXT2, C_TEXT3, C_BORDER, C_SURFACE, C_BG, render_sidebar_nav,
                          )
from utils.db import fetch_endpoint_stats

inject_css()
render_sidebar_nav("endpoint")

page_title("📡", "Endpoint Analytics", "endpoint_stats · per-path traffic analysis")
st.markdown("<div style='height:0.9rem'></div>", unsafe_allow_html=True)

# ── Load + Controls ───────────────────────────────────────────────────────────
ctrl1, ctrl2, _ = st.columns([1, 1, 3])
with ctrl1:
    top_n = st.slider("Endpoints to display", 10, 100, 25, 5, key="ep_top_n")
with ctrl2:
    st.markdown("<div style='height:1.6rem'></div>", unsafe_allow_html=True)
    if st.button("↺  Refresh", key="ep_refresh"):
        st.cache_data.clear()

with st.spinner(""):
    df = fetch_endpoint_stats(limit=500)

if df.empty:
    empty_state("No endpoint stats data available")
    st.stop()

# ── Summary Metrics ───────────────────────────────────────────────────────────
section("Snapshot")
mc = st.columns(5)

total_endpoints = len(df)
total_hits      = int(df["total_hits"].sum()) if "total_hits" in df.columns else 0
total_404       = int(df["not_found_count"].sum()) if "not_found_count" in df.columns else 0
avg_bytes       = float(df["avg_bytes"].mean()) if "avg_bytes" in df.columns else 0
hotspot_ep      = df.loc[df["total_hits"].idxmax(), "endpoint"] if "total_hits" in df.columns and "endpoint" in df.columns else "—"

def _kpi(col, label, value, sub="", color=C_CYAN):
    with col:
        st.markdown(f"""
        <div class="kpi-tile" style="padding:0.9rem 1rem;">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value" style="color:{color};font-size:1.65rem;">{value}</div>
            <div class="kpi-sub" style="word-break:break-all;">{sub}</div>
        </div>
        """, unsafe_allow_html=True)

_kpi(mc[0], "Unique Endpoints",  f"{total_endpoints:,}",     "tracked paths", C_CYAN)
_kpi(mc[1], "Total Hits",        f"{total_hits:,}",           "across all endpoints", C_GREEN)
_kpi(mc[2], "Total 404 Errors",  f"{total_404:,}",            "not-found responses", C_RED if total_404 > 1000 else C_AMBER)
_kpi(mc[3], "Avg Response Size", f"{avg_bytes/1024:.1f} KB",  "mean bytes / response", C_PURPLE)
_kpi(mc[4], "Hotspot Endpoint",  "↑",                         hotspot_ep, C_AMBER)

st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

# ── Top Endpoints by Hits ─────────────────────────────────────────────────────
section("Top Endpoints by Traffic")

top_df = df.head(top_n).copy()

if "endpoint" in top_df.columns and "total_hits" in top_df.columns:
    left, right = st.columns([1.4, 1], gap="large")

    with left:
        fig_hits = go.Figure(go.Bar(
            x=top_df["total_hits"],
            y=top_df["endpoint"],
            orientation="h",
            marker=dict(
                color=top_df["total_hits"],
                colorscale=[[0, "#1C2B4A"], [1.0, C_CYAN]],
                showscale=False,
                line=dict(width=0),
            ),
            hovertemplate="<b>%{y}</b><br>Hits: %{x:,}<extra></extra>",
        ))
        fig_hits.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
            height=max(320, top_n * 20),
            margin=dict(l=270, r=20, t=10, b=40),
            xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                       tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
            yaxis=dict(autorange="reversed", linecolor=C_BORDER,
                       tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
            hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                            font=dict(family="JetBrains Mono", size=11)),
        )
        st.plotly_chart(fig_hits, width="stretch", config={"displayModeBar": False})

    with right:
        # Unique hosts per endpoint
        if "unique_visitors" in top_df.columns:
            fig_uh = go.Figure(go.Bar(
                x=top_df["unique_visitors"],
                y=top_df["endpoint"],
                orientation="h",
                marker=dict(color=C_GREEN, opacity=0.8, line=dict(width=0)),
                hovertemplate="<b>%{y}</b><br>Unique Visitors: %{x:,}<extra></extra>",
            ))
            fig_uh.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
                title=dict(text="Unique Visitors per Endpoint", font=dict(size=11, color=C_TEXT2), x=0),
                height=max(320, top_n * 20),
                margin=dict(l=270, r=20, t=30, b=40),
                xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                           tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
                yaxis=dict(autorange="reversed", linecolor=C_BORDER,
                           tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
                hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                                font=dict(family="JetBrains Mono", size=11)),
            )
            st.plotly_chart(fig_uh, width="stretch", config={"displayModeBar": False})
        else:
            empty_state("unique_visitors column not found")
else:
    empty_state("Required columns (endpoint, total_hits) not found")

# ── 404-Heavy Endpoints ───────────────────────────────────────────────────────
section("404-Heavy Endpoints — Not Found Hotspots")

if "not_found_count" in df.columns and "endpoint" in df.columns:
    err404_df = (
        df[df["not_found_count"] > 0]
        .sort_values("not_found_count", ascending=False)
        .head(top_n)
        .copy()
    )

    if err404_df.empty:
        empty_state("No 404 errors found — great news!")
    else:
        # 404 rate = 404_count / total_hits
        if "total_hits" in err404_df.columns:
            err404_df["404_rate"] = err404_df["not_found_count"] / err404_df["total_hits"].clip(lower=1)
            err404_df["404_pct"]  = err404_df["404_rate"] * 100
        else:
            err404_df["404_pct"] = 0

        left404, right404 = st.columns([1.4, 1], gap="large")

        with left404:
            colors_404 = [
                C_RED    if v > 50 else
                "#FF6B35" if v > 20 else
                C_AMBER
                for v in err404_df["404_pct"]
            ]
            fig_404 = go.Figure(go.Bar(
                x=err404_df["not_found_count"],
                y=err404_df["endpoint"],
                orientation="h",
                marker=dict(color=colors_404, line=dict(width=0)),
                hovertemplate="<b>%{y}</b><br>404 Errors: %{x:,}<extra></extra>",
            ))
            fig_404.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
                height=max(280, len(err404_df) * 22),
                margin=dict(l=270, r=20, t=10, b=40),
                xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER,
                           tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3),
                           title="404 Count"),
                yaxis=dict(autorange="reversed", linecolor=C_BORDER,
                           tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
                hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                                font=dict(family="JetBrains Mono", size=11)),
            )
            st.plotly_chart(fig_404, width="stretch", config={"displayModeBar": False})

        with right404:
            # 404 rate as %
            if "404_pct" in err404_df.columns:
                fig_404r = go.Figure(go.Bar(
                    x=err404_df["404_pct"],
                    y=err404_df["endpoint"],
                    orientation="h",
                    marker=dict(
                        color=err404_df["404_pct"],
                        colorscale=[[0, C_AMBER], [0.5, "#FF6B35"], [1, C_RED]],
                        showscale=False, line=dict(width=0),
                    ),
                    hovertemplate="<b>%{y}</b><br>404 Rate: %{x:.1f}%<extra></extra>",
                ))
                fig_404r.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
                    title=dict(text="404 Rate (% of hits)", font=dict(size=11, color=C_TEXT2), x=0),
                    height=max(280, len(err404_df) * 22),
                    margin=dict(l=270, r=20, t=30, b=40),
                    xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER, ticksuffix="%",
                               tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
                    yaxis=dict(autorange="reversed", linecolor=C_BORDER,
                               tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
                    hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                                    font=dict(family="JetBrains Mono", size=11)),
                )
                st.plotly_chart(fig_404r, width="stretch", config={"displayModeBar": False})
else:
    empty_state("not_found_count column not found in endpoint_stats")

# ── Avg Bytes per Endpoint ────────────────────────────────────────────────────
section("Avg Response Size per Endpoint (KB)")

if "avg_bytes" in df.columns and "endpoint" in df.columns:
    bytes_df = (
        df.sort_values("avg_bytes", ascending=False)
        .head(top_n)
        .copy()
    )
    bytes_df["avg_kb"] = bytes_df["avg_bytes"] / 1024

    fig_bytes = go.Figure(go.Bar(
        x=bytes_df["avg_kb"],
        y=bytes_df["endpoint"],
        orientation="h",
        marker=dict(
            color=bytes_df["avg_kb"],
            colorscale=[[0, "#1C2B4A"], [0.5, C_PURPLE], [1.0, "#E040FB"]],
            showscale=True,
            colorbar=dict(
                title=dict(text="KB", font=dict(family="JetBrains Mono", size=10, color=C_TEXT3)),
                tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3),
                bgcolor="rgba(0,0,0,0)", bordercolor=C_BORDER, borderwidth=1,
                len=0.7,
            ),
            line=dict(width=0),
        ),
        hovertemplate="<b>%{y}</b><br>Avg Size: %{x:.1f} KB<extra></extra>",
    ))
    fig_bytes.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans", color=C_TEXT2, size=11),
        height=max(320, len(bytes_df) * 20),
        margin=dict(l=270, r=60, t=10, b=40),
        xaxis=dict(gridcolor=C_BORDER, linecolor=C_BORDER, ticksuffix=" KB",
                   tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
        yaxis=dict(autorange="reversed", linecolor=C_BORDER,
                   tickfont=dict(family="JetBrains Mono", size=9, color=C_TEXT3)),
        hoverlabel=dict(bgcolor=C_SURFACE, bordercolor=C_BORDER,
                        font=dict(family="JetBrains Mono", size=11)),
    )
    st.plotly_chart(fig_bytes, width="stretch", config={"displayModeBar": False})
else:
    empty_state("avg_bytes column not found")

# ── Full Endpoint Table ───────────────────────────────────────────────────────
with st.expander("📋 Full Endpoint Stats Table", expanded=False):
    show_cols = [c for c in ["endpoint", "total_hits", "not_found_count", "avg_bytes", "unique_visitors"]
                 if c in df.columns]
    show_df = df[show_cols].copy() if show_cols else df.copy()
    if "avg_bytes" in show_df.columns:
        show_df["avg_bytes"] = show_df["avg_bytes"].apply(
            lambda v: f"{float(v)/1024:.1f} KB" if pd.notna(v) else "—"
        )
    st.dataframe(show_df, width="stretch", hide_index=True, height=400)

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="margin-top:1.5rem;padding-top:0.75rem;border-top:1px solid {C_BORDER};
            font-family:'JetBrains Mono',monospace;font-size:0.65rem;color:{C_TEXT3};">
    Source: <code>/Volumes/workspace/default/logs_volume/gold/endpoint_stats</code>
    &nbsp;·&nbsp; Cache TTL: 120s
</div>
""", unsafe_allow_html=True)
