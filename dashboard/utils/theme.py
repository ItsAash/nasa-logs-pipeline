"""
utils/theme.py
--------------
Injects global CSS for the dark Datadog-style monitoring aesthetic
and provides a shared Plotly chart template factory.

Call inject_css() once at the top of every page.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

# ── Palette ───────────────────────────────────────────────────────────────────
C_BG        = "#05080F"
C_SURFACE   = "#0D1321"
C_SURFACE2  = "#111827"
C_BORDER    = "#1C2B4A"
C_BORDER2   = "#243350"
C_CYAN      = "#00D4FF"
C_CYAN_DIM  = "#0099BB"
C_RED       = "#FF2D55"
C_AMBER     = "#FFB020"
C_GREEN     = "#00E5A0"
C_PURPLE    = "#BF80FF"
C_TEXT      = "#E8EAF0"
C_TEXT2     = "#7B8DB0"
C_TEXT3     = "#3D5070"

SEVERITY_COLORS = {
    "CRITICAL": C_RED,
    "HIGH":     "#FF6B35",
    "MEDIUM":   C_AMBER,
    "LOW":      C_GREEN,
}

SEVERITY_BG = {
    "CRITICAL": "rgba(255,45,85,0.12)",
    "HIGH":     "rgba(255,107,53,0.12)",
    "MEDIUM":   "rgba(255,176,32,0.12)",
    "LOW":      "rgba(0,229,160,0.12)",
}

NAV_ITEMS = [
    {
        "key": "overview",
        "label": "Command Center",
        "caption": "System-wide overview",
        "icon": "🛰️",
        "page": "app.py",
        "href": "/",
    },
    {
        "key": "anomalies",
        "label": "Real-Time Anomalies",
        "caption": "Live detections and severity mix",
        "icon": "🚨",
        "page": "pages/1_anomalies.py",
        "href": "/anomalies",
    },
    {
        "key": "traffic",
        "label": "Traffic Overview",
        "caption": "Volume, trend, and error drift",
        "icon": "📈",
        "page": "pages/2_traffic.py",
        "href": "/traffic",
    },
    {
        "key": "ip_intelligence",
        "label": "IP Intelligence",
        "caption": "Host behavior and risk shape",
        "icon": "🌐",
        "page": "pages/3_ip_intelligence.py",
        "href": "/ip_intelligence",
    },
    {
        "key": "endpoint",
        "label": "Endpoint Analytics",
        "caption": "Routes, failures, and payload profile",
        "icon": "📡",
        "page": "pages/4_endpoint.py",
        "href": "/endpoint",
    },
]


def inject_css():
    """
    Injects global CSS into the Streamlit app.
    Call this at the top of every page ONCE.
    """
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

    /* ── Root ─────────────────────────────────────────────────────────── */
    :root {
        --bg:       #05080F;
        --surface:  #0D1321;
        --surface2: #111827;
        --border:   #1C2B4A;
        --cyan:     #00D4FF;
        --red:      #FF2D55;
        --amber:    #FFB020;
        --green:    #00E5A0;
        --purple:   #BF80FF;
        --text:     #E8EAF0;
        --text2:    #7B8DB0;
        --text3:    #3D5070;
    }

    /* ── Base layout ─────────────────────────────────────────────────── */
    html, body, [class*="css"] {
        font-family: 'IBM Plex Sans', sans-serif;
        background-color: var(--bg) !important;
    }

    /* Dot-grid texture on app background */
    .stApp {
        background-color: var(--bg) !important;
        background-image: radial-gradient(var(--border) 1px, transparent 1px);
        background-size: 28px 28px;
    }

    .main .block-container {
        padding-top: 1.5rem !important;
        padding-bottom: 2rem !important;
        max-width: 1520px !important;
    }
    /* Responsive sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(0,212,255,0.06), rgba(8,12,23,0.96)), rgba(8,12,23,0.92) !important;
        border-right: 1px solid var(--border) !important;
        backdrop-filter: blur(14px);
    }

    /* Responsive sidebar behavior */
    [data-testid="stSidebar"][aria-expanded="true"] {
        width: 260px;
    }

    [data-testid="stSidebar"][aria-expanded="false"] {
        width: 0px;
    }

    [data-testid="stSidebar"] > div:first-child {
        width: 260px;
    }

    /* ── Typography ──────────────────────────────────────────────────── */
    h1 { font-size: 1.4rem !important; font-weight: 600 !important; color: var(--text) !important; letter-spacing: -0.01em; }
    h2 { font-size: 1.1rem !important; font-weight: 500 !important; color: var(--text) !important; }
    h3 { font-size: 0.95rem !important; font-weight: 500 !important; color: var(--text2) !important; text-transform: uppercase; letter-spacing: 0.06em; }
    p, li { color: var(--text2) !important; font-size: 0.875rem; }
    .stMarkdown code { font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; color: var(--cyan); background: rgba(0,212,255,0.08); padding: 0.1em 0.4em; border-radius: 3px; }

    /* ── Metric cards ────────────────────────────────────────────────── */
    [data-testid="stMetric"] {
        background: var(--surface) !important;
        border: 1px solid var(--border) !important;
        border-radius: 8px !important;
        padding: 1rem 1.25rem !important;
        transition: border-color 0.2s, box-shadow 0.2s;
    }
    [data-testid="stMetric"]:hover {
        border-color: var(--cyan) !important;
        box-shadow: 0 0 16px rgba(0,212,255,0.1) !important;
    }
    [data-testid="stMetricLabel"] { font-size: 0.7rem !important; font-weight: 500 !important; text-transform: uppercase !important; letter-spacing: 0.08em !important; color: var(--text2) !important; }
    [data-testid="stMetricValue"] { font-family: 'JetBrains Mono', monospace !important; font-size: 2rem !important; font-weight: 700 !important; color: var(--text) !important; line-height: 1.2 !important; }
    [data-testid="stMetricDelta"] { font-family: 'JetBrains Mono', monospace !important; font-size: 0.8rem !important; }

    /* ── Dataframes / tables ─────────────────────────────────────────── */
    [data-testid="stDataFrame"] {
        border: 1px solid var(--border) !important;
        border-radius: 8px !important;
        overflow: hidden;
    }

    /* ── Selectbox / filter inputs ───────────────────────────────────── */
    [data-testid="stSelectbox"] > div,
    [data-testid="stMultiSelect"] > div {
        background: var(--surface) !important;
        border: 1px solid var(--border) !important;
        border-radius: 6px !important;
        color: var(--text) !important;
    }
    [data-baseweb="select"] {
        background: var(--surface) !important;
    }

    /* ── Buttons ─────────────────────────────────────────────────────── */
    .stButton > button {
        background: transparent !important;
        border: 1px solid var(--border) !important;
        color: var(--text2) !important;
        border-radius: 6px !important;
        font-size: 0.8rem !important;
        font-family: 'IBM Plex Sans', sans-serif !important;
        letter-spacing: 0.04em;
        transition: all 0.2s;
    }
    .stButton > button:hover {
        border-color: var(--cyan) !important;
        color: var(--cyan) !important;
        box-shadow: 0 0 10px rgba(0,212,255,0.15) !important;
    }

    /* ── Dividers ─────────────────────────────────────────────────────── */
    hr { border-color: var(--border) !important; margin: 0.5rem 0 !important; }

    /* ── Plotly charts ────────────────────────────────────────────────── */
    .js-plotly-plot .plotly { background: transparent !important; }

    /* ── Scrollbar ────────────────────────────────────────────────────── */
    ::-webkit-scrollbar { width: 4px; height: 4px; }
    ::-webkit-scrollbar-track { background: var(--bg); }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--cyan); }

    /* ── Section header component ─────────────────────────────────────── */
    .section-header {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        margin: 1.5rem 0 0.75rem;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid var(--border);
    }
    .section-header .label {
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: var(--text2);
    }
    .section-header .accent {
        width: 3px;
        height: 14px;
        background: var(--cyan);
        border-radius: 2px;
        box-shadow: 0 0 6px var(--cyan);
    }

    /* ── Severity badges ──────────────────────────────────────────────── */
    .badge {
        display: inline-block;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.65rem;
        font-weight: 600;
        letter-spacing: 0.06em;
        padding: 0.15em 0.55em;
        border-radius: 3px;
        text-transform: uppercase;
    }
    .badge-CRITICAL { background: rgba(255,45,85,0.15); color: #FF2D55; border: 1px solid rgba(255,45,85,0.4); }
    .badge-HIGH     { background: rgba(255,107,53,0.15); color: #FF6B35; border: 1px solid rgba(255,107,53,0.4); }
    .badge-MEDIUM   { background: rgba(255,176,32,0.15); color: #FFB020; border: 1px solid rgba(255,176,32,0.4); }
    .badge-LOW      { background: rgba(0,229,160,0.15); color: #00E5A0; border: 1px solid rgba(0,229,160,0.4); }

    /* ── Pulse animation for CRITICAL ────────────────────────────────── */
    @keyframes pulse-red {
        0%, 100% { box-shadow: 0 0 4px rgba(255,45,85,0.6); }
        50%       { box-shadow: 0 0 12px rgba(255,45,85,0.9); }
    }
    .pulse { animation: pulse-red 2s ease-in-out infinite; }

    /* ── Status dot ──────────────────────────────────────────────────── */
    @keyframes blink { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }
    .status-live {
        display: inline-block;
        width: 7px; height: 7px;
        background: #00E5A0;
        border-radius: 50%;
        animation: blink 2s ease-in-out infinite;
        margin-right: 6px;
        vertical-align: middle;
        box-shadow: 0 0 6px #00E5A0;
    }

    /* ── KPI tile (custom HTML component) ────────────────────────────── */
    .kpi-tile {
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 10px;
        padding: 1.25rem 1.5rem;
        transition: all 0.2s;
    }
    .kpi-tile:hover {
        border-color: rgba(0,212,255,0.4);
        box-shadow: 0 0 20px rgba(0,212,255,0.07);
    }
    .kpi-tile .kpi-label {
        font-size: 0.65rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: var(--text2);
        margin-bottom: 0.4rem;
    }
    .kpi-tile .kpi-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 2.2rem;
        font-weight: 700;
        line-height: 1;
        color: var(--text);
        margin-bottom: 0.3rem;
    }
    .kpi-tile .kpi-sub {
        font-size: 0.7rem;
        color: var(--text3);
    }

    /* ── Clean sidebar nav ───────────────────────────────────────────── */
    .sidebar-nav-item {
        display: flex;
        align-items: center;
        gap: 0.6rem;
        padding: 0.5rem 0.6rem;
        margin-bottom: 0.25rem;
        border-radius: 6px;
        color: var(--text2);
        font-size: 0.82rem;
        text-decoration: none;
        transition: all 0.15s ease;
    }

    .sidebar-nav-item:hover {
        background: rgba(0,212,255,0.08);
        color: var(--cyan);
    }

    .sidebar-nav-item.active {
        background: rgba(0,212,255,0.12);
        color: var(--cyan);
        font-weight: 600;
    }

    .sidebar-section-title {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.65rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--text3);
        margin: 1rem 0 0.5rem;
    }

    /* ── Command deck ───────────────────────────────────────────────── */
    .command-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 0.9rem;
        margin: 0.25rem 0 1.3rem;
    }
    .command-card {
        display: block;
        text-decoration: none;
        background: linear-gradient(180deg, rgba(13, 19, 33, 0.95), rgba(10, 14, 26, 0.96));
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 1rem 1rem 0.95rem;
        min-height: 126px;
        transition: all 0.18s ease;
    }
    .command-card:hover {
        border-color: rgba(0,212,255,0.45);
        transform: translateY(-2px);
        box-shadow: 0 0 22px rgba(0,212,255,0.08);
    }
    .command-card.active {
        border-color: rgba(0,212,255,0.55);
        background: linear-gradient(180deg, rgba(0,212,255,0.12), rgba(13, 19, 33, 0.95));
    }
    .command-kicker {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.62rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--text3);
        margin-bottom: 0.6rem;
    }
    .command-title {
        display: flex;
        align-items: center;
        gap: 0.55rem;
        color: var(--text);
        font-weight: 600;
        font-size: 0.95rem;
        margin-bottom: 0.45rem;
    }
    .command-desc {
        color: var(--text2);
        font-size: 0.78rem;
        line-height: 1.45;
    }


    /* ── Page title bar ─────────────────────────────────────────────── */
    .page-title {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        margin-bottom: 1.25rem;
    }
    .page-title h1 {
        font-family: 'JetBrains Mono', monospace !important;
        font-size: 1.1rem !important;
        font-weight: 600 !important;
        color: var(--text) !important;
        letter-spacing: 0.04em !important;
        margin: 0 !important;
    }
    .page-title .pipe { color: var(--text3); }

    /* ── Info/warning boxes ──────────────────────────────────────────── */
    [data-testid="stAlert"] {
        background: var(--surface) !important;
        border: 1px solid var(--border) !important;
        border-radius: 8px !important;
    }

    /* ── Tab styling ─────────────────────────────────────────────────── */
    [data-baseweb="tab-list"] {
        background: transparent !important;
        border-bottom: 1px solid var(--border) !important;
        gap: 0 !important;
    }
    [data-baseweb="tab"] {
        background: transparent !important;
        color: var(--text2) !important;
        font-size: 0.8rem !important;
        letter-spacing: 0.04em;
        padding: 0.4rem 1rem !important;
    }
    [aria-selected="true"][data-baseweb="tab"] {
        color: var(--cyan) !important;
        border-bottom: 2px solid var(--cyan) !important;
    }

    /* ── Hide Streamlit branding ─────────────────────────────────────── */
    #MainMenu, footer, header { visibility: hidden !important; }
    [data-testid="stDecoration"] { display: none !important; }
    /* Show collapse/expand sidebar button (remove hiding) */
    </style>
    """, unsafe_allow_html=True)


def section(label: str):
    """Renders a styled section header."""
    st.markdown(f"""
    <div class="section-header">
        <div class="accent"></div>
        <span class="label">{label}</span>
    </div>
    """, unsafe_allow_html=True)


def page_title(icon: str, title: str, subtitle: str = ""):
    """Renders the top page title bar."""
    live_dot = '<span class="status-live"></span>' if subtitle else ""
    sub_html  = f'<span style="color:var(--text3);font-size:0.75rem;font-family:JetBrains Mono,monospace;">{live_dot}{subtitle}</span>' if subtitle else ""
    st.markdown(f"""
    <div class="page-title">
        <span style="font-size:1.3rem">{icon}</span>
        <div>
            <h1>{title}</h1>
            {sub_html}
        </div>
    </div>
    """, unsafe_allow_html=True)


def severity_badge(severity: str) -> str:
    """Returns HTML for a severity badge."""
    cls = f"badge badge-{severity.upper()}"
    pulse = " pulse" if severity.upper() == "CRITICAL" else ""
    return f'<span class="{cls}{pulse}">{severity}</span>'


# ── Plotly base layout ────────────────────────────────────────────────────────
def _base_layout(**kwargs) -> dict:
    base = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Sans, sans-serif", color=C_TEXT2, size=11),
        margin=dict(l=40, r=20, t=36, b=40),
        xaxis=dict(
            gridcolor=C_BORDER,
            linecolor=C_BORDER,
            tickfont=dict(family="JetBrains Mono, monospace", size=10, color=C_TEXT3),
            title_font=dict(size=11, color=C_TEXT2),
        ),
        yaxis=dict(
            gridcolor=C_BORDER,
            linecolor=C_BORDER,
            tickfont=dict(family="JetBrains Mono, monospace", size=10, color=C_TEXT3),
            title_font=dict(size=11, color=C_TEXT2),
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            bordercolor=C_BORDER,
            borderwidth=1,
            font=dict(size=10, color=C_TEXT2),
        ),
        hoverlabel=dict(
            bgcolor=C_SURFACE,
            bordercolor=C_BORDER,
            font=dict(family="JetBrains Mono, monospace", size=11, color=C_TEXT),
        ),
    )
    base.update(kwargs)
    return base


def line_chart(df, x, y_cols: list[str], title: str,
               colors=None, fill=False) -> go.Figure:
    """Generic multi-line time-series chart."""
    if colors is None:
        colors = [C_CYAN, C_GREEN, C_AMBER, C_PURPLE, C_RED]
    fig = go.Figure()
    for i, col in enumerate(y_cols):
        if col not in df.columns:
            continue
        color = colors[i % len(colors)]
        fill_arg = "tozeroy" if fill and i == 0 else "none"
        fig.add_trace(go.Scatter(
            x=df[x], y=df[col],
            name=col.replace("_", " ").title(),
            mode="lines",
            line=dict(color=color, width=1.8),
            fill=fill_arg,
            fillcolor=f"rgba({_hex_to_rgb(color)},0.06)" if fill and i == 0 else None,
            hovertemplate=f"<b>{col}</b>: %{{y:,.0f}}<br>%{{x}}<extra></extra>",
        ))
    fig.update_layout(**_base_layout(title=dict(text=title, font=dict(size=12, color=C_TEXT), x=0)))
    return fig


def bar_chart(df, x, y, title: str, color=C_CYAN, horizontal=False) -> go.Figure:
    """Single-series bar chart."""
    fig = go.Figure()
    orientation = "h" if horizontal else "v"
    x_vals = df[y] if horizontal else df[x]
    y_vals = df[x] if horizontal else df[y]
    fig.add_trace(go.Bar(
        x=x_vals, y=y_vals,
        orientation=orientation,
        marker=dict(
            color=color,
            opacity=0.85,
            line=dict(width=0),
        ),
        hovertemplate="<b>%{y}</b><br>Count: %{x:,}<extra></extra>" if horizontal
                      else "<b>%{x}</b><br>%{y:,}<extra></extra>",
    ))
    layout = _base_layout(title=dict(text=title, font=dict(size=12, color=C_TEXT), x=0))
    if horizontal:
        layout["yaxis"]["autorange"] = "reversed"
    fig.update_layout(**layout)
    return fig


def pie_chart(df, names_col, values_col, title: str) -> go.Figure:
    """Donut-style pie chart for severity distribution."""
    color_map = {
        "CRITICAL": C_RED, "HIGH": "#FF6B35",
        "MEDIUM": C_AMBER, "LOW": C_GREEN,
    }
    colors = [color_map.get(n, C_CYAN) for n in df[names_col]]
    fig = go.Figure(go.Pie(
        labels=df[names_col],
        values=df[values_col],
        hole=0.6,
        marker=dict(colors=colors, line=dict(color=C_BG, width=2)),
        textinfo="label+percent",
        textfont=dict(family="JetBrains Mono, monospace", size=10, color=C_TEXT),
        hovertemplate="<b>%{label}</b><br>%{value:,} anomalies (%{percent})<extra></extra>",
    ))
    fig.update_layout(**_base_layout(
        title=dict(text=title, font=dict(size=12, color=C_TEXT), x=0),
        showlegend=True,
        margin=dict(l=20, r=20, t=36, b=20),
    ))
    return fig


def heatmap_chart(z, x, y, title: str) -> go.Figure:
    """Error rate heatmap."""
    fig = go.Figure(go.Heatmap(
        z=z, x=x, y=y,
        colorscale=[[0, C_SURFACE], [0.5, C_AMBER], [1, C_RED]],
        hovertemplate="IP: <b>%{y}</b><br>Hour: %{x}<br>Error Rate: %{z:.1%}<extra></extra>",
        xgap=2, ygap=2,
    ))
    fig.update_layout(**_base_layout(
        title=dict(text=title, font=dict(size=12, color=C_TEXT), x=0),
        margin=dict(l=120, r=20, t=36, b=60),
    ))
    return fig


def _hex_to_rgb(hex_color: str) -> str:
    hex_color = hex_color.lstrip("#")
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    return f"{r},{g},{b}"


def empty_state(message: str = "No data available"):
    """Renders a styled empty state."""
    st.markdown(f"""
    <div style="
        display:flex; align-items:center; justify-content:center;
        height:180px; border:1px dashed {C_BORDER}; border-radius:10px;
        color:{C_TEXT3}; font-size:0.82rem; letter-spacing:0.04em;
        font-family:'JetBrains Mono',monospace;
    ">{message}</div>
    """, unsafe_allow_html=True)


def render_sidebar_nav(active_key: str):
    """Minimal, clean sidebar navigation inside native Streamlit sidebar."""

    with st.sidebar:
        st.markdown("""
        <div style="padding:0.5rem 0.5rem 0.75rem 0.5rem;">
            <div style="font-family:JetBrains Mono;font-size:0.7rem;color:#3D5070;letter-spacing:0.12em;text-transform:uppercase;">
                Mission Control
            </div>
            <div style="font-family:JetBrains Mono;font-size:1rem;font-weight:700;color:#E8EAF0;margin-top:0.3rem;">
                🛰️ NASA LOGS
            </div>
            <div style="font-size:0.72rem;color:#7B8DB0;margin-top:0.3rem;">
                Real-time anomaly monitoring
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('<div class="sidebar-section-title">Views</div>', unsafe_allow_html=True)

        for item in NAV_ITEMS:
            active_class = "active" if item["key"] == active_key else ""

            st.markdown(
                f'<a class="sidebar-nav-item {active_class}" href="{item["href"]}" target="_self">'
                f'<span>{item["icon"]}</span>'
                f'<span>{item["label"]}</span>'
                f'</a>',
                unsafe_allow_html=True,
            )


