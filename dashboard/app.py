"""
Real-Time Crypto Analytics Dashboard
======================================
Interactive Streamlit dashboard for visualizing cryptocurrency market data.
Features: KPI cards, price charts, market overview, volatility analysis, and volume trends.
"""

import sys
import logging
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timezone

# Setup path for imports
sys.path.insert(0, ".")

from config.settings import settings
from database.connection import test_connection, init_database, get_session
from database.queries import CryptoAnalytics

logger = logging.getLogger(__name__)

# ─── Page Configuration ───────────────────────────────────────────────────────

st.set_page_config(
    page_title="Crypto Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ───────────────────────────────────────────────────────────────

st.markdown("""
<style>
    /* Import Google Font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    /* Global Styles */
    html, body, [class*="st-"] {
        font-family: 'Inter', sans-serif;
    }

    .main .block-container {
        padding-top: 1.5rem;
        padding-bottom: 1rem;
        max-width: 1400px;
    }

    /* Header Styling */
    .dashboard-header {
        background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
        padding: 1.8rem 2rem;
        border-radius: 16px;
        margin-bottom: 1.5rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        border: 1px solid rgba(255, 255, 255, 0.08);
    }

    .dashboard-header h1 {
        color: #ffffff;
        font-size: 2rem;
        font-weight: 800;
        margin: 0;
        letter-spacing: -0.5px;
    }

    .dashboard-header p {
        color: rgba(255, 255, 255, 0.65);
        font-size: 0.95rem;
        margin: 0.3rem 0 0 0;
        font-weight: 400;
    }

    /* KPI Card Styles */
    .kpi-card {
        background: linear-gradient(145deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 14px;
        padding: 1.4rem 1.5rem;
        border: 1px solid rgba(255, 255, 255, 0.06);
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.2);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 30px rgba(0, 0, 0, 0.35);
    }

    .kpi-label {
        color: rgba(255, 255, 255, 0.5);
        font-size: 0.78rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-bottom: 0.4rem;
    }

    .kpi-value {
        color: #ffffff;
        font-size: 1.65rem;
        font-weight: 700;
        letter-spacing: -0.5px;
    }

    .kpi-change-positive {
        color: #00d4aa;
        font-size: 0.85rem;
        font-weight: 600;
    }

    .kpi-change-negative {
        color: #ff4757;
        font-size: 0.85rem;
        font-weight: 600;
    }

    /* Section Headers */
    .section-header {
        color: #e2e8f0;
        font-size: 1.2rem;
        font-weight: 700;
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid rgba(99, 102, 241, 0.3);
        letter-spacing: -0.3px;
    }

    /* Coin Row */
    .coin-row {
        background: linear-gradient(145deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 12px;
        padding: 1rem 1.2rem;
        margin-bottom: 0.6rem;
        border: 1px solid rgba(255, 255, 255, 0.05);
        display: flex;
        align-items: center;
        transition: background 0.2s ease;
    }

    .coin-row:hover {
        background: linear-gradient(145deg, #1e1e3a 0%, #1a2745 100%);
    }

    /* Status Badge */
    .status-badge {
        display: inline-block;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
    }

    .status-connected {
        background: rgba(0, 212, 170, 0.15);
        color: #00d4aa;
        border: 1px solid rgba(0, 212, 170, 0.3);
    }

    .status-disconnected {
        background: rgba(255, 71, 87, 0.15);
        color: #ff4757;
        border: 1px solid rgba(255, 71, 87, 0.3);
    }

    /* Hide default Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0c29 0%, #1a1a2e 100%);
    }

    [data-testid="stSidebar"] .stMarkdown {
        color: #e2e8f0;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# ─── Color Palette ────────────────────────────────────────────────────────────

COLORS = {
    "primary": "#6366f1",
    "secondary": "#8b5cf6",
    "success": "#00d4aa",
    "danger": "#ff4757",
    "warning": "#ffa502",
    "info": "#3742fa",
    "bg_dark": "#0f0c29",
    "bg_card": "#1a1a2e",
    "text": "#e2e8f0",
    "text_muted": "rgba(255,255,255,0.5)",
}

COIN_COLORS = {
    "bitcoin": "#f7931a",
    "ethereum": "#627eea",
    "solana": "#00ffa3",
    "cardano": "#0033ad",
    "ripple": "#00aae4",
}

CHART_TEMPLATE = "plotly_dark"


# ─── Helper Functions ─────────────────────────────────────────────────────────

def format_currency(value, decimals=2):
    """Format a number as currency."""
    if value is None:
        return "N/A"
    value = float(value)  # Handle decimal.Decimal from PostgreSQL
    if abs(value) >= 1e12:
        return f"${value/1e12:,.{decimals}f}T"
    elif abs(value) >= 1e9:
        return f"${value/1e9:,.{decimals}f}B"
    elif abs(value) >= 1e6:
        return f"${value/1e6:,.{decimals}f}M"
    else:
        return f"${value:,.{decimals}f}"


def format_change(value):
    """Format a percentage change with color."""
    if value is None:
        return "N/A", "kpi-change-positive"
    value = float(value)  # Handle decimal.Decimal from PostgreSQL
    css_class = "kpi-change-positive" if value >= 0 else "kpi-change-negative"
    arrow = "▲" if value >= 0 else "▼"
    return f"{arrow} {value:+.2f}%", css_class


def render_kpi_card(label, value, change=None, prefix=""):
    """Render a styled KPI card."""
    change_html = ""
    if change is not None:
        change_text, css_class = format_change(change)
        change_html = f'<div class="{css_class}">{change_text}</div>'

    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{prefix}{value}</div>
        {change_html}
    </div>
    """, unsafe_allow_html=True)


# ─── Data Loading Functions ───────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_latest_prices():
    """Load latest prices for all tracked coins."""
    return CryptoAnalytics.get_latest_prices()


@st.cache_data(ttl=30)
def load_market_overview():
    """Load market overview data."""
    return CryptoAnalytics.get_market_overview()


@st.cache_data(ttl=60)
def load_price_history(coin_id, hours):
    """Load price history for a specific coin."""
    return CryptoAnalytics.get_price_history(coin_id, hours)


@st.cache_data(ttl=60)
def load_volatility():
    """Load volatility analysis data."""
    return CryptoAnalytics.get_volatility_analysis()


@st.cache_data(ttl=60)
def load_volume_trends(hours):
    """Load volume trend data."""
    return CryptoAnalytics.get_volume_trends(hours)


@st.cache_data(ttl=30)
def load_record_count():
    """Load total record count."""
    return CryptoAnalytics.get_record_count()


@st.cache_data(ttl=60)
def load_all_price_data(hours):
    """Load price history for all tracked coins."""
    all_data = []
    for coin in settings.COINS:
        data = CryptoAnalytics.get_price_history(coin, hours)
        all_data.extend(data)
    return all_data


# ─── Sidebar ──────────────────────────────────────────────────────────────────

def render_sidebar():
    """Render the sidebar with filters and status."""
    with st.sidebar:
        st.markdown("### ⚙️ Controls")

        # Auto-refresh toggle
        auto_refresh = st.toggle("Auto Refresh (30s)", value=False)

        # Time range selector
        time_range = st.selectbox(
            "Time Range",
            options=[1, 6, 12, 24, 48, 72, 168],
            format_func=lambda x: f"{x}h" if x < 24 else f"{x//24}d",
            index=3,
        )

        # Coin filter
        selected_coins = st.multiselect(
            "Filter Coins",
            options=settings.COINS,
            default=settings.COINS,
            format_func=lambda x: x.title(),
        )

        st.markdown("---")

        # Database status
        st.markdown("### 📡 System Status")
        db_connected = test_connection()
        if db_connected:
            st.markdown(
                '<span class="status-badge status-connected">● Database Connected</span>',
                unsafe_allow_html=True,
            )
            record_count = load_record_count()
            st.metric("Total Records", f"{record_count:,}")
        else:
            st.markdown(
                '<span class="status-badge status-disconnected">● Database Disconnected</span>',
                unsafe_allow_html=True,
            )

        st.markdown("---")
        st.markdown("### 📋 Tracked Coins")
        for coin in settings.COINS:
            color = COIN_COLORS.get(coin, "#6366f1")
            st.markdown(
                f'<span style="color:{color}; font-weight:600;">● {coin.title()}</span>',
                unsafe_allow_html=True,
            )

        st.markdown("---")
        st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")

        return auto_refresh, time_range, selected_coins


# ─── Dashboard Sections ──────────────────────────────────────────────────────

def render_header():
    """Render the dashboard header."""
    st.markdown("""
    <div class="dashboard-header">
        <h1>📊 Crypto Analytics Dashboard</h1>
        <p>Real-time cryptocurrency market intelligence · Powered by CoinGecko API</p>
    </div>
    """, unsafe_allow_html=True)


def render_kpi_section(latest_prices, overview):
    """Render the KPI cards section."""
    if not latest_prices:
        st.warning("No data available. Run `python main.py --ingest` first.")
        return

    st.markdown('<div class="section-header">📈 Market Overview</div>', unsafe_allow_html=True)

    # Row 1: Market-wide KPIs
    col1, col2, col3, col4 = st.columns(4)

    total_mcap = overview.get("total_market_cap", 0)
    total_vol = overview.get("total_volume", 0)
    avg_change = overview.get("avg_change_24h", 0)
    total_coins = overview.get("total_coins", 0)

    with col1:
        render_kpi_card("Total Market Cap", format_currency(total_mcap), avg_change)
    with col2:
        render_kpi_card("24h Volume", format_currency(total_vol))
    with col3:
        render_kpi_card("Avg 24h Change", f"{avg_change:+.2f}%")
    with col4:
        render_kpi_card("Tracked Coins", str(total_coins))

    st.markdown("")

    # Row 2: Individual coin KPIs
    cols = st.columns(len(latest_prices))
    for i, coin in enumerate(latest_prices):
        with cols[i]:
            color = COIN_COLORS.get(coin["coin_id"], "#6366f1")
            change = coin.get("price_change_pct_24h", 0) or 0
            change_text, css_class = format_change(change)

            st.markdown(f"""
            <div class="kpi-card" style="border-left: 3px solid {color};">
                <div class="kpi-label">{coin['name']} ({coin['symbol'].upper()})</div>
                <div class="kpi-value">${coin['current_price']:,.2f}</div>
                <div class="{css_class}">{change_text}</div>
                <div style="color: rgba(255,255,255,0.4); font-size: 0.72rem; margin-top: 0.3rem;">
                    MCap: {format_currency(coin.get('market_cap', 0))}
                </div>
            </div>
            """, unsafe_allow_html=True)


def render_price_charts(selected_coins, time_range):
    """Render price history charts."""
    st.markdown('<div class="section-header">💹 Price History</div>', unsafe_allow_html=True)

    all_data = load_all_price_data(time_range)

    if not all_data:
        st.info(
            f"No price history data for the last {time_range}h. "
            "Run the scheduler to collect more data: `python main.py --schedule`"
        )
        return

    df = pd.DataFrame(all_data)
    df = df[df["coin_id"].isin(selected_coins)]

    if df.empty:
        st.info("No data for selected coins and time range.")
        return

    # Main price chart
    fig = px.line(
        df,
        x="ingested_at",
        y="current_price",
        color="coin_id",
        title="Price Trends",
        labels={"current_price": "Price (USD)", "ingested_at": "Time", "coin_id": "Coin"},
        color_discrete_map=COIN_COLORS,
        template=CHART_TEMPLATE,
    )

    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color="#e2e8f0"),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="right", x=1, font=dict(size=12)
        ),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        height=450,
        margin=dict(l=20, r=20, t=50, b=20),
        hovermode="x unified",
    )

    fig.update_traces(line=dict(width=2.5))

    st.plotly_chart(fig, use_container_width=True)

    # Per-coin sparklines in columns
    if len(selected_coins) > 1:
        cols = st.columns(min(len(selected_coins), 3))
        for i, coin_id in enumerate(selected_coins):
            coin_df = df[df["coin_id"] == coin_id]
            if coin_df.empty:
                continue

            with cols[i % len(cols)]:
                color = COIN_COLORS.get(coin_id, "#6366f1")

                fig_spark = go.Figure()
                fig_spark.add_trace(go.Scatter(
                    x=coin_df["ingested_at"],
                    y=coin_df["current_price"],
                    mode="lines",
                    fill="tozeroy",
                    line=dict(color=color, width=2),
                    fillcolor=f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.1)",
                    name=coin_id.title(),
                ))

                fig_spark.update_layout(
                    title=dict(text=coin_id.title(), font=dict(size=14, color="#e2e8f0")),
                    template=CHART_TEMPLATE,
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    height=200,
                    margin=dict(l=10, r=10, t=35, b=10),
                    showlegend=False,
                    xaxis=dict(visible=False),
                    yaxis=dict(visible=False),
                )

                st.plotly_chart(fig_spark, use_container_width=True)


def render_market_comparison(latest_prices):
    """Render market comparison charts."""
    if not latest_prices:
        return

    st.markdown('<div class="section-header">🏆 Market Comparison</div>', unsafe_allow_html=True)

    df = pd.DataFrame(latest_prices)

    col1, col2 = st.columns(2)

    with col1:
        # Market Cap comparison (pie chart)
        fig_mcap = px.pie(
            df,
            values="market_cap",
            names="name",
            title="Market Cap Distribution",
            color="coin_id",
            color_discrete_map=COIN_COLORS,
            template=CHART_TEMPLATE,
            hole=0.45,
        )
        fig_mcap.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color="#e2e8f0"),
            height=380,
            margin=dict(l=20, r=20, t=50, b=20),
            showlegend=True,
            legend=dict(font=dict(size=11)),
        )
        fig_mcap.update_traces(
            textposition="inside",
            textinfo="percent+label",
            textfont_size=11,
        )
        st.plotly_chart(fig_mcap, use_container_width=True)

    with col2:
        # 24h Change comparison (bar chart)
        df["change_color"] = df["price_change_pct_24h"].apply(
            lambda x: COLORS["success"] if (x or 0) >= 0 else COLORS["danger"]
        )

        fig_change = go.Figure()
        fig_change.add_trace(go.Bar(
            x=df["name"],
            y=df["price_change_pct_24h"].fillna(0),
            marker_color=df["change_color"],
            text=df["price_change_pct_24h"].apply(lambda x: f"{x:+.2f}%" if x else "0%"),
            textposition="outside",
            textfont=dict(color="#e2e8f0", size=11),
        ))

        fig_change.update_layout(
            title="24h Price Change (%)",
            template=CHART_TEMPLATE,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color="#e2e8f0"),
            height=380,
            margin=dict(l=20, r=20, t=50, b=20),
            showlegend=False,
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(
                gridcolor="rgba(255,255,255,0.05)",
                title="Change (%)",
                zeroline=True,
                zerolinecolor="rgba(255,255,255,0.2)",
            ),
        )

        st.plotly_chart(fig_change, use_container_width=True)


def render_volatility_section():
    """Render volatility analysis."""
    st.markdown('<div class="section-header">⚡ Volatility & Liquidity Analysis</div>', unsafe_allow_html=True)

    vol_data = load_volatility()

    if not vol_data:
        st.info("Insufficient data for volatility analysis. Collect more data points first.")
        return

    df = pd.DataFrame(vol_data)

    col1, col2 = st.columns(2)

    with col1:
        # Volatility scatter
        fig_vol = px.scatter(
            df,
            x="avg_spread_pct",
            y="avg_liquidity",
            size="data_points",
            color="coin_id",
            color_discrete_map=COIN_COLORS,
            title="Volatility vs Liquidity",
            labels={
                "avg_spread_pct": "Avg Price Spread (%)",
                "avg_liquidity": "Avg Vol/MCap Ratio",
                "data_points": "Data Points",
            },
            template=CHART_TEMPLATE,
            hover_name="name",
        )

        fig_vol.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color="#e2e8f0"),
            height=380,
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        )

        st.plotly_chart(fig_vol, use_container_width=True)

    with col2:
        # Price range bar chart
        fig_range = go.Figure()

        for _, row in df.iterrows():
            color = COIN_COLORS.get(row["coin_id"], "#6366f1")
            fig_range.add_trace(go.Bar(
                x=[row["name"]],
                y=[row["max_price"] - row["min_price"]],
                base=[row["min_price"]],
                marker_color=color,
                name=row["name"],
                text=f"${row['min_price']:,.0f} - ${row['max_price']:,.0f}",
                textposition="outside",
                textfont=dict(size=9),
            ))

        fig_range.update_layout(
            title="Price Range (24h)",
            template=CHART_TEMPLATE,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Inter", color="#e2e8f0"),
            height=380,
            margin=dict(l=20, r=20, t=50, b=20),
            showlegend=False,
            yaxis=dict(
                title="Price (USD)",
                gridcolor="rgba(255,255,255,0.05)",
            ),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        )

        st.plotly_chart(fig_range, use_container_width=True)


def render_volume_section(time_range):
    """Render volume analysis."""
    st.markdown('<div class="section-header">📊 Volume Analysis</div>', unsafe_allow_html=True)

    vol_data = load_volume_trends(time_range)

    if not vol_data:
        st.info("No volume data available for the selected time range.")
        return

    df = pd.DataFrame(vol_data)

    fig = px.bar(
        df,
        x="hour",
        y="avg_volume",
        color="coin_id",
        color_discrete_map=COIN_COLORS,
        title="Hourly Trading Volume",
        labels={"avg_volume": "Avg Volume (USD)", "hour": "Hour", "coin_id": "Coin"},
        template=CHART_TEMPLATE,
        barmode="group",
    )

    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter", color="#e2e8f0"),
        height=400,
        margin=dict(l=20, r=20, t=50, b=20),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="right", x=1, font=dict(size=11)
        ),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
    )

    st.plotly_chart(fig, use_container_width=True)


def render_data_table(latest_prices):
    """Render a detailed data table."""
    st.markdown('<div class="section-header">📋 Detailed Market Data</div>', unsafe_allow_html=True)

    if not latest_prices:
        return

    df = pd.DataFrame(latest_prices)

    # Format columns for display
    display_df = df[["name", "symbol", "current_price", "price_change_pct_24h",
                      "market_cap", "total_volume", "market_cap_rank"]].copy()

    display_df.columns = ["Coin", "Symbol", "Price (USD)", "24h Change %",
                          "Market Cap", "Volume", "Rank"]

    display_df["Symbol"] = display_df["Symbol"].str.upper()
    display_df["Price (USD)"] = display_df["Price (USD)"].apply(lambda x: f"${x:,.2f}")
    display_df["24h Change %"] = display_df["24h Change %"].apply(
        lambda x: f"{x:+.2f}%" if x else "0%"
    )
    display_df["Market Cap"] = display_df["Market Cap"].apply(lambda x: format_currency(x))
    display_df["Volume"] = display_df["Volume"].apply(lambda x: format_currency(x))

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=250,
    )


# ─── Main App ─────────────────────────────────────────────────────────────────

def main():
    """Main dashboard application."""

    # Check database connection
    if not test_connection():
        st.error(
            "❌ Cannot connect to PostgreSQL database. "
            "Please check your DATABASE_URL in `.env` and run `python main.py --init-db`."
        )
        return

    # Render sidebar and get filters
    auto_refresh, time_range, selected_coins = render_sidebar()

    # Auto-refresh
    if auto_refresh:
        import time
        time.sleep(0.1)
        st.rerun()

    # Render header
    render_header()

    # Load data
    latest_prices = load_latest_prices()
    overview = load_market_overview()

    # Filter by selected coins
    if latest_prices:
        latest_prices = [p for p in latest_prices if p["coin_id"] in selected_coins]

    # Render dashboard sections
    render_kpi_section(latest_prices, overview)

    st.markdown("")

    # Tabs for different analytics views
    tab1, tab2, tab3, tab4 = st.tabs([
        "💹 Price Charts",
        "🏆 Market Comparison",
        "⚡ Volatility",
        "📊 Volume"
    ])

    with tab1:
        render_price_charts(selected_coins, time_range)

    with tab2:
        render_market_comparison(latest_prices)

    with tab3:
        render_volatility_section()

    with tab4:
        render_volume_section(time_range)

    # Data table
    st.markdown("")
    render_data_table(latest_prices)

    # Footer
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.caption("📊 Crypto Analytics Dashboard v1.0")
    with col2:
        st.caption(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    with col3:
        st.caption("🔗 Data: CoinGecko API (Free Tier)")


if __name__ == "__main__":
    main()
