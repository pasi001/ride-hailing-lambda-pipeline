import os
from typing import Any, Optional

import pandas as pd
import requests
import streamlit as st

try:
    import altair as alt
except ImportError:
    alt = None

# Use 127.0.0.1 by default to avoid Windows IPv6 localhost lookup latency
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
REQUEST_TIMEOUT_SEC = 5

st.set_page_config(
    page_title="Ride-Hailing Fleet Operations",
    page_icon="🚖",
    layout="wide",
    initial_sidebar_state="expanded",
)


def fetch_api(
    endpoint: str, params: Optional[dict[str, Any]] = None
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    """Fetch JSON strictly from FastAPI with graceful offline error handling."""
    url = f"{API_BASE_URL}{endpoint}"
    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
        resp.raise_for_status()
        return resp.json(), None
    except requests.exceptions.ConnectionError:
        return None, (
            f"FastAPI server ({API_BASE_URL}) is offline or unreachable. "
            "Start uvicorn first: `uvicorn api.main:app --host 127.0.0.1 --port 8000`"
        )
    except requests.exceptions.Timeout:
        return None, f"FastAPI request timed out after {REQUEST_TIMEOUT_SEC}s ({endpoint})."
    except requests.exceptions.HTTPError as exc:
        return None, f"FastAPI returned HTTP {exc.response.status_code} on {endpoint}."
    except ValueError:
        return None, f"Invalid JSON response received from {endpoint}."


def format_age(seconds: Optional[float]) -> str:
    """Format UTC event age seconds into readable units."""
    if seconds is None or seconds < 0:
        return "N/A"
    sec = int(seconds)
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m {sec % 60}s"
    hours = sec // 3600
    mins = (sec % 3600) // 60
    return f"{hours}h {mins}m"


def format_lkr(amount: Any) -> str:
    try:
        return f"LKR {float(amount):,.2f}"
    except (TypeError, ValueError):
        return "LKR 0.00"


def render_api_offline_banner(error_msg: str) -> None:
    st.error(f"**Serving API Offline:** {error_msg}", icon="🚨")


# ---------------- SIDEBAR CONTROLS & OBSERVABILITY ----------------
with st.sidebar:
    st.title("🚖 Fleet Ops Control")
    st.caption(f"API: `{API_BASE_URL}`")

    auto_refresh = st.toggle("Live Auto-Refresh (5s)", value=True)
    alert_limit = st.slider("Max Alerts to Display", min_value=10, max_value=100, value=25, step=5)

    if st.button("Manual Refresh Now"):
        st.rerun()

    st.divider()
    st.subheader("Pipeline Health")
    health_data, health_err = fetch_api("/health")
    metrics_data, _ = fetch_api("/metrics", params={"format": "json"})

    if health_err or not health_data:
        st.error("API Offline", icon="🔴")
    else:
        st.success("API & Postgres Connected", icon="🟢")
        st.caption(f"UTC: `{health_data.get('server_time_utc', '')[:19]}Z`")

        if metrics_data:
            c1, c2 = st.columns(2)
            c1.metric("Windows", int(metrics_data.get("pipeline_processed_windows_total", 0)))
            c2.metric("Days", int(metrics_data.get("pipeline_reconciled_days_total", 0)))
            c3, c4 = st.columns(2)
            c3.metric("Fleet Alerts", int(metrics_data.get("pipeline_fleet_alerts_total", 0)))
            c4.metric("Bad Records", int(metrics_data.get("pipeline_dropped_bad_records_total", 0)))


# ---------------- MAIN HEADER ----------------
st.title("Ride-Hailing Fleet Operations — Lambda Architecture")
st.caption("Speed Layer (Real-Time Utilization & Alerts) + Batch Layer (Daily Profitability Reconciliation)")

if health_err:
    render_api_offline_banner(health_err)


# ---------------- TAB 1: REAL-TIME FLEET UTILIZATION ----------------
def render_realtime_content() -> None:
    data, err = fetch_api("/api/v1/fleet/realtime")
    if err or not data:
        render_api_offline_banner(err or "No real-time data returned.")
        return

    totals = data.get("fleet_totals", {})
    zones = data.get("zones", [])
    recent_windows = data.get("recent_windows", [])
    age_sec = data.get("latest_event_age_sec")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Active Vehicles", int(totals.get("active_vehicles", 0)))
    k2.metric("Avg Idle Ratio", f"{float(totals.get('avg_idle_ratio', 0.0)) * 100:.1f}%")
    k3.metric("Total Earnings (Latest)", format_lkr(totals.get("total_earnings", 0.0)))
    k4.metric("Latest Event Age (UTC)", format_age(age_sec))

    if not zones:
        st.info("No real-time zone metrics available yet.")
        return

    zones_df = pd.DataFrame(zones)
    zones_df["idle_pct"] = (zones_df["idle_ratio"].astype(float) * 100).round(2)
    zones_df["total_earnings"] = zones_df["total_earnings"].astype(float).round(2)

    col_left, col_right = st.columns(2)
    with col_left:
        st.subheader("Zone Earnings (Latest Window)")
        st.bar_chart(zones_df.set_index("zone")["total_earnings"], color="#2ecc71")

    with col_right:
        st.subheader("Zone Idle Ratio % (Latest Window)")
        st.bar_chart(zones_df.set_index("zone")["idle_pct"], color="#f39c12")

    st.subheader("Recent Window Trend (Last 20 Windows)")
    if recent_windows:
        trend_df = pd.DataFrame(recent_windows)
        trend_df["window_label"] = pd.to_datetime(trend_df["window_end"]).dt.strftime("%H:%M:%S")
        trend_df["total_earnings"] = trend_df["total_earnings"].astype(float)
        trend_df["avg_idle_pct"] = (trend_df["avg_idle_ratio"].astype(float) * 100).round(2)

        t_left, t_right = st.columns(2)
        with t_left:
            st.caption("Fleet Earnings Trend (LKR)")
            st.line_chart(trend_df.set_index("window_label")["total_earnings"])
        with t_right:
            st.caption("Avg Idle Ratio Trend (%)")
            st.line_chart(trend_df.set_index("window_label")["avg_idle_pct"])
    else:
        st.info("Not enough window history for trend charts yet.")

    with st.expander("View Latest Per-Zone Snapshot Table"):
        display_cols = [
            "zone",
            "active_vehicles",
            "trip_count",
            "idle_pct",
            "total_earnings",
            "window_end",
        ]
        available_cols = [c for c in display_cols if c in zones_df.columns]
        st.dataframe(zones_df[available_cols], hide_index=True)

    st.caption(f"Last refreshed (UTC): {data.get('server_time_utc', '')}")


@st.fragment(run_every=5)
def realtime_fragment() -> None:
    render_realtime_content()


# ---------------- TAB 2: ACTIVE ALERTS ----------------
def render_alerts_content(limit: int) -> None:
    data, err = fetch_api("/api/v1/alerts", params={"limit": limit})
    if err or not data:
        render_api_offline_banner(err or "No alert data returned.")
        return

    counts = data.get("counts", {})
    fleet_alerts = data.get("fleet_alerts", [])
    health_alerts = data.get("pipeline_health_alerts", [])

    a1, a2, a3 = st.columns(3)
    a1.metric("Fleet Idle Alerts", int(counts.get("fleet_alerts", 0)))
    a2.metric("Pipeline Health Alerts", int(counts.get("pipeline_health_alerts", 0)))
    a3.metric("Total Shown", int(counts.get("total", 0)))

    st.subheader("Idle Vehicle Alerts (HIGH_IDLE_ALERT)")
    if fleet_alerts:
        fa_df = pd.DataFrame(fleet_alerts)
        fa_cols = [
            "created_at",
            "vehicle_id",
            "zone",
            "alert_type",
            "idle_duration_sec",
            "message",
        ]
        fa_cols = [c for c in fa_cols if c in fa_df.columns]
        st.dataframe(fa_df[fa_cols], hide_index=True)
    else:
        st.success("No idle vehicle alerts recorded.")

    st.subheader("Pipeline Health Alerts")
    if health_alerts:
        ha_df = pd.DataFrame(health_alerts)
        ha_cols = ["created_at", "stage", "severity", "alert_type", "message"]
        ha_cols = [c for c in ha_cols if c in ha_df.columns]
        st.dataframe(ha_df[ha_cols], hide_index=True)
    else:
        st.success("No pipeline health alerts — all stages healthy.")

    st.caption(f"Last refreshed (UTC): {data.get('server_time_utc', '')}")


@st.fragment(run_every=5)
def alerts_fragment(limit: int) -> None:
    render_alerts_content(limit)


# ---------------- TAB 3: DAILY PROFITABILITY RECONCILIATION ----------------
def highlight_unprofitable_rows(row: pd.Series) -> list[str]:
    if bool(row.get("is_unprofitable")):
        return ["background-color: rgba(231, 76, 60, 0.25); color: #ff6b6b; font-weight: 600;"] * len(row)
    return [""] * len(row)


def render_daily_report_tab() -> None:
    initial_data, err = fetch_api("/api/v1/reports/daily")
    if err or not initial_data:
        render_api_offline_banner(err or "No daily profitability report available.")
        return

    available_days = initial_data.get("available_sim_days", [])
    if not available_days:
        st.info("No reconciled daily profitability records found in Postgres yet.")
        return

    selected_day = st.selectbox(
        "Select Simulation Day (sim_day)",
        options=available_days,
        index=0,
        format_func=lambda d: f"Day {d}",
    )

    if selected_day == initial_data.get("selected_sim_day"):
        report = initial_data
    else:
        report, day_err = fetch_api("/api/v1/reports/daily", params={"sim_day": selected_day})
        if day_err or not report:
            render_api_offline_banner(day_err or f"Failed to load Day {selected_day}.")
            return

    kpi = report.get("kpi_summary", {})
    rows = report.get("rows", [])
    unprofitable = report.get("unprofitable_vehicles", [])

    p1, p2, p3, p4 = st.columns(4)
    p1.metric("Total Fare", format_lkr(kpi.get("total_fare", 0.0)))
    p2.metric("Total Cost (Fuel + Maint)", format_lkr(kpi.get("total_cost", 0.0)))
    p3.metric("Fleet Net Profit", format_lkr(kpi.get("net_profit", 0.0)))
    p4.metric("Unprofitable Vehicles", int(kpi.get("unprofitable_count", 0)))

    if unprofitable:
        flagged_ids = ", ".join(str(v.get("vehicle_id")) for v in unprofitable)
        st.warning(f"**Flagged Unprofitable Vehicles (Day {selected_day}):** {flagged_ids}")

    if not rows:
        st.info(f"No vehicle records found for Day {selected_day}.")
        return

    df = pd.DataFrame(rows)
    for num_col in ("total_fare", "fuel_cost", "maintenance_cost", "distance_covered", "net_profit"):
        if num_col in df.columns:
            df[num_col] = df[num_col].astype(float).round(2)

    df["status"] = df["is_unprofitable"].apply(lambda x: "UNPROFITABLE" if x else "PROFITABLE")

    st.subheader(f"Vehicle Net Profit / Loss — Day {selected_day}")
    if alt is not None:
        chart = (
            alt.Chart(df)
            .mark_bar()
            .encode(
                x=alt.X("vehicle_id:N", title="Vehicle ID", sort=None),
                y=alt.Y("net_profit:Q", title="Net Profit (LKR)"),
                color=alt.condition(
                    alt.datum.net_profit >= 0,
                    alt.value("#2ecc71"),
                    alt.value("#e74c3c"),
                ),
                tooltip=[
                    "vehicle_id",
                    "total_trips",
                    "total_fare",
                    "fuel_cost",
                    "maintenance_cost",
                    "net_profit",
                    "status",
                ],
            )
            .properties(height=340)
        )
        st.altair_chart(chart)
    else:
        st.bar_chart(df.set_index("vehicle_id")["net_profit"])

    st.subheader(f"Full Profitability Reconciliation Table — Day {selected_day}")
    table_cols = [
        "vehicle_id",
        "status",
        "total_trips",
        "distance_covered",
        "total_fare",
        "fuel_cost",
        "maintenance_cost",
        "service_flag",
        "net_profit",
        "is_unprofitable",
    ]
    table_cols = [c for c in table_cols if c in df.columns]
    styled_df = df[table_cols].style.apply(highlight_unprofitable_rows, axis=1).format(
        {
            "total_fare": "{:,.2f}",
            "fuel_cost": "{:,.2f}",
            "maintenance_cost": "{:,.2f}",
            "distance_covered": "{:,.2f}",
            "net_profit": "{:,.2f}",
        }
    )
    st.dataframe(styled_df, hide_index=True)


# ---------------- RENDER TABS ----------------
tab1, tab2, tab3 = st.tabs(
    [
        "⚡ Real-Time Fleet Utilization",
        "🚨 Active Alerts",
        "📊 Daily Profitability Reconciliation",
    ]
)

with tab1:
    if auto_refresh:
        realtime_fragment()
    else:
        render_realtime_content()

with tab2:
    if auto_refresh:
        alerts_fragment(alert_limit)
    else:
        render_alerts_content(alert_limit)

with tab3:
    render_daily_report_tab()