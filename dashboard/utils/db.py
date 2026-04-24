"""
Databricks SQL access for the Streamlit dashboard.

This module only reads from the existing Delta outputs. It does not duplicate
pipeline logic, and it keeps the connector on short timeouts so connection
problems fail fast instead of freezing the UI.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import tomllib
import warnings

import pandas as pd
import streamlit as st
from databricks import sql
from urllib3.exceptions import InsecureRequestWarning


ANOMALIES_PATH = "/Volumes/workspace/default/logs_volume/output/anomalies"
HOURLY_STATS_PATH = "/Volumes/workspace/default/logs_volume/gold/hourly_stats"
IP_BEHAVIOUR_PATH = "/Volumes/workspace/default/logs_volume/gold/ip_behaviour"
ENDPOINT_STATS_PATH = "/Volumes/workspace/default/logs_volume/gold/endpoint_stats"


@dataclass(frozen=True)
class DatabricksSettings:
    server_hostname: str
    http_path: str
    access_token: str
    tls_no_verify: bool = False
    tls_trusted_ca_file: str | None = None
    socket_timeout: float = 10.0
    retry_stop_after_attempts_duration: float = 12.0
    retry_stop_after_attempts_count: int = 2
    retry_delay_min: float = 1.0
    retry_delay_default: float = 1.0


def _to_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _load_local_secrets() -> dict:
    candidate_paths = [
        Path(__file__).resolve().parents[1] / ".streamlit" / "secrets.toml",
        Path(__file__).resolve().parents[2] / ".streamlit" / "secrets.toml",
    ]

    for path in candidate_paths:
        if path.exists():
            with path.open("rb") as secret_file:
                return tomllib.load(secret_file)

    return {}


def _read_secret(name: str, default=None):
    try:
        if "databricks" in st.secrets and name in st.secrets["databricks"]:
            return st.secrets["databricks"][name]
    except Exception:
        pass

    local = _load_local_secrets()
    if "databricks" in local and name in local["databricks"]:
        return local["databricks"][name]

    env_key = f"DATABRICKS_{name.upper()}"
    return os.getenv(env_key, default)


@st.cache_resource(show_spinner=False)
def get_settings() -> DatabricksSettings:
    settings = DatabricksSettings(
        server_hostname=str(_read_secret("server_hostname", "")).strip(),
        http_path=str(_read_secret("http_path", "")).strip(),
        access_token=str(_read_secret("access_token", "")).strip(),
        tls_no_verify=_to_bool(_read_secret("tls_no_verify", False), default=False),
        tls_trusted_ca_file=(
            str(_read_secret("tls_trusted_ca_file", "")).strip() or None
        ),
        socket_timeout=float(_read_secret("socket_timeout", 10)),
        retry_stop_after_attempts_duration=float(
            _read_secret("retry_stop_after_attempts_duration", 12)
        ),
        retry_stop_after_attempts_count=int(
            _read_secret("retry_stop_after_attempts_count", 2)
        ),
        retry_delay_min=float(_read_secret("retry_delay_min", 1)),
        retry_delay_default=float(_read_secret("retry_delay_default", 1)),
    )

    missing = [
        key
        for key, value in (
            ("server_hostname", settings.server_hostname),
            ("http_path", settings.http_path),
            ("access_token", settings.access_token),
        )
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Missing Databricks settings in dashboard secrets/environment: "
            + ", ".join(missing)
        )

    return settings


def _connection_kwargs(settings: DatabricksSettings) -> dict:
    kwargs = {
        "_socket_timeout": settings.socket_timeout,
        "_retry_stop_after_attempts_duration": settings.retry_stop_after_attempts_duration,
        "_retry_stop_after_attempts_count": settings.retry_stop_after_attempts_count,
        "_retry_delay_min": settings.retry_delay_min,
        "_retry_delay_default": settings.retry_delay_default,
    }

    if settings.tls_trusted_ca_file:
        kwargs["_tls_trusted_ca_file"] = settings.tls_trusted_ca_file

    if settings.tls_no_verify:
        kwargs["_tls_no_verify"] = True

    return kwargs


def _run_query(query: str) -> pd.DataFrame:
    try:
        settings = get_settings()
        with warnings.catch_warnings():
            if settings.tls_no_verify:
                warnings.simplefilter("ignore", InsecureRequestWarning)

            with sql.connect(
                server_hostname=settings.server_hostname,
                http_path=settings.http_path,
                access_token=settings.access_token,
                **_connection_kwargs(settings),
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    if cursor.description is None:
                        return pd.DataFrame()

                    columns = [col[0] for col in cursor.description]
                    rows = cursor.fetchall()
                    return pd.DataFrame(rows, columns=columns)

    except Exception as exc:
        message = str(exc)
        if "CERTIFICATE_VERIFY_FAILED" in message:
            st.error(
                "Databricks TLS verification failed. Configure `tls_trusted_ca_file` "
                "for your corporate CA, or set `tls_no_verify = true` in "
                "`dashboard/.streamlit/secrets.toml` for this environment."
            )
        else:
            st.error(f"Databricks query failed: {exc}")
        return pd.DataFrame()


@st.cache_data(ttl=30, show_spinner=False)
def fetch_anomalies(limit: int = 1000) -> pd.DataFrame:
    query = f"""
    SELECT
        host,
        window_start,
        live_requests,
        live_errors,
        live_404s,
        live_error_rate,
        batch_total_requests,
        batch_error_rate,
        batch_endpoint_breadth,
        expected_hourly_requests,
        rate_multiplier,
        global_z_score,
        anomaly_type,
        severity
    FROM delta.`{ANOMALIES_PATH}`
    ORDER BY
        CASE severity
            WHEN 'CRITICAL' THEN 1
            WHEN 'HIGH' THEN 2
            WHEN 'MEDIUM' THEN 3
            WHEN 'LOW' THEN 4
            ELSE 5
        END,
        window_start DESC
    LIMIT {int(limit)}
    """
    return _run_query(query)


@st.cache_data(ttl=120, show_spinner=False)
def fetch_hourly_stats(hours: int = 168) -> pd.DataFrame:
    query = f"""
    SELECT
        hour_bucket,
        total_requests,
        total_bytes,
        errors_404,
        errors_500,
        success_count,
        error_rate,
        success_rate,
        rolling_3hr_avg_requests
    FROM delta.`{HOURLY_STATS_PATH}`
    ORDER BY hour_bucket DESC
    LIMIT {int(hours)}
    """
    df = _run_query(query)
    if not df.empty and "hour_bucket" in df.columns:
        df = df.sort_values("hour_bucket")
    return df


@st.cache_data(ttl=120, show_spinner=False)
def fetch_ip_behaviour(limit: int = 500) -> pd.DataFrame:
    query = f"""
    SELECT
        host,
        total_requests,
        total_bytes_transferred,
        total_errors,
        error_rate_pct,
        first_seen,
        last_seen,
        unique_endpoints_hit
    FROM delta.`{IP_BEHAVIOUR_PATH}`
    ORDER BY total_requests DESC
    LIMIT {int(limit)}
    """
    return _run_query(query)


@st.cache_data(ttl=120, show_spinner=False)
def fetch_endpoint_stats(limit: int = 500) -> pd.DataFrame:
    query = f"""
    SELECT
        endpoint,
        total_hits,
        avg_bytes,
        max_bytes,
        not_found_count,
        success_count,
        unique_visitors
    FROM delta.`{ENDPOINT_STATS_PATH}`
    ORDER BY total_hits DESC
    LIMIT {int(limit)}
    """
    return _run_query(query)


@st.cache_data(ttl=30, show_spinner=False)
def fetch_kpi_summary() -> dict:
    results: dict[str, int | float] = {}

    anomaly_summary = _run_query(
        f"""
        SELECT
            COUNT(*) AS total_anomalies,
            SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
            SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) AS high_count
        FROM delta.`{ANOMALIES_PATH}`
        """
    )
    if not anomaly_summary.empty:
        results["total_anomalies"] = int(anomaly_summary["total_anomalies"].iloc[0] or 0)
        results["critical_count"] = int(anomaly_summary["critical_count"].iloc[0] or 0)
        results["high_count"] = int(anomaly_summary["high_count"].iloc[0] or 0)

    latest_traffic = _run_query(
        f"""
        SELECT
            error_rate AS latest_error_rate,
            total_requests AS latest_total_requests
        FROM delta.`{HOURLY_STATS_PATH}`
        ORDER BY hour_bucket DESC
        LIMIT 1
        """
    )
    if not latest_traffic.empty:
        results["latest_error_rate"] = float(
            latest_traffic["latest_error_rate"].iloc[0] or 0
        )
        results["latest_total_requests"] = int(
            latest_traffic["latest_total_requests"].iloc[0] or 0
        )

    unique_ips = _run_query(
        f"""
        SELECT COUNT(*) AS unique_ips
        FROM delta.`{IP_BEHAVIOUR_PATH}`
        """
    )
    if not unique_ips.empty:
        results["unique_ips"] = int(unique_ips["unique_ips"].iloc[0] or 0)

    return results
