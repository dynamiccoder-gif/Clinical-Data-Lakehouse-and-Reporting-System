from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config


CATALOG = "workspace"
SCHEMA = "default"
TABLES = {
    "patient360": f"{CATALOG}.{SCHEMA}.patient_360",
    "careGaps": f"{CATALOG}.{SCHEMA}.care_gaps",
    "clinicalMetrics": f"{CATALOG}.{SCHEMA}.clinical_metrics",
    "diseaseRankings": f"{CATALOG}.{SCHEMA}.disease_rankings",
    "demographics": f"{CATALOG}.{SCHEMA}.demographics",
    "comorbidityPairs": f"{CATALOG}.{SCHEMA}.comorbidity_pairs",
    "returnGaps": f"{CATALOG}.{SCHEMA}.return_gaps",
    "audit": f"{CATALOG}.{SCHEMA}.batch_audit",
    "kafkaVitalsSummary": f"{CATALOG}.{SCHEMA}.gold_kafka_vitals_summary",
    "kafkaVitalsPatients": f"{CATALOG}.{SCHEMA}.gold_kafka_vitals_by_patient",
    "kafkaEncountersSummary": f"{CATALOG}.{SCHEMA}.gold_kafka_encounters_summary",
    "kafkaEncountersByType": f"{CATALOG}.{SCHEMA}.gold_kafka_encounters_by_type",
    "kafkaEncountersPatients": f"{CATALOG}.{SCHEMA}.gold_kafka_encounters_by_patient",
}

COLORS = {
    "green": "#0f9f93",
    "blue": "#2563eb",
    "red": "#dc2626",
    "amber": "#f59e0b",
    "muted": "#68748a",
}


def _connect():
    config = Config()
    warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
    return sql.connect(
        server_hostname=config.host.replace("https://", ""),
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: config.authenticate,
    )


def _execute_query(query: str) -> pd.DataFrame:
    with _connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()


def _execute_queries(queries: dict[str, str]) -> dict[str, pd.DataFrame]:
    with _connect() as connection:
        with connection.cursor() as cursor:
            data = {}
            for name, query in queries.items():
                cursor.execute(query)
                data[name] = cursor.fetchall_arrow().to_pandas()
            return data


def is_retryable_databricks_error(error: Exception) -> bool:
    message = str(error)
    return (
        "Invalid SessionHandle" in message
        or "SESSION_CLOSED" in message
        or "INVALID_STATE" in message
        or "not STARTING or RUNNING" in message
        or "temporarily unavailable" in message.lower()
    )


def is_free_limit_error(error: Exception) -> bool:
    message = str(error).lower()
    return "free daily limit" in message or "come back again tomorrow" in message


@st.cache_data(ttl=300, show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    max_attempts = 8
    delay_seconds = 15

    for attempt in range(1, max_attempts + 1):
        try:
            return _execute_query(query)
        except Exception as error:
            if not is_retryable_databricks_error(error) or attempt == max_attempts:
                raise
            time.sleep(delay_seconds)

    raise RuntimeError("SQL warehouse did not become available.")


@st.cache_data(ttl=300, show_spinner=False)
def load_live_data() -> dict[str, pd.DataFrame]:
    queries = {
        "patient360": f"""
            WITH ranked_patients AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(risk_band, 'UNKNOWN')
                        ORDER BY risk_score DESC, patient_id
                    ) AS dashboard_rank
                FROM {TABLES["patient360"]}
            )
            SELECT *
            FROM ranked_patients
            WHERE dashboard_rank <= 2000
            ORDER BY
                CASE risk_band
                    WHEN 'HIGH' THEN 1
                    WHEN 'MEDIUM' THEN 2
                    WHEN 'LOW' THEN 3
                    ELSE 4
                END,
                risk_score DESC
            LIMIT 5000
        """,
        "careGaps": f"""
            SELECT *
            FROM {TABLES["careGaps"]}
            ORDER BY readmission_risk_score DESC
            LIMIT 500
        """,
        "clinicalMetrics": f"""
            SELECT *
            FROM {TABLES["clinicalMetrics"]}
            ORDER BY avg_readmission_risk_score DESC
            LIMIT 500
        """,
        "diseaseRankings": f"""
            SELECT *
            FROM {TABLES["diseaseRankings"]}
            ORDER BY affected_patients DESC
            LIMIT 500
        """,
        "demographics": f"""
            SELECT *
            FROM {TABLES["demographics"]}
            LIMIT 500
        """,
        "comorbidityPairs": f"""
            SELECT *
            FROM {TABLES["comorbidityPairs"]}
            ORDER BY patient_count DESC
            LIMIT 100
        """,
        "returnGaps": f"""
            SELECT *
            FROM {TABLES["returnGaps"]}
            LIMIT 50
        """,
        "audit": f"""
            SELECT *
            FROM {TABLES["audit"]}
            ORDER BY execution_timestamp DESC
            LIMIT 50
        """,
        "kafkaVitalsSummary": f"""
            SELECT *
            FROM {TABLES["kafkaVitalsSummary"]}
        """,
        "kafkaVitalsPatients": f"""
            SELECT *
            FROM {TABLES["kafkaVitalsPatients"]}
            ORDER BY abnormal_vitals_events DESC, latest_event_time DESC
            LIMIT 500
        """,
        "kafkaEncountersSummary": f"""
            SELECT *
            FROM {TABLES["kafkaEncountersSummary"]}
        """,
        "kafkaEncountersByType": f"""
            SELECT *
            FROM {TABLES["kafkaEncountersByType"]}
            ORDER BY encounters DESC
            LIMIT 100
        """,
        "kafkaEncountersPatients": f"""
            SELECT *
            FROM {TABLES["kafkaEncountersPatients"]}
            ORDER BY risk_flagged_encounters DESC, latest_event_time DESC
            LIMIT 500
        """,
    }
    max_attempts = 8
    delay_seconds = 15

    for attempt in range(1, max_attempts + 1):
        try:
            return _execute_queries(queries)
        except Exception as error:
            if is_free_limit_error(error):
                raise
            if not is_retryable_databricks_error(error) or attempt == max_attempts:
                raise
            time.sleep(delay_seconds)

    raise RuntimeError("SQL warehouse did not become available.")


def clear_live_cache() -> None:
    run_query.clear()
    load_live_data.clear()


def as_frame(snapshot: dict[str, Any], key: str) -> pd.DataFrame:
    return pd.DataFrame(snapshot.get(key, []))


def num(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def metric_from_audit(audit_df: pd.DataFrame, column: str, default: float = 0.0) -> float:
    if audit_df.empty or column not in audit_df.columns:
        return default
    return num(audit_df.iloc[0].get(column), default)


def short_conditions(value: Any, limit: int = 3) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for raw in str(value or "").split("|"):
        item = raw.strip()
        if not item or item in seen or item in {"General Clinical Encounter", "General Outpatient Checkup"}:
            continue
        seen.add(item)
        parts.append(item)
    if not parts:
        return "No condition linked"
    shown = parts[:limit]
    suffix = f" and {len(parts) - len(shown)} more" if len(parts) > len(shown) else ""
    return " | ".join(shown) + suffix


def inject_css() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background-color: #F5F7FB;
            color: #172033;
        }

        .block-container {
            max-width: 1240px;
            padding-top: 1.4rem;
        }

        [data-testid="stMetric"] {
            background: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-radius: 8px;
            padding: 16px;
            box-shadow: 0 4px 14px rgba(15, 23, 42, 0.06);
        }

        [data-testid="stMetricLabel"] p {
            color: #64748B;
            font-weight: 700;
        }

        [data-testid="stMetricValue"] {
            color: #172033;
            font-weight: 800;
        }

        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
            border-bottom: 1px solid #E2E8F0;
        }

        .stTabs [data-baseweb="tab"] {
            color: #475569;
            font-weight: 700;
            padding-left: 14px;
            padding-right: 14px;
        }

        .stTabs [aria-selected="true"] {
            color: #DC2626 !important;
        }

        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div {
            background-color: #FFFFFF;
            color: #172033;
            border-color: #CBD5E1;
        }

        div[data-baseweb="select"] span,
        div[data-baseweb="input"] input {
            color: #172033 !important;
        }

        h1, h2, h3 {
            color: #172033;
        }

        p, label {
            color: #475569;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def style_chart(fig, height: int = 420):
    fig.update_layout(
        template="plotly_white",
        height=height,
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font={"color": "#334155", "size": 12},
        title={"font": {"color": "#172033", "size": 18}},
        margin={"l": 20, "r": 20, "t": 60, "b": 30},
        legend_title_text="",
        hoverlabel={"bgcolor": "#FFFFFF", "font_color": "#172033"},
    )
    fig.update_xaxes(
        gridcolor="#E2E8F0",
        linecolor="#CBD5E1",
        tickfont={"color": "#475569"},
        title_font={"color": "#475569"},
    )
    fig.update_yaxes(
        gridcolor="#E2E8F0",
        linecolor="#CBD5E1",
        tickfont={"color": "#475569"},
        title_font={"color": "#475569"},
    )
    return fig


def show_chart(fig, height: int = 420) -> None:
    st.plotly_chart(style_chart(fig, height), width="stretch", theme="streamlit")


def latest_run_time(audit_df: pd.DataFrame) -> str:
    if audit_df.empty or "execution_timestamp" not in audit_df.columns:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    value = audit_df.iloc[0].get("execution_timestamp")
    if pd.isna(value):
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return str(value)


def render_header(audit_df: pd.DataFrame) -> None:
    left, right = st.columns([3, 1])
    with left:
        st.caption("Clinical 360 Live Gold Report")
        st.title("Patient Risk & Care Quality Intelligence")
        st.write("Focused view of who needs attention, why they need attention, and whether the pipeline run is trustworthy.")
    with right:
        st.caption("Latest pipeline run")
        st.write(latest_run_time(audit_df))


def render_metric_strip(patient_df: pd.DataFrame, care_gap_df: pd.DataFrame, audit_df: pd.DataFrame, kpis: dict[str, Any]) -> None:
    high_risk_patients = int(kpis.get("high_risk_patients") or (patient_df.get("risk_band", pd.Series()) == "HIGH").sum())
    patients_monitored = int(kpis.get("patients_monitored") or patient_df.get("patient_id", pd.Series()).nunique())
    critical_gaps = int(kpis.get("critical_care_gaps") or (care_gap_df.get("care_gap_severity", pd.Series()) == "CRITICAL").sum())
    recent_returns = int(kpis.get("recent_return_signal_patients") or patient_df.get("reason_recent_readmission", pd.Series()).sum())
    trust_score = num(kpis.get("pipeline_trust_score_pct") or metric_from_audit(audit_df, "pass_rate_pct", 0))
    latest_records = int(metric_from_audit(audit_df, "source_records_read", 0))

    cols = st.columns(6)
    cols[0].metric("Source Records", f"{latest_records:,}")
    cols[1].metric(
        "Validation Pass Rate",
        f"{metric_from_audit(audit_df, 'pass_rate_pct', 0):.2f}%",
        help="Percentage of source records not sent to quarantine. This does not guarantee every field is clinically perfect.",
    )
    cols[2].metric("Silver Records", f"{int(metric_from_audit(audit_df, 'silver_records_written', 0)):,}")
    cols[3].metric("Quarantine Records", f"{int(metric_from_audit(audit_df, 'quarantine_records_written', 0)):,}")
    cols[4].metric("Duplicates Skipped", f"{int(metric_from_audit(audit_df, 'duplicate_records_skipped', 0)):,}")
    cols[5].metric("High-Risk Patients", f"{high_risk_patients:,}", help=f"Pipeline trust score: {trust_score:.2f}%")


def priority_worklist(patient_df: pd.DataFrame) -> None:
    st.subheader("Priority Patient Worklist")
    st.caption("Full Patient 360 population is available for filtering; the table highlights the top 50 patients after filters.")
    if patient_df.empty:
        st.info("No Patient 360 records available.")
        return

    available_risks = patient_df["risk_band"].dropna().astype(str).unique().tolist()
    ordered_risks = [risk for risk in ["HIGH", "MEDIUM", "LOW"] if risk in available_risks]
    ordered_risks.extend(sorted(risk for risk in available_risks if risk not in ordered_risks))
    left, right = st.columns([1, 2])
    with left:
        selected_risks = st.multiselect("Risk bands", ordered_risks, default=ordered_risks)
    with right:
        search = st.text_input("Search patient, condition, reason, or suggested focus")

    filtered = patient_df.copy()
    if selected_risks:
        filtered = filtered[filtered["risk_band"].astype(str).isin(selected_risks)]
    if search:
        mask = pd.Series(False, index=filtered.index)
        for column in ["patient_id", "condition_names", "risk_reasons", "suggested_focus"]:
            if column in filtered:
                mask = mask | filtered[column].astype(str).str.contains(search, case=False, na=False)
        filtered = filtered[mask]

    chart_df = filtered["risk_band"].fillna("UNKNOWN").value_counts().reset_index()
    chart_df.columns = ["risk_band", "patients"]
    chart_df["risk_order"] = chart_df["risk_band"].map({"HIGH": 1, "MEDIUM": 2, "LOW": 3}).fillna(4)
    chart_df = chart_df.sort_values(["risk_order", "risk_band"]).drop(columns=["risk_order"])
    fig = px.bar(
        chart_df,
        x="risk_band",
        y="patients",
        color="risk_band",
        title="Worklist by Risk Band",
        color_discrete_map={"HIGH": COLORS["red"], "MEDIUM": COLORS["amber"], "LOW": COLORS["green"]},
    )
    show_chart(fig)

    display = filtered.sort_values("risk_score", ascending=False).head(50).copy()
    st.caption(f"Showing top {len(display):,} of {len(filtered):,} filtered patients, sorted by risk score.")
    display["top_conditions"] = display["condition_names"].map(short_conditions)
    columns = [
        "patient_id",
        "risk_band",
        "risk_score",
        "top_conditions",
        "unique_medications",
        "risk_reasons",
        "suggested_focus",
    ]
    st.dataframe(display[[column for column in columns if column in display]], width="stretch", hide_index=True)


def population_profile(demographics_df: pd.DataFrame, comorbidity_df: pd.DataFrame) -> None:
    st.subheader("Population Health Profile")
    st.caption("Who is represented in the monitored synthetic population, and which conditions commonly appear together.")
    left, right = st.columns([1, 1])

    with left:
        if demographics_df.empty:
            st.info("No demographic records available.")
        else:
            fig = px.treemap(
                demographics_df,
                path=["race", "gender", "age_group"],
                values="unique_patients",
                title="Patients by Race, Gender, and Age Group",
                color="unique_patients",
                color_continuous_scale="Teal",
            )
            show_chart(fig)

    with right:
        if comorbidity_df.empty:
            st.info("No comorbidity pairs available.")
        else:
            pairs = comorbidity_df.sort_values("patient_count", ascending=False).head(12).copy()
            pairs["pair"] = pairs["condition_a"].str.slice(0, 38) + " + " + pairs["condition_b"].str.slice(0, 38)
            fig = px.bar(
                pairs,
                x="patient_count",
                y="pair",
                orientation="h",
                title="Top Comorbidity Pairs",
                color="patient_count",
                color_continuous_scale="Blues",
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            show_chart(fig)

    if not demographics_df.empty:
        st.dataframe(
            demographics_df.sort_values("unique_patients", ascending=False).head(30),
            width="stretch",
            hide_index=True,
        )


def care_gap_worklist(care_gap_df: pd.DataFrame) -> None:
    st.subheader("Care Gap Worklist")
    st.caption("Action-oriented queue for care-plan, vitals, and preventive-care follow-up.")
    if care_gap_df.empty:
        st.info("No active care gaps available.")
        return

    left, right = st.columns([1, 2])
    with left:
        severity = care_gap_df["care_gap_severity"].fillna("UNKNOWN").value_counts().reset_index()
        severity.columns = ["severity", "count"]
        fig = px.pie(
            severity,
            names="severity",
            values="count",
            title="Care Gaps by Severity",
            hole=0.5,
            color="severity",
            color_discrete_map={"CRITICAL": COLORS["red"], "WARNING": COLORS["amber"], "INFO": COLORS["blue"]},
        )
        show_chart(fig)

    with right:
        actions = care_gap_df["recommended_action"].fillna("Unknown").value_counts().head(8).reset_index()
        actions.columns = ["recommended_action", "count"]
        fig = px.bar(
            actions,
            x="count",
            y="recommended_action",
            orientation="h",
            title="Most Common Recommended Actions",
            color="count",
            color_continuous_scale="Reds",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        show_chart(fig)

    display = care_gap_df.sort_values("readmission_risk_score", ascending=False).head(75)
    st.dataframe(display, width="stretch", hide_index=True)


def diagnosis_risk(clinical_df: pd.DataFrame, disease_df: pd.DataFrame, return_gap_df: pd.DataFrame) -> None:
    st.subheader("Highest-Risk Diagnoses")
    st.caption("Diagnosis-level risk, disease prevalence, and recent-return distribution.")
    if clinical_df.empty:
        st.info("No clinical metrics available.")
        return

    left, right = st.columns([1, 1])
    with left:
        risk = clinical_df.sort_values("avg_readmission_risk_score", ascending=False).head(12)
        fig = px.bar(
            risk,
            x="avg_readmission_risk_score",
            y="condition_name",
            orientation="h",
            title="Highest Average Risk Score",
            color="avg_readmission_risk_score",
            color_continuous_scale="Reds",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        show_chart(fig)

    with right:
        if not disease_df.empty:
            common = disease_df.sort_values("affected_patients", ascending=False).head(12)
            fig = px.bar(
                common,
                x="affected_patients",
                y="condition_name",
                orientation="h",
                title="Most Common Diseases",
                color="affected_patients",
                color_continuous_scale="Teal",
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            show_chart(fig)

    if not return_gap_df.empty:
        fig = px.bar(
            return_gap_df,
            x="return_gap_bucket",
            y="patient_count",
            color="encounter_count",
            title="Return Gap Distribution",
            labels={
                "return_gap_bucket": "Days between consecutive encounters",
                "patient_count": "Patients",
                "encounter_count": "Encounters",
            },
            color_continuous_scale="Blues",
        )
        show_chart(fig)

    st.dataframe(
        clinical_df.sort_values("avg_readmission_risk_score", ascending=False).head(50),
        width="stretch",
        hide_index=True,
    )


def batch_ledger(audit_df: pd.DataFrame, kpis: dict[str, Any]) -> None:
    st.subheader("Recent Batch Ledger")
    st.caption("Validation pass rate means source records not sent to quarantine; soft clinical warnings can still remain in Silver.")

    left, right = st.columns([1, 2])
    with left:
        st.metric("Validation Pass Rate", f"{num(kpis.get('pipeline_trust_score_pct') or metric_from_audit(audit_df, 'pass_rate_pct', 0)):.2f}%")
        st.metric("Source Records", f"{int(metric_from_audit(audit_df, 'source_records_read', 0)):,}")
        st.metric("Silver Records", f"{int(metric_from_audit(audit_df, 'silver_records_written', 0)):,}")
        st.metric("Quarantine Records", f"{int(metric_from_audit(audit_df, 'quarantine_records_written', 0)):,}")
        st.metric("Duplicates Skipped", f"{int(metric_from_audit(audit_df, 'duplicate_records_skipped', 0)):,}")

    with right:
        if audit_df.empty:
            st.info("No audit records available.")
        else:
            records = audit_df.sort_values("execution_timestamp", ascending=False)
            st.dataframe(records, width="stretch", hide_index=True)


def kafka_vitals(kafka_summary_df: pd.DataFrame, kafka_patient_df: pd.DataFrame) -> None:
    st.subheader("Kafka Vitals Monitoring")
    st.caption(
        "Real-time vitals path from Aiven Kafka through Databricks Bronze, Silver, quarantine, and Gold summary tables. "
        "The synthetic producer intentionally emits many abnormal readings to demonstrate alerting."
    )

    if kafka_summary_df.empty:
        st.info("No Kafka vitals Gold summary records available.")
        return

    summary = kafka_summary_df.iloc[0]
    avg_bp = f"{num(summary.get('avg_systolic_bp')):.1f}/{num(summary.get('avg_diastolic_bp')):.1f}"
    cols = st.columns(7)
    cols[0].metric("Vitals Events", f"{int(num(summary.get('total_vitals_events'))):,}")
    cols[1].metric("Abnormal Events", f"{int(num(summary.get('abnormal_vitals_count'))):,}")
    cols[2].metric("Abnormal Rate", f"{num(summary.get('abnormal_rate_pct')):.1f}%")
    cols[3].metric("Patients Monitored", f"{int(num(summary.get('patients_monitored'))):,}")
    cols[4].metric("Avg Heart Rate", f"{num(summary.get('avg_heart_rate')):.1f}")
    cols[5].metric("Avg BP", avg_bp)
    cols[6].metric("Avg Temperature", f"{num(summary.get('avg_temperature')):.1f}")

    left, right = st.columns([1, 1])
    with left:
        metric_df = pd.DataFrame(
            [
                {"metric": "Normal", "events": max(num(summary.get("total_vitals_events")) - num(summary.get("abnormal_vitals_count")), 0)},
                {"metric": "Abnormal", "events": num(summary.get("abnormal_vitals_count"))},
            ]
        )
        fig = px.pie(
            metric_df,
            names="metric",
            values="events",
            title="Kafka Vitals Event Mix",
            hole=0.5,
            color="metric",
            color_discrete_map={"Normal": COLORS["green"], "Abnormal": COLORS["red"]},
        )
        show_chart(fig)

    with right:
        if kafka_patient_df.empty:
            st.info("No patient-level Kafka vitals records available.")
        else:
            ranked = kafka_patient_df.sort_values(["abnormal_vitals_events", "latest_event_time"], ascending=[False, False]).head(15)
            fig = px.bar(
                ranked,
                x="abnormal_vitals_events",
                y="patient_id",
                orientation="h",
                title="Patient-Level Abnormal Vitals",
                color="abnormal_rate_pct",
                color_continuous_scale="Reds",
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            show_chart(fig)

    if not kafka_patient_df.empty:
        display_columns = [
            "patient_id",
            "vitals_events",
            "abnormal_vitals_events",
            "abnormal_rate_pct",
            "has_abnormal_vitals",
            "avg_heart_rate",
            "avg_systolic_bp",
            "avg_diastolic_bp",
            "avg_temperature",
            "latest_event_time",
        ]
        st.dataframe(
            kafka_patient_df[[column for column in display_columns if column in kafka_patient_df.columns]],
            width="stretch",
            hide_index=True,
        )


def kafka_encounters(
    encounter_summary_df: pd.DataFrame,
    encounter_type_df: pd.DataFrame,
    encounter_patient_df: pd.DataFrame,
) -> None:
    st.subheader("Kafka Encounter Monitoring")
    st.caption(
        "Second Kafka event domain from clinical-encounters through Databricks Bronze, Silver, quarantine, and Gold summary tables."
    )

    if encounter_summary_df.empty:
        st.info("No Kafka encounter Gold summary records available.")
        return

    summary = encounter_summary_df.iloc[0]
    cols = st.columns(6)
    cols[0].metric("Total Encounters", f"{int(num(summary.get('total_encounters'))):,}")
    cols[1].metric("Emergency", f"{int(num(summary.get('emergency_encounters'))):,}")
    cols[2].metric("Long Stay", f"{int(num(summary.get('long_stay_encounters'))):,}")
    cols[3].metric("Risk Flagged", f"{int(num(summary.get('risk_flagged_encounters'))):,}")
    cols[4].metric("Unique Patients", f"{int(num(summary.get('unique_patients'))):,}")
    cols[5].metric("Avg LOS Hours", f"{num(summary.get('avg_length_of_stay_hours')):.1f}")

    left, right = st.columns([1, 1])
    with left:
        if encounter_type_df.empty:
            st.info("No encounter type records available.")
        else:
            fig = px.bar(
                encounter_type_df,
                x="encounter_type",
                y="encounters",
                color="risk_flag_rate_pct",
                title="Encounters by Type",
                color_continuous_scale="Reds",
                labels={"risk_flag_rate_pct": "Risk flag rate %"},
            )
            show_chart(fig)

    with right:
        if encounter_type_df.empty:
            st.info("No encounter risk mix available.")
        else:
            mix_df = encounter_type_df[["encounter_type", "long_stay_encounters", "emergency_encounters"]].copy()
            mix_df = mix_df.melt(
                id_vars=["encounter_type"],
                value_vars=["long_stay_encounters", "emergency_encounters"],
                var_name="risk_signal",
                value_name="encounters",
            )
            fig = px.bar(
                mix_df,
                x="encounters",
                y="encounter_type",
                color="risk_signal",
                orientation="h",
                title="Encounter Risk Signals",
                color_discrete_map={
                    "long_stay_encounters": COLORS["amber"],
                    "emergency_encounters": COLORS["red"],
                },
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            show_chart(fig)

    if not encounter_patient_df.empty:
        display_columns = [
            "patient_id",
            "encounters",
            "emergency_encounters",
            "long_stay_encounters",
            "risk_flagged_encounters",
            "risk_flag_rate_pct",
            "has_emergency_encounter",
            "has_long_stay",
            "has_encounter_risk_flag",
            "avg_length_of_stay_hours",
            "latest_event_time",
        ]
        st.dataframe(
            encounter_patient_df[[column for column in display_columns if column in encounter_patient_df.columns]],
            width="stretch",
            hide_index=True,
        )


def main() -> None:
    st.set_page_config(
        page_title="Clinical 360 Dashboard",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    inject_css()

    with st.sidebar:
        st.title("Clinical 360")
        st.caption("Live Databricks SQL dashboard")
        st.caption(f"Views: `{CATALOG}.{SCHEMA}`")
        if st.button("Refresh now", width="stretch"):
            clear_live_cache()
            st.rerun()

    try:
        with st.spinner("Loading live Gold views from Databricks..."):
            data = load_live_data()
    except Exception as exc:
        message = str(exc)
        warehouse_unavailable = (
            "INVALID_STATE" in message
            or "not STARTING or RUNNING" in message
            or "temporarily unavailable" in message.lower()
            or "SQL warehouse did not become available" in message
        )
        if warehouse_unavailable:
            st.warning(
                "The Databricks SQL warehouse is stopped or still starting. "
                "Please wait one or two minutes, then refresh the dashboard."
            )
            return
        if is_free_limit_error(exc):
            st.warning(
                "The Databricks free daily compute limit has been reached. "
                "The live dashboard will work again when the limit resets."
            )
            return
        st.error("Could not load live Databricks data.")
        st.info(
            "Check `.streamlit/secrets.toml` and confirm the SQL warehouse can read "
            "`workspace.default.patient_360` and the other Gold views."
        )
        st.exception(exc)
        return

    patient_df = data["patient360"]
    care_gap_df = data["careGaps"]
    clinical_df = data["clinicalMetrics"]
    disease_df = data["diseaseRankings"]
    demographics_df = data["demographics"]
    comorbidity_df = data["comorbidityPairs"]
    return_gap_df = data["returnGaps"]
    audit_df = data["audit"]
    kafka_summary_df = data["kafkaVitalsSummary"]
    kafka_patient_df = data["kafkaVitalsPatients"]
    kafka_encounter_summary_df = data["kafkaEncountersSummary"]
    kafka_encounter_type_df = data["kafkaEncountersByType"]
    kafka_encounter_patient_df = data["kafkaEncountersPatients"]
    kpis: dict[str, Any] = {}

    render_header(audit_df)
    render_metric_strip(patient_df, care_gap_df, audit_df, kpis)

    tabs = st.tabs(
        [
            "Priority Worklist",
            "Population Profile",
            "Care Gaps",
            "Diagnosis Risk",
            "Kafka Vitals",
            "Kafka Encounters",
            "Batch Ledger",
        ]
    )

    with tabs[0]:
        priority_worklist(patient_df)
    with tabs[1]:
        population_profile(demographics_df, comorbidity_df)
    with tabs[2]:
        care_gap_worklist(care_gap_df)
    with tabs[3]:
        diagnosis_risk(clinical_df, disease_df, return_gap_df)
    with tabs[4]:
        kafka_vitals(kafka_summary_df, kafka_patient_df)
    with tabs[5]:
        kafka_encounters(kafka_encounter_summary_df, kafka_encounter_type_df, kafka_encounter_patient_df)
    with tabs[6]:
        batch_ledger(audit_df, kpis)

    st.divider()
    st.caption("All healthcare records are synthetic and used only for educational and portfolio demonstration.")


if __name__ == "__main__":
    main()
