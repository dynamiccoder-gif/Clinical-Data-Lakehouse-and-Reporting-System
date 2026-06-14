import argparse
import json
import time
from datetime import datetime, timezone
from html import escape
from pathlib import Path

import streamlit as st


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SNAPSHOT_PATH = ROOT_DIR / "reports" / "gold_snapshot.json"


def _as_list(value):
    return value if isinstance(value, list) else []


def _num(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _fmt_int(value):
    return f"{int(round(_num(value))):,}"


def _fmt_pct(value):
    pct = _num(value)
    if pct <= 1:
        pct *= 100
    return f"{pct:.2f}%"


def _safe(value):
    return escape("" if value is None else str(value))


def _top(rows, key, limit=10):
    return sorted(_as_list(rows), key=lambda row: _num(row.get(key)), reverse=True)[:limit]


def _latest_by_timestamp(rows):
    rows = _as_list(rows)
    if not rows:
        return {}
    return max(rows, key=lambda row: str(row.get("execution_timestamp", "")))


def _short_conditions(value, limit=3):
    parts = []
    seen = set()
    for raw in str(value or "").split("|"):
        item = raw.strip()
        if not item or item in seen or item in {"General Clinical Encounter", "General Outpatient Checkup"}:
            continue
        seen.add(item)
        parts.append(item)
    if not parts:
        return "No condition linked"
    shown = parts[:limit]
    remaining = len(parts) - len(shown)
    suffix = f" and {remaining} more" if remaining > 0 else ""
    return " | ".join(shown) + suffix


def load_snapshot(snapshot_path):
    path = Path(snapshot_path)
    if not path.exists():
        return {}, None
    with path.open("r", encoding="utf-8") as file:
        return json.load(file), path.stat().st_mtime


def summarize(snapshot):
    clinical_metrics = _as_list(snapshot.get("clinicalMetrics"))
    demographics = _as_list(snapshot.get("demographics"))
    patient_360 = _as_list(snapshot.get("patient360"))
    care_gaps = _as_list(snapshot.get("careGaps"))
    comorbidity_pairs = _as_list(snapshot.get("comorbidityPairs"))
    return_gaps = _as_list(snapshot.get("returnGaps"))
    disease_rankings = _as_list(snapshot.get("diseaseRankings"))
    audit = _as_list(snapshot.get("audit"))
    kpis = snapshot.get("kpis") if isinstance(snapshot.get("kpis"), dict) else {}

    high_risk_patients = sum(1 for row in patient_360 if str(row.get("risk_band", "")).upper() == "HIGH")
    if not high_risk_patients:
        high_risk_patients = sum(_num(row.get("high_risk_patients")) for row in clinical_metrics)

    return {
        "generated_at": snapshot.get("generatedAt") or datetime.now(timezone.utc).isoformat(),
        "latest_audit": _latest_by_timestamp(audit),
        "kpis": kpis,
        "demographics": demographics,
        "total_encounters": sum(_num(row.get("total_encounters")) for row in clinical_metrics),
        "mastered_patients": sum(_num(row.get("unique_patients")) for row in demographics),
        "high_risk_patients": high_risk_patients,
        "top_patient_360": _top(patient_360, "risk_score", 12),
        "top_care_gaps": _top(care_gaps, "readmission_risk_score", 10),
        "comorbidity_pairs": _top(comorbidity_pairs, "patient_count", 15),
        "return_gaps": return_gaps,
        "common_diseases": _top(disease_rankings, "affected_patients", 10),
        "deceased_disease_associations": _top(disease_rankings, "deceased_patients", 10),
        "top_risk": _top(clinical_metrics, "avg_readmission_risk_score", 8),
        "audit": audit,
    }


def inject_css():
    st.markdown(
        """
        <style>
          :root {
            --bg: #f4f7fb;
            --panel: #ffffff;
            --ink: #172033;
            --muted: #68748a;
            --line: #dfe6f0;
            --accent: #0f9f93;
            --blue: #2563eb;
            --danger: #dc2626;
          }
          .stApp { background: var(--bg); color: var(--ink); }
          .block-container { max-width: 1220px; padding: 1.5rem 1.25rem 2rem; }
          [data-testid="stSidebar"] { background: #ffffff; border-right: 1px solid var(--line); }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _table(headers, rows, empty_message):
    if not rows:
        return f'<table><tbody><tr><td colspan="{len(headers)}">{_safe(empty_message)}</td></tr></tbody></table>'
    header_html = "".join(f"<th>{_safe(header)}</th>" for header in headers)
    row_html = "\n".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return f"""
      <table>
        <thead><tr>{header_html}</tr></thead>
        <tbody>{row_html}</tbody>
      </table>
    """


def _metric_card(label, value, note):
    return f"""
      <article class="metric-card">
        <span>{_safe(label)}</span>
        <strong>{_safe(value)}</strong>
        <small>{_safe(note)}</small>
      </article>
    """


def _patient_360_table(rows):
    table_rows = [
        [
            _safe(str(row.get("patient_id", ""))[:10]),
            f'<span class="badge">{_safe(row.get("risk_band", "LOW"))} {_fmt_int(row.get("risk_score"))}</span>',
            _safe(_short_conditions(row.get("condition_names"))),
            _fmt_int(row.get("unique_medications")),
            _safe(row.get("risk_reasons", "")),
            _safe(row.get("suggested_focus", "")),
        ]
        for row in rows
    ]
    return _table(["Patient", "Risk", "Top Conditions", "Active Meds", "Reasons", "Suggested Focus"], table_rows, "No Patient 360 records available.")


def _care_gap_table(rows):
    table_rows = [
        [
            _safe(str(row.get("patient_id", ""))[:10]),
            _safe(row.get("condition_name", "Unknown")),
            f'<span class="badge">{_safe(row.get("care_gap_severity", "INFO"))}</span>',
            _safe(row.get("recommended_action", "")),
            _fmt_int(row.get("chronic_condition_count")),
            _fmt_int(row.get("chronic_careplan_count")),
            _fmt_int(row.get("abnormal_vital_count")),
        ]
        for row in rows
    ]
    return _table(["Patient", "Condition", "Severity", "Action", "Chronic", "Care Plans", "Vitals"], table_rows, "No active care gaps available.")


def _risk_bars(rows):
    if not rows:
        return '<p class="empty">No records available.</p>'
    max_value = max(_num(row.get("avg_readmission_risk_score")) for row in rows) or 1
    parts = []
    for row in rows:
        value = _num(row.get("avg_readmission_risk_score"))
        width = max(4, min(100, (value / max_value) * 100))
        parts.append(f"""
          <div class="bar-row">
            <div class="bar-label">
              <span>{_safe(row.get("condition_name", "Unknown"))}</span>
              <strong>{value:.1f}</strong>
            </div>
            <div class="bar-track"><div class="bar-fill" style="width:{width:.1f}%"></div></div>
          </div>
        """)
    return "\n".join(parts)


def _common_disease_table(rows):
    table_rows = [
        [
            str(index),
            _safe(row.get("condition_name", "Unknown")),
            _fmt_int(row.get("affected_patients")),
        ]
        for index, row in enumerate(rows, start=1)
    ]
    return _table(["Rank", "Disease", "Affected Patients"], table_rows, "Run setup to build disease rankings.")


def _deceased_disease_table(rows):
    table_rows = [
        [
            str(index),
            _safe(row.get("condition_name", "Unknown")),
            _fmt_int(row.get("deceased_patients")),
            _fmt_int(row.get("affected_patients")),
            _fmt_pct(row.get("deceased_association_pct")),
        ]
        for index, row in enumerate(rows, start=1)
    ]
    return _table(
        ["Rank", "Disease", "Deceased Patients", "Affected Patients", "Association Rate"],
        table_rows,
        "Run setup to build deceased-patient disease associations.",
    )


def _audit_table(rows):
    table_rows = [
        [
            f'#{_safe(row.get("batch_id", ""))}',
            _safe(row.get("execution_timestamp", "")),
            _fmt_int(row.get("source_records_read")),
            _fmt_int(row.get("silver_records_written")),
            _fmt_int(row.get("quarantine_records_written")),
            _fmt_pct(row.get("pass_rate_pct")),
        ]
        for row in _as_list(rows)[:8]
    ]
    return _table(["Batch", "Time", "Records", "Clean", "Quarantine", "Pass Rate"], table_rows, "No audit records available.")


def _comorbidity_table(rows):
    table_rows = [
        [
            _safe(row.get("condition_a", "")),
            _safe(row.get("condition_b", "")),
            _fmt_int(row.get("patient_count")),
        ]
        for row in rows
    ]
    return _table(["Condition A", "Condition B", "Patients"], table_rows, "Run a new batch to build comorbidity pairs.")


def _return_gap_table(rows):
    order = {"0-7 days": 0, "8-30 days": 1, "31-90 days": 2, "90+ days": 3}
    table_rows = [
        [
            _safe(row.get("return_gap_bucket", "")),
            _fmt_int(row.get("encounter_count")),
            _fmt_int(row.get("patient_count")),
        ]
        for row in sorted(rows, key=lambda row: order.get(row.get("return_gap_bucket"), 99))
    ]
    return _table(["Return Gap", "Encounters", "Patients"], table_rows, "Run a new batch to build return-gap distribution.")


def _population_table(rows):
    table_rows = [
        [
            _safe(row.get("age_group", "")),
            _safe(row.get("gender", "")),
            _safe(row.get("race", "")),
            _fmt_int(row.get("unique_patients")),
        ]
        for row in sorted(rows, key=lambda row: _num(row.get("unique_patients")), reverse=True)[:16]
    ]
    return _table(["Age Group", "Gender", "Race", "Patients"], table_rows, "No demographic Gold records available.")


def render_dashboard(summary):
    latest = summary["latest_audit"]
    kpis = summary["kpis"]
    html = f"""<!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>
        :root {{
          --bg: #f4f7fb;
          --panel: #ffffff;
          --ink: #172033;
          --muted: #68748a;
          --line: #dfe6f0;
          --accent: #0f9f93;
          --blue: #2563eb;
          --danger: #dc2626;
        }}
        * {{ box-sizing: border-box; }}
        body {{
          margin: 0;
          font-family: Arial, Helvetica, sans-serif;
          color: var(--ink);
          background: var(--bg);
        }}
        main {{ width: 100%; margin: 0 auto; }}
        header {{
          display: flex;
          justify-content: space-between;
          gap: 24px;
          align-items: flex-start;
          margin-bottom: 24px;
        }}
        h1 {{ margin: 0; font-size: 34px; line-height: 1.1; letter-spacing: 0; }}
        h2 {{ margin: 0 0 14px; font-size: 20px; letter-spacing: 0; }}
        p {{ color: var(--muted); }}
        .eyebrow {{
          color: var(--accent);
          font-weight: 700;
          font-size: 12px;
          letter-spacing: .06em;
          text-transform: uppercase;
          margin-bottom: 8px;
        }}
        .timestamp {{ color: var(--muted); font-size: 13px; text-align: right; min-width: 210px; }}
        .grid {{ display: grid; gap: 14px; }}
        .metrics {{ grid-template-columns: repeat(4, minmax(0, 1fr)); margin-bottom: 14px; }}
        .panel, .metric-card {{
          background: var(--panel);
          border: 1px solid var(--line);
          border-radius: 8px;
          box-shadow: 0 1px 2px rgba(15, 23, 42, .04);
        }}
        .panel {{ padding: 18px; margin-top: 14px; overflow-x: auto; }}
        .metric-card {{ padding: 18px; min-height: 112px; }}
        .metric-card span {{ color: var(--muted); font-size: 13px; font-weight: 700; }}
        .metric-card strong {{ display: block; margin-top: 8px; font-size: 26px; color: var(--ink); }}
        .metric-card small {{ display: block; margin-top: 8px; color: var(--muted); }}
        .bar-row {{ margin: 13px 0; }}
        .bar-label {{ display: flex; justify-content: space-between; gap: 16px; font-size: 14px; }}
        .bar-label span {{ white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
        .bar-label strong {{ color: var(--blue); }}
        .bar-track {{ height: 8px; background: #edf2f7; border-radius: 999px; margin-top: 7px; overflow: hidden; }}
        .bar-fill {{ height: 100%; background: linear-gradient(90deg, var(--accent), var(--blue)); border-radius: inherit; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
        th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--line); vertical-align: top; }}
        th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; }}
        .badge {{
          display: inline-block;
          padding: 4px 8px;
          border-radius: 999px;
          color: var(--danger);
          background: #fee2e2;
          font-weight: 700;
          font-size: 12px;
          white-space: nowrap;
        }}
        footer {{ color: var(--muted); font-size: 13px; margin: 20px 0 4px; }}
        @media (max-width: 860px) {{
          header {{ display: block; }}
          .metrics {{ grid-template-columns: 1fr 1fr; }}
          .timestamp {{ text-align: left; margin-top: 10px; }}
        }}
        @media (max-width: 620px) {{
          .metrics {{ grid-template-columns: 1fr; }}
          h1 {{ font-size: 28px; }}
          table {{ font-size: 11px; }}
          th, td {{ padding: 8px 6px; }}
        }}
      </style>
    </head>
    <body>
      <main>
        <header class="report-header">
          <div>
            <div class="eyebrow">Clinical 360 Gold Report</div>
            <h1 class="report-title">Clinical 360 Patient Risk & Care Quality Intelligence</h1>
            <p class="report-subtitle">Focused view of who needs attention, why they need attention, and whether the pipeline run is trustworthy.</p>
          </div>
          <div class="timestamp">Generated<br>{_safe(summary["generated_at"])}</div>
        </header>

        <section class="grid metrics">
          {_metric_card("Latest Batch Records", _fmt_int(latest.get("source_records_read", 0)), latest.get("status", "Latest batch"))}
          {_metric_card("Quality Pass Rate", _fmt_pct(latest.get("pass_rate_pct", 0)), "Hard quality gates")}
          {_metric_card("Patients Monitored", _fmt_int(kpis.get("patients_monitored", summary["mastered_patients"])), "Full Patient 360 Gold table")}
          {_metric_card("High-Risk Patients", _fmt_int(kpis.get("high_risk_patients", summary["high_risk_patients"])), "Clinical risk index >= 60")}
          {_metric_card("Critical Care Gap Patients", _fmt_int(kpis.get("critical_care_gaps", 0)), "Urgent follow-up queue")}
          {_metric_card("Patients With Recent Return", _fmt_int(kpis.get("recent_return_signal_patients", 0)), "Distinct patients with a return within 30 days")}
          {_metric_card("Pipeline Trust Score", _fmt_pct(kpis.get("pipeline_trust_score_pct", 0)), "Required fields and date validity")}
        </section>

        <section class="panel">
          <h2>Priority Patient Worklist</h2>
          {_patient_360_table(summary["top_patient_360"])}
        </section>

        <section class="panel">
          <h2>Population Health Profile</h2>
          <p>Monitored patient identities grouped by age band, gender, and race.</p>
          {_population_table(summary["demographics"])}
        </section>

        <section class="panel">
          <h2>Comorbidity Pairs</h2>
          <p>Conditions that repeatedly appear in the same patient history.</p>
          {_comorbidity_table(summary["comorbidity_pairs"])}
        </section>

        <section class="panel">
          <h2>Care Gap Worklist</h2>
          {_care_gap_table(summary["top_care_gaps"])}
        </section>

        <section class="panel">
          <h2>Highest-Risk Diagnoses</h2>
          {_risk_bars(summary["top_risk"])}
          <h3>Most Common Diseases</h3>
          <p>Ranked by distinct monitored synthetic patients affected.</p>
          {_common_disease_table(summary["common_diseases"])}
          <h3>Diseases Associated With Deceased Patients</h3>
          <p>Ranked by deceased monitored synthetic patients who had the disease before death. This is association, not a proven cause of death.</p>
          {_deceased_disease_table(summary["deceased_disease_associations"])}
        </section>

        <section class="panel">
          <h2>Return Gap Distribution</h2>
          <p>Days between consecutive encounters. This is a return-gap view, not a CMS readmission metric.</p>
          {_return_gap_table(summary["return_gaps"])}
        </section>

        <section class="panel">
          <h2>Recent Batch Ledger</h2>
          {_audit_table(summary["audit"])}
        </section>

        <footer>
          Source: Gold serving snapshot. Spark Structured Streaming + Delta Lake produce the data; this dashboard presents the final Gold outputs.
        </footer>
      </main>
    </body>
    </html>
    """
    st.html(html)


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--snapshot", default=str(DEFAULT_SNAPSHOT_PATH))
    args, _ = parser.parse_known_args()

    st.set_page_config(
        page_title="Clinical 360 Dashboard",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    inject_css()

    snapshot_path = Path(args.snapshot)
    snapshot, modified_at = load_snapshot(snapshot_path)

    with st.sidebar:
        st.title("Clinical 360")
        refresh_enabled = st.toggle("Auto refresh", value=True)
        refresh_seconds = st.slider("Refresh seconds", 5, 60, 10, 5)
        st.caption(f"Snapshot: {snapshot_path}")
        if modified_at:
            st.caption("Updated: " + datetime.fromtimestamp(modified_at).strftime("%Y-%m-%d %H:%M:%S"))
        if st.button("Refresh now", use_container_width=True):
            st.rerun()

    if not snapshot:
        st.warning("No Gold snapshot found yet. Run `./scripts/project.sh run` to generate it.")
        if refresh_enabled:
            time.sleep(refresh_seconds)
            st.rerun()
        return

    render_dashboard(summarize(snapshot))

    if refresh_enabled:
        time.sleep(refresh_seconds)
        st.rerun()


if __name__ == "__main__":
    main()
