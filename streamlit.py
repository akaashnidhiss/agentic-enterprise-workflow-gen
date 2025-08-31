"""
Streamlit UI for Planner → Executor → UI pipeline

Reads the contracts defined under ./backend/ and presents:
- Checks list (checks/checks.json)
- Recent runs (cached_mem/checks_last.csv)
- Run detail (cached_mem/workflows.json)
- Planner conversation log (cached_mem/plan_prompt_log.jsonl)
- Explainability caches (cached_mem/ai_table_summaries.json, cached_mem/schema_cols.json)

How to run (from repo root):
  pip install streamlit pandas
  streamlit run streamlit.py

If your backend folder is elsewhere, set it in the sidebar.
"""
from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))  # Adds project root to sys.path
import json
import csv
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import pandas as pd
import streamlit as st

# -----------------------------
# Config & helpers
# -----------------------------

st.set_page_config(page_title="Checks Dashboard", layout="wide")

DEFAULT_BASE_DIR = "backend"

STATUS_COLORS = {
    "PASS": "#10b981",    # emerald
    "FAIL": "#ef4444",    # red
    "ERROR": "#f59e0b",   # amber
    "SKIPPED": "#6b7280", # gray
}

@st.cache_data(ttl=2)
def read_json(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

@st.cache_data(ttl=2)
def read_jsonl(path: Path, max_lines: Optional[int] = None) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    if max_lines is not None:
        lines = lines[-max_lines:]
    out: List[Dict[str, Any]] = []
    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except Exception:
            # skip malformed lines
            pass
    return out

@st.cache_data(ttl=2)
def read_csv_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        # fallback: manual csv
        rows = []
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append(dict(r))
        return pd.DataFrame(rows)


def badge(text: str, color: str) -> str:
    return f"""
    <span style='display:inline-block;padding:2px 8px;border-radius:999px;background:{color};color:white;font-weight:600;font-size:0.85rem;'>
        {text}
    </span>
    """


def find_latest_status_for_check(check_id: str, checks_last: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if checks_last.empty:
        return None
    df = checks_last[checks_last["check_id"].astype(str) == str(check_id)].copy()
    if df.empty:
        return None
    # sort by finished_at if present
    if "finished_at" in df.columns:
        try:
            df["_finished_at"] = pd.to_datetime(df["finished_at"], errors="coerce")
            df = df.sort_values("_finished_at", ascending=False)
        except Exception:
            pass
    return df.iloc[0].to_dict()


def runs_for_check(workflows: Dict[str, Any], check_id: str) -> List[Dict[str, Any]]:
    # Find the key that starts with the selected check_id
    key = next((k for k in workflows.keys() if k.startswith(f"{check_id}::")), None)
    if key:
        return workflows[key]
    return []


def extract_run_ids(runs: List[Dict[str, Any]]) -> List[str]:
    ids = []
    for r in runs:
        rid = r.get("run_id")
        if rid:
            ids.append(rid)
    # sort newest first based on timestamp inside run_id if present
    try:
        ids = sorted(ids, key=lambda s: s.split("_")[0], reverse=True)
    except Exception:
        pass
    return ids


def safe_get(d: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return cur if cur is not None else default


def filter_plan_logs_by_check(plan_logs: List[Dict[str, Any]], check_id: str) -> List[Dict[str, Any]]:
    return [x for x in plan_logs if str(x.get("check_id")) == str(check_id)]


def pretty_json(obj: Any) -> str:
    try:
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        return str(obj)


# -----------------------------
# Sidebar: base dir and check selection
# -----------------------------

st.sidebar.header("Settings")
base_dir = Path(st.sidebar.text_input("Backend base dir", value=DEFAULT_BASE_DIR))

checks_path = base_dir.resolve() / "checks" / "checks.json"
schema_path = base_dir.resolve() / "cached_mem" / "schema_cols.json"
ai_summaries_path = base_dir.resolve() / "cached_mem" / "ai_table_summaries.json"
checks_last_path = base_dir.resolve() / "cached_mem" / "checks_last.csv"
workflows_path = base_dir.resolve() / "cached_mem" / "workflows.json"
plan_log_path = base_dir.resolve() / "cached_mem" / "plan_prompt_log.jsonl"
last_result_path = base_dir.resolve() / "cached_mem" / "last_result.txt"

col1, col2 = st.sidebar.columns(2)
with col1:
    if st.button("Refresh data"):
        read_json.clear()
        read_jsonl.clear()
        read_csv_df.clear()
        st.rerun()
with col2:
    st.write("")

checks = read_json(checks_path) or []
checks_df = pd.DataFrame(checks)

if checks_df.empty:
    st.sidebar.error(f"No checks found at {checks_path}")
    st.stop()

# Display basic filters
enabled_only = st.sidebar.checkbox("Show only enabled", value=False)
if enabled_only and "enabled" in checks_df.columns:
    checks_df = checks_df[checks_df["enabled"].astype(str).str.lower().isin(["true", "1", "yes"])].copy()

# Choose a check
check_names = [f"{row.get('check_name','(unnamed)')} ({row.get('check_id','?')})" for _, row in checks_df.iterrows()]
choice = st.sidebar.selectbox("Select a check", options=check_names)
sel_idx = check_names.index(choice)
sel_row = checks_df.iloc[sel_idx].to_dict()
sel_check_id = str(sel_row.get("check_id"))

# -----------------------------
# Load rest of the artifacts
# -----------------------------

checks_last = read_csv_df(checks_last_path)
workflows = read_json(workflows_path) or {"runs": []}
plan_logs_all = read_jsonl(plan_log_path, max_lines=2000)
plan_logs_for_check = filter_plan_logs_by_check(plan_logs_all, sel_check_id)

schema_cols = read_json(schema_path) or {}
ai_summaries = read_json(ai_summaries_path) or {}

# -----------------------------
# Main layout
# -----------------------------

st.title("Checks Dashboard")

# Header with status
latest_row = find_latest_status_for_check(sel_check_id, checks_last)
status_text = (latest_row or {}).get("status", "-")
status_color = STATUS_COLORS.get(str(status_text).upper(), "#6b7280")

left, right = st.columns([0.7, 0.3])
with left:
    st.subheader(sel_row.get("check_name", "(Unnamed check)"))
    meta = {
        "Check ID": sel_check_id,
        "Severity": sel_row.get("severity", "-"),
        "Owner": sel_row.get("owner", "-"),
        "Tags": ", ".join(sel_row.get("tags", [])) if isinstance(sel_row.get("tags"), list) else sel_row.get("tags", "") or "-",
        "Target table(s)": sel_row.get("target_table", "-"),
    }
    st.write("\n".join([f"**{k}:** {v}" for k, v in meta.items()]))
with right:
    st.markdown(badge(str(status_text), status_color), unsafe_allow_html=True)
    if latest_row and latest_row.get("summary"):
        st.caption(str(latest_row.get("summary")))

# Recent runs table
st.markdown("---")
st.markdown("### Recent runs")
if checks_last.empty:
    st.info("No recent runs.")
else:
    show_df = checks_last.copy()
    # keep only this check's runs visible by default, with toggle
    only_this = st.checkbox("Show only this check", value=True)
    if only_this:
        show_df = show_df[show_df["check_id"].astype(str) == sel_check_id]
    st.dataframe(show_df, use_container_width=True, hide_index=True)


# import sys
# sys.path.append(str(base_dir))  # Use base_dir, not base_dir / "backend"

from backend.agent import main as agent_main

if st.button("Run Agent Workflow"):
    with st.spinner("Running agent.py..."):
        agent_main()
    st.success("Agent workflow completed.")

# -----------------------------
# Run detail
# -----------------------------
st.markdown("---")
st.markdown("### Run detail")

_sel_runs = runs_for_check(workflows, sel_check_id)
if not _sel_runs:
    st.info("No run details found for this check in workflows.json")
else:
    # Build labels from the structure you actually have: type + compiled_at (+ status if present)
    def run_label(run: Dict[str, Any]) -> str:
        t = run.get("type", "?")
        ts = run.get("compiled_at", "")
        res_status = (run.get("artifact", {}) or {}).get("result", {}) or {}
        s = res_status.get("status")
        return f"{t}{f' — {s}' if s else ''} @ {ts}"

    # Use index-based selection (since there's no run_id in the JSON you posted)
    idx_options = list(range(len(_sel_runs)))
    run_idx = st.selectbox("Choose a run", options=idx_options,
                           format_func=lambda i: run_label(_sel_runs[i]),
                           index=0)
    run_obj = _sel_runs[run_idx]
    artifact = run_obj.get("artifact", {}) or {}

    # Summary line (execution runs have artifact.result)
    result = artifact.get("result") or {}
    if result:
        result_status = result.get("status", "-")
        result_summary = result.get("summary", "-")
        st.markdown(
            badge(str(result_status), STATUS_COLORS.get(str(result_status).upper(), "#6b7280")),
            unsafe_allow_html=True
        )
        st.write(result_summary)
    else:
        st.caption("Plan artifact (no execution result recorded).")

    # Code & stdout
    st.markdown("#### Code & output")
    repls: List[Dict[str, Any]] = artifact.get("python_repls", []) or []
    if not repls:
        st.caption("No python executions recorded.")
    for i, cell in enumerate(repls):
        st.write(f"**Cell {i+1}** (exit_code={cell.get('exit_code', '-')})")
        st.code(cell.get("python_repl", "") or "# (no code)", language="python")
        if cell.get("stdout"):
            with st.expander("stdout"):
                st.code(str(cell.get("stdout")))

    # Saved/linked artifacts (prefer explicit saved path if present; else fallback)
    saved_to = (result.get("saved") or {}).get("saved_to") or str(last_result_path)
    if saved_to and Path(saved_to).exists():
        with st.expander("Saved artifact"):
            try:
                content = Path(saved_to).read_text(encoding="utf-8")
                st.code(content)
            except Exception as e:
                st.caption(f"Could not read {saved_to}: {e}")

    # Plan details (when the selected entry is a plan)
    with st.expander("Planner plan JSON (artifact)"):
        st.code(pretty_json(artifact), language="json")

    # Planner conversation remains filtered by check_id as you already do:
    with st.expander("Planner conversation (prompt & response)"):
        if not plan_logs_for_check:
            st.caption("No planner logs for this check.")
        else:
            # same logic you already have…
            started_at = run_obj.get("started_at")
            def parse_ts(ts: Optional[str]) -> Optional[datetime]:
                if not ts:
                    return None
                try:
                    return datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except Exception:
                    return None
            start_dt = parse_ts(started_at)
            logs = plan_logs_for_check
            if start_dt:
                def dist(l):
                    lts = parse_ts(l.get("ts"))
                    return abs((lts - start_dt).total_seconds()) if lts and start_dt else 9e18
                logs = sorted(logs, key=dist)
            logs = logs[:8]
            for j, rec in enumerate(logs, start=1):
                st.write(f"**Log {j} — {rec.get('ts','')} — model: {rec.get('model','')}**")
                for m in rec.get("prompt_msgs", []):
                    st.markdown(f"**{m.get('role','').upper()}**")
                    st.code(m.get("content", ""))
                with st.expander("model response"):
                    st.code(rec.get("response_text", ""))
                with st.expander("inputs passed to planner"):
                    st.code(pretty_json(rec.get("inputs", {})), language="json")
            dl = "\n".join(json.dumps(x, ensure_ascii=False) for x in plan_logs_for_check)
            st.download_button("Download planner logs (this check)", dl.encode("utf-8"),
                               file_name=f"{sel_check_id}_planner_logs.jsonl")

    with st.expander("AI table summaries"):
        if not ai_summaries:
            st.caption("No ai_table_summaries.json found")
        else:
            # Show only relevant tables if target_table exists
            tgt = sel_row.get("target_table") or ""
            targets = [t.strip() for t in str(tgt).split(",") if t.strip()]
            tables = targets or list(ai_summaries.keys())
            for t in tables:
                if t in ai_summaries:
                    st.write(f"**{t}**")
                    st.code(pretty_json(ai_summaries.get(t)), language="json")

    with st.expander("Schemas (schema_cols.json)"):
        if not schema_cols:
            st.caption("No schema_cols.json found")
        else:
            tgt = sel_row.get("target_table") or ""
            targets = [t.strip() for t in str(tgt).split(",") if t.strip()]
            to_show = {t: schema_cols.get(t) for t in (targets or list(schema_cols.keys())) if t in schema_cols}
            st.code(pretty_json(to_show or schema_cols), language="json")

# Footer note
st.markdown("---")
st.caption(f"Base dir: {base_dir.resolve()} | checks: {checks_path} | workflows: {workflows_path} | log: {plan_log_path}")
