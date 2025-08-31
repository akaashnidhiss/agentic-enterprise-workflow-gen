import os
import json
import hashlib
import datetime
from typing import Dict, Any, List, Tuple

import pandas as pd

# Local "agent" that fabricates python REPL steps based on the check and schema.
from backend.use_agent import use_agent

ROOT = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, "data")
CHECKS_JSON = os.path.join(ROOT, "checks", "checks.json")
CACHE_DIR = os.path.join(ROOT, "cached_mem")
os.makedirs(CACHE_DIR, exist_ok=True)

CHECKS_LAST_CSV = os.path.join(CACHE_DIR, "checks_last.csv")
SCHEMA_COLS_CACHE = os.path.join(CACHE_DIR, "schema_cols.json")
WORKFLOW_CACHE = os.path.join(CACHE_DIR, "workflows.json")


# -------------------------
# Helpers
# -------------------------
def _stable_csv(df: pd.DataFrame) -> str:
    """Return a normalized CSV string (sorted cols/rows) for stable hashing/compare."""
    # Sort columns and rows for stability
    df2 = df.copy()
    # Ensure consistent column order
    cols = sorted(df2.columns.tolist())
    df2 = df2[cols]
    # Try to coerce list/dict columns to JSON strings for stability
    for c in df2.columns:
        df2[c] = df2[c].apply(lambda x: json.dumps(x, sort_keys=True) if isinstance(x, (list, dict)) else x)
    # Sort rows by all cols (stringify to avoid mixed types)
    df2["_sortkey"] = df2.astype(str).agg("|".join, axis=1)
    df2 = df2.sort_values("_sortkey").drop(columns=["_sortkey"])
    return df2.to_csv(index=False, lineterminator="\n")


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def load_csv(name: str) -> pd.DataFrame:
    p = os.path.join(DATA_DIR, f"{name}.csv")
    if not os.path.exists(p):
        # If the table is absent, return empty df with no cols so schema change detection can work.
        return pd.DataFrame()
    df = pd.read_csv(p)
    # soft parse date-like columns
    for c in df.columns:
        lc = c.lower()
        if "date" in lc or "login" in lc:
            try:
                df[c] = pd.to_datetime(df[c]).dt.date
            except Exception:
                pass
    return df


def detect_checks_changes() -> Tuple[bool, pd.DataFrame]:
    """Returns (changed?, checks_df). Also updates the cached CSV if changed."""
    if not os.path.exists(CHECKS_JSON):
        raise FileNotFoundError(f"Missing {CHECKS_JSON}")

    with open(CHECKS_JSON, "r", encoding="utf-8") as f:
        checks = json.load(f)

    # Normalize to DataFrame
    checks_df = pd.DataFrame(checks)
    if checks_df.empty:
        # Create a stable empty CSV if needed
        current_csv = "check_id\n"  # minimal
    else:
        current_csv = _stable_csv(checks_df)

    if not os.path.exists(CHECKS_LAST_CSV):
        # First run: write baseline and mark as changed
        with open(CHECKS_LAST_CSV, "w", encoding="utf-8") as f:
            f.write(current_csv)
        return True, checks_df

    with open(CHECKS_LAST_CSV, "r", encoding="utf-8") as f:
        last_csv = f.read()

    changed = _sha256(current_csv) != _sha256(last_csv)
    if changed:
        with open(CHECKS_LAST_CSV, "w", encoding="utf-8") as f:
            f.write(current_csv)

    return changed, checks_df


def detect_schema_changes(tables: List[str]) -> Tuple[bool, Dict[str, List[str]]]:
    """
    Returns (any_changed?, current_schema_cols).
    Also updates the schema cache if changed.
    """
    current: Dict[str, List[str]] = {}
    for t in tables:
        df = load_csv(t)
        current[t] = df.columns.astype(str).tolist()

    if not os.path.exists(SCHEMA_COLS_CACHE):
        with open(SCHEMA_COLS_CACHE, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2)
        return True, current

    with open(SCHEMA_COLS_CACHE, "r", encoding="utf-8") as f:
        prev = json.load(f)

    changed = prev != current
    if changed:
        with open(SCHEMA_COLS_CACHE, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2)

    return changed, current


def load_workflows() -> Dict[str, List[Dict[str, Any]]]:
    if not os.path.exists(WORKFLOW_CACHE):
        return {}
    with open(WORKFLOW_CACHE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return {}


def save_workflows(wf: Dict[str, List[Dict[str, Any]]]) -> None:
    with open(WORKFLOW_CACHE, "w", encoding="utf-8") as f:
        json.dump(wf, f, indent=2)


def key_for(check_row: Dict[str, Any]) -> str:
    """Key format: '<check_id>::<check_name>'"""
    return f"{check_row.get('check_id')}::{check_row.get('check_name')}"


def parse_targets(target_table: Any) -> List[str]:
    """
    The checks may list 'events,users' or 'orders'.
    Return normalized list of table names (trimmed, lowercased).
    """
    if target_table is None:
        return []
    if isinstance(target_table, list):
        return [str(x).strip().lower() for x in target_table]
    if isinstance(target_table, str):
        return [x.strip().lower() for x in target_table.split(",") if x.strip()]
    # fallback
    return [str(target_table).strip().lower()]


# -------------------------
# Main routine
# -------------------------
def main():
    print("🚀 Starting agent...")
    # 1) Detect changes in checks.json vs cached CSV
    checks_changed, checks_df = detect_checks_changes()
    print(f"• checks.json changed? {checks_changed}")

    # 2) Detect schema/column changes on the observed tables
    #    Determine union of target tables from the checks registry
    if checks_df.empty:
        target_tables = []
    else:
        all_targets = set()
        for _, row in checks_df.iterrows():
            for t in parse_targets(row.get("target_table")):
                all_targets.add(t)
        target_tables = sorted(all_targets)

    schema_changed, current_schema = detect_schema_changes(target_tables)
    print(f"• schema columns changed? {schema_changed}")

    # 3) Load current workflows cache
    workflows = load_workflows()

    # 4) Decide which checks require (re)compilation
    checks_to_compile: List[Dict[str, Any]] = []
    if checks_changed or schema_changed:
        # If either checks or schema changed, we recompile only the affected checks.
        # Affected if:
        #   - checks_changed: compile all checks (simple rule)
        #   - schema_changed: compile checks whose target tables intersect changed tables
        changed_tables = set()
        if schema_changed:
            # Compare current vs cached to find exactly which tables changed
            try:
                with open(SCHEMA_COLS_CACHE, "r", encoding="utf-8") as f:
                    # This now contains CURRENT. We need previous to diff exactly, but we didn't retain it.
                    # For simplicity: if schema_changed True, treat all target tables as changed.
                    pass
            except Exception:
                pass
            changed_tables = set(target_tables)

        for _, row in checks_df.iterrows():
            targets = set(parse_targets(row.get("target_table")))
            if checks_changed:
                checks_to_compile.append(row.to_dict())
            else:
                # Only schema changed: include check if any target table changed
                if targets & changed_tables:
                    checks_to_compile.append(row.to_dict())
    else:
        # Nothing changed → reuse existing workflows
        checks_to_compile = []

    # 5) Compile (generate python REPL code) for selected checks
    compiled = 0
    if checks_to_compile:
        print(f"• Recompiling {len(checks_to_compile)} check(s)...")
        # Provide a thin "schema context" to the agent: table -> columns
        schema_context = {t: current_schema.get(t, []) for t in target_tables}

        for chk in checks_to_compile:
            k = key_for(chk)
            # Build an instruction payload for the agent
            # The agent returns {"python_repl": "....."}
            agent_out = use_agent(check_row=chk, schema_cols=schema_context)

            workflows[k] = [
                {
                    "type": "plan",
                    "artifact": agent_out.get("plan", {}),
                    "compiled_at": datetime.datetime.now(datetime.UTC).isoformat() + "Z",
                    "compiled_against": {
                        "checks_hash": _sha256(_stable_csv(checks_df) if not checks_df.empty else ""),
                        "schema_cols": schema_context
                    }
                },
                {
                    "type": "execution",
                    "artifact": agent_out.get("execution", {}),
                    "compiled_at": datetime.datetime.now(datetime.UTC).isoformat() + "Z",
                    "compiled_against": {
                        "checks_hash": _sha256(_stable_csv(checks_df) if not checks_df.empty else ""),
                        "schema_cols": schema_context
                    }
                }
            ]

            compiled += 1

        save_workflows(workflows)

    # 6) If nothing changed, we can simply "use" (not execute) the cached workflows
    if compiled == 0:
        print("• No changes detected. Using cached workflows as-is.")
    else:
        print(f"• Compiled and cached {compiled} workflow(s).")

    # 7) Show a concise summary of what would run (without executing)
    if workflows:
        print("\n=== Workflow Cache Summary ===")
        for k, steps in workflows.items():
            code_preview = (steps[0].get("python_repl") or "").strip().splitlines()
            head = code_preview[0] if code_preview else ""
            print(f"- {k}: {len(steps)} step(s). First line: {head[:110]}")
    else:
        print("\n(No workflows cached yet.)")


if __name__ == "__main__":
    main()
