# """
# A tiny, swappable facade for your future LLM agent.

# For now:
# - It takes a `check_row` (one row from checks.json) and `schema_cols` (table->columns list)
# - Produces a naive pandas REPL string that *would* implement the check.
# - We don't execute it here; agent.py just caches it for later execution by a REPL runner.

# Later:
# - Replace this with a real prompt and call to your LLM/tooling.
# """

# from typing import Dict, Any, List


# PROMPT_TEMPLATE = """
# You are a data quality analyst. Produce *only* Python (pandas) code that performs the described check.
# Assume CSVs live in ./data and have already been loaded by the caller if needed.
# You MUST:
# - Write code that reads only the necessary CSV(s) using pandas.read_csv (no imports besides pandas).
# - Name DataFrames after their tables (e.g., users, events, orders).
# - Calculate the metric(s) needed for the check.
# - Print a single human-readable line to stdout summarizing result and key numbers.
# Do NOT include explanations or comments—only code.

# Check:
# - check_id: {check_id}
# - check_name: {check_name}
# - description: {description}
# - calculation_hint: {calculation_hint}
# - target_table(s): {target_tables}

# Schemas (columns per table):
# {schemas}
# """.strip()


# def _format_schema(schema_cols: Dict[str, List[str]]) -> str:
#     if not schema_cols:
#         return "(none)"
#     lines = []
#     for t, cols in schema_cols.items():
#         lines.append(f"- {t}: {', '.join(cols) if cols else '(no columns)'}")
#     return "\n".join(lines)


# def _default_code_for(check: Dict[str, Any], schema_cols: Dict[str, List[str]]) -> str:
#     """
#     Dumb code generator for a few common patterns—good enough to cache and demo.
#     If the check name contains certain keywords, we tailor the code. Otherwise, a generic loader.
#     """
#     name = (check.get("check_name") or "").lower()
#     targets = [t for t in (check.get("target_table") or "").split(",") if t.strip()]
#     targets = [t.strip() for t in targets]

#     # Helper snippets
#     load_snips = {
#         "users":  "users = pd.read_csv('./data/users.csv')",
#         "events": "events = pd.read_csv('./data/events.csv')",
#         "orders": "orders = pd.read_csv('./data/orders.csv')",
#     }

#     # Base import line
#     header = "import pandas as pd"

#     # Keyword heuristics (extend later as needed)
#     if "distinct user drop" in name and "events" in targets:
#         code = f"""
# {header}
# {load_snips['events']}
# events['event_date'] = pd.to_datetime(events['event_date']).dt.date
# g = events.groupby('event_date')['user_id'].nunique().sort_index()
# if len(g) < 2:
#     print("SKIPPED: Need at least 2 days of events")
# else:
#     last, prev = g.iloc[-1], g.iloc[-2]
#     if prev == 0:
#         print("SKIPPED: Previous day had 0 users")
#     else:
#         drop_pct = (prev - last) / prev * 100
#         status = "FAIL" if drop_pct > 20 else "PASS"
#         print(f"{{status}}: Distinct users prev={{prev}}, last={{last}}, drop={{drop_pct:.1f}}%")
# """.strip()
#         return code

#     if "new nulls" in name and "users" in targets:
#         code = f"""
# {header}
# {load_snips['users']}
# null_email = users['email'].isna().sum() if 'email' in users.columns else -1
# null_signup = users['signup_date'].isna().sum() if 'signup_date' in users.columns else -1
# print(f"INFO: nulls email={{null_email}}, signup_date={{null_signup}}")
# """.strip()
#         return code

#     if "unknown users" in name and "events" in targets and "users" in targets:
#         code = f"""
# {header}
# {load_snips['events']}
# {load_snips['users']}
# known = set(users['user_id'].astype(str)) if 'user_id' in users.columns else set()
# ev_unknown = (~events['user_id'].astype(str).isin(known)).sum() if 'user_id' in events.columns else -1
# status = "FAIL" if ev_unknown > 0 else "PASS"
# print(f"{{status}}: events_with_unknown_users={{ev_unknown}}")
# """.strip()
#         return code

#     # Generic fallback: load targets and print head shapes
#     lines = [header]
#     for t in targets:
#         if t in load_snips:
#             lines.append(load_snips[t])
#     lines.append("print(" + repr(f"Loaded: {', '.join(targets) if targets else '(no targets)'}") + ")")
#     return "\n".join(lines)


# def use_agent(check_row: Dict[str, Any], schema_cols: Dict[str, List[str]]) -> Dict[str, str]:
#     """
#     Replace this with a real LLM prompt call later.
#     For now, we build a prompt (unused) and return naive code that matches the check.
#     """
#     prompt = PROMPT_TEMPLATE.format(
#         check_id=check_row.get("check_id"),
#         check_name=check_row.get("check_name"),
#         description=check_row.get("description"),
#         calculation_hint=check_row.get("calculation_hint"),
#         target_tables=check_row.get("target_table"),
#         schemas=_format_schema(schema_cols),
#     )

#     # Future: send `prompt` to your LLM. For now, heuristic generator:
#     code = _default_code_for(check_row, schema_cols)

#     return {"python_repl": code}



from typing import Dict, Any, List
from backend.core_agents.orchestrator_agent import orchestrate

PROMPT_TEMPLATE = """
You are a data quality analyst. Produce *only* Python (pandas) code that performs the described check.
Assume CSVs live in ./data and may be loaded by you.
You MUST:
- Use pandas only.
- Name DataFrames after their tables (users, events, orders) when you load them.
- Calculate the metric(s) needed for the check.
- Print a single human-readable line to stdout summarizing result and key numbers.
- Do not include comments or explanation text (code only).
Check:
- check_id: {check_id}
- check_name: {check_name}
- description: {description}
- calculation_hint: {calculation_hint}
- target_table(s): {target_tables}
Schemas (columns per table):
{schemas}
""".strip()


def _format_schema(schema_cols: Dict[str, List[str]]) -> str:
    if not schema_cols:
        return "(none)"
    lines = []
    for t, cols in schema_cols.items():
        lines.append(f"- {t}: {', '.join(cols) if cols else '(no columns)'}")
    return "\n".join(lines)


def use_agent(check_row: Dict[str, Any], schema_cols: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Orchestrates planning + execution via the orchestrator (which calls plan_agent and execute_agent).
    Returns a dict with both the planning artifact and the execution artifact.
    Also includes `python_repl` (first code block) for backward compatibility with older callers.
    """
    prompt = PROMPT_TEMPLATE.format(
        check_id=check_row.get("check_id"),
        check_name=check_row.get("check_name"),
        description=check_row.get("description"),
        calculation_hint=check_row.get("calculation_hint"),
        target_tables=check_row.get("target_table"),
        schemas=_format_schema(schema_cols),
    )

    # Delegate to orchestrator (which calls plan_agent and execute_agent)
    orchestrated = orchestrate(prompt=prompt, check_row=check_row, schema_cols=schema_cols)

    # Back-compat convenience: surface first python_repl (if any) at the top level
    first_repl = None
    try:
        first_repl = (orchestrated.get("execution", {})
                                   .get("python_repls", [])[0]
                                   .get("python_repl"))
    except Exception:
        pass

    return {
        "plan": orchestrated.get("plan"),
        "execution": orchestrated.get("execution"),
        "python_repl": first_repl or "",
    }
