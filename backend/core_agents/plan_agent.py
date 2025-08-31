import os, time, json, csv, hashlib, datetime
from typing import Dict, Any, List, Optional
import openai
from .utils.plan_logging import log_plan_interaction
from dotenv import load_dotenv
load_dotenv()


MODEL_NAME = "gpt-5-mini"
openai.api_key = os.getenv("OPENAI_API_KEY")

def _get_openai_client():
    """
    Returns an OpenAI client instance.
    Assumes you have set the OPENAI_API_KEY environment variable.
    """
    openai.api_key = os.getenv("OPENAI_API_KEY")
    return openai


def chat_with_AI(prompt_msgs, model=MODEL_NAME, temperature=1):
    """
    Chat function used to interact with the agent.
    Retries with exponential backoff. Caller handles parsing.
    """
    # client = _get_openai_client()
    delay = 1.5
    for _ in range(3):
        try:
            rsp = openai.chat.completions.create(
                model=model,
                messages=prompt_msgs,
                # temperature=temperature,
            )
            return rsp.choices[0].message.content
        except Exception as e:
            print(f"LLM error: {e}")
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("LLM failed 3 ×")


# ----------------------------
# Utilities
# ----------------------------

def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: str, obj) -> None:
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def _dir_list(root: str = ".") -> List[str]:
    paths = []
    for base, _, files in os.walk(root):
        # Skip venvs and .git to keep it clean
        if any(skip in base for skip in (".git", ".venv", "venv", "__pycache__")):
            continue
        for f in files:
            paths.append(os.path.join(base, f))
    return sorted(paths)


def _format_dir_tree(root: str = ".") -> str:
    """
    Human-friendly tree for the prompt (stable, compact).
    """
    lines = []
    root = os.path.abspath(root)
    for p in _dir_list(root):
        lines.append(os.path.relpath(p, root))
    return "\n".join(lines)


def _read_csv_head(csv_path: str, n: int = 100) -> Dict[str, Any]:
    """
    Read header and up to n rows from a CSV, returning simple profiling info.
    No pandas dependency.
    """
    out = {"columns": [], "rows_read": 0, "samples_by_col": {}, "distinct_counts": {}}
    if not os.path.exists(csv_path):
        return out

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cols = list(reader.fieldnames or [])
        out["columns"] = cols
        # gather up to 4 examples and distinct counts (bounded)
        examples = {c: [] for c in cols}
        seen = {c: set() for c in cols}
        limit = max(50, n)  # bound for distinct sampling
        for i, row in enumerate(reader):
            if i >= n:
                break
            out["rows_read"] += 1
            for c in cols:
                v = row.get(c, "")
                if len(examples[c]) < 4 and v not in examples[c]:
                    examples[c].append(v)
                if len(seen[c]) < limit:
                    seen[c].add(v)
        out["samples_by_col"] = examples
        out["distinct_counts"] = {c: len(seen[c]) for c in cols}
    return out


def _guess_type(samples: List[str]) -> str:
    """
    Tiny type guesser for prompt context.
    """
    if not samples:
        return "unknown"
    digits, floats, dates = 0, 0, 0
    for s in samples[:8]:
        s2 = (s or "").strip()
        if s2.isdigit():
            digits += 1
            continue
        try:
            float(s2)
            floats += 1
            continue
        except Exception:
            pass
        # very rough date sniff
        if any(ch in s2 for ch in ("-", "/", ":")) and any(k in s2.lower() for k in ("202", "19", "jan", "feb", "mar", "apr",
                                                                                      "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")):
            dates += 1
    if dates >= max(digits, floats, 1):
        return "date/datetime-like"
    if digits > floats and digits >= 2:
        return "integer-like"
    if floats >= 2:
        return "float-like"
    return "text/mixed"


def _column_cards(head_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    cards = []
    for c in head_info.get("columns", []):
        ex = head_info["samples_by_col"].get(c, [])
        cards.append({
            "name": c,
            "type_guess": _guess_type(ex),
            "examples": ex,
            "distinct_count_sampled": head_info["distinct_counts"].get(c, 0),
        })
    return cards


def _hash_schema(cols: List[str]) -> str:
    s = "|".join(cols)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


# ----------------------------
# AI Table Summaries (cached)
# ----------------------------

def _ai_table_summaries(
    schema_cols: Dict[str, List[str]],
    data_dir: str = "data",
    cache_path: str = "cached_mem/ai_table_summaries.json",
) -> Dict[str, Dict[str, Any]]:
    """
    Uses the LLM to generate short natural-language summaries of tables.
    Caches by (table, column schema). If schema unchanged, reuses cache.

    Returns:
      {
        "<table>": {
          "columns": [...],
          "schema_hash": "...",
          "summary": "2–3 sentence AI-generated summary ...",
          "llm_model": "gpt-5-mini",
          "updated_at": "ISO8601",
        },
        ...
      }
    """
    cache = _load_json(cache_path, default={})
    out: Dict[str, Dict[str, Any]] = {}

    for table, cols in schema_cols.items():
        cols = list(cols or [])
        new_hash = _hash_schema(cols)

        cached = cache.get(table)
        if cached and cached.get("schema_hash") == new_hash and cached.get("summary"):
            # reuse cached
            out[table] = cached
            continue

        # Build compact “table card”
        csv_path = os.path.join(data_dir, f"{table}.csv")
        head_info = _read_csv_head(csv_path, n=100)
        col_cards = _column_cards(head_info)

        sys = {
            "role": "system",
            "content": (
                "You are a data analyst. Given a table description (columns, example values, distinct counts), "
                "write a concise 2–3 sentence summary describing entity type, keys, likely joins, and any quality flags. "
                "Keep it under 80 words. Do NOT invent columns."
            )
        }
        usr = {
            "role": "user",
            "content": json.dumps({
                "table_name": table,
                "columns_declared": cols,
                "profile": {
                    "rows_sampled": head_info.get("rows_read", 0),
                    "columns": col_cards
                }
            }, ensure_ascii=False)
        }

        try:
            llm_text = chat_with_AI([sys, usr], model=MODEL_NAME, temperature=0)
        except Exception as e:
            print(f"[AI summary] Error for table '{table}': {e}")
            llm_text = f"Table '{table}' with columns: {', '.join(cols)}. (Fallback summary; LLM unavailable [AI summary error]: {e})"

        out[table] = {
            "columns": cols,
            "schema_hash": new_hash,
            "summary": llm_text.strip(),
            "llm_model": MODEL_NAME,
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
        }

    # persist merged cache (keep other tables if present)
    merged = dict(cache)
    merged.update(out)
    _save_json(cache_path, merged)
    return out


# ----------------------------
# Planning
# ----------------------------

def _derive_steps(check_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Minimal fallback plan: load → compute → decide → emit.
    """
    targets = [s.strip() for s in str(check_row.get("target_table", "")).split(",") if s.strip()]
    calc_hint = check_row.get("calculation_hint", "")
    return [
        {"step": 1, "action": "load_tables", "targets": targets, "notes": "Read required CSVs from ./data"},
        {"step": 2, "action": "compute_metrics", "notes": f"Follow hint: {calc_hint}"},
        {"step": 3, "action": "decide_status", "notes": "Map computed metrics to PASS/FAIL/SKIPPED/ERROR"},
        {"step": 4, "action": "emit_summary", "notes": "Print single-line summary for human readout"},
    ]


def plan(
    prompt: str,
    check_row: Dict[str, Any],
    schema_cols: Dict[str, List[str]],
    repo_root: str = ".",
) -> Dict[str, Any]:
    """
    Creates a model-generated structured plan using:
      - check_row (the check definition),
      - directory listing (so the model knows what's in /data),
      - AI table summaries (from cache or fresh),
      - declared schema_cols.

    If the model returns invalid JSON, falls back to a deterministic artifact.
    """
    targets = [s.strip() for s in str(check_row.get("target_table", "")).split(",") if s.strip()]
    dir_tree = _format_dir_tree(repo_root)
    ai_summaries = _ai_table_summaries(schema_cols)

    # Build strict JSON contract for the model to fill
    contract = {
        "check_id": check_row.get("check_id"),
        "check_name": check_row.get("check_name"),
        "targets": targets,
        "ai_table_summaries": ai_summaries,  # model may reference but must not alter
        "dir_list": dir_tree.splitlines(),
        "steps": "LLM_TO_FILL",
        "output_contract": {
            "format": "single_line",
            "fields": ["status", "summary"],
            "status_domain": ["PASS", "FAIL", "SKIPPED", "ERROR"]
        }
    }

    sys = {
        "role": "system",
        "content": (
            "You are a senior data engineer. Create a STRICT JSON plan for an execution agent.\n"
            "Constraints:\n"
            "- Return ONLY JSON (no prose).\n"
            "- Each step MUST include: {step:int, action:str, notes:str, inputs?:object, outputs?:object}.\n"
            "- Prefer these actions when relevant: load_tables, transform, compute_metrics, validate, decide_status, emit_summary, write_artifact.\n"
            "- Use provided ai_table_summaries and dir_list to ground paths and column names; do not invent columns.\n"
            "- If multiple tables are needed, list them in 'inputs'.\n"
            "- Keep to ~4–8 steps."
        )
    }
    usr = {
        "role": "user",
        "content": json.dumps({
            "goal_prompt": prompt,
            "check_row": check_row,
            "schema_cols_declared": schema_cols,
            "dir_tree": dir_tree,
            "ai_table_summaries": ai_summaries,
            "return_json_shape": contract
        }, ensure_ascii=False)
    }

    plan_obj: Optional[Dict[str, Any]] = None
    try:
        raw = chat_with_AI([sys, usr], model=MODEL_NAME, temperature=0)
        log_plan_interaction(
            check_id=str(check_row.get("check_id") or ""),
            model=MODEL_NAME,
            prompt_msgs=[sys, usr],   # the exact list you passed to the model
            response_text=raw,
            inputs={
                "dir_list": dir_tree.splitlines(),
                "schema_cols": schema_cols,
                "ai_table_summaries": ai_summaries
            },
        )
        # Be tolerant of code fences or stray text:
        clean = raw.strip()
        if clean.startswith("```"):
            clean = clean.strip("`")
            # could be like ```json ... ```
            if "\n" in clean:
                clean = "\n".join(clean.split("\n")[1:-1]).strip()
        plan_obj = json.loads(clean)
    except Exception as e:
        print(f"[plan] JSON parse failed or LLM error → fallback. Details: {e}")

    if not isinstance(plan_obj, dict) or "steps" not in plan_obj:
        # Deterministic fallback
        plan_obj = {
            "check_id": check_row.get("check_id"),
            "check_name": check_row.get("check_name"),
            "targets": targets,
            "ai_table_summaries": ai_summaries,
            "dir_list": dir_tree.splitlines(),
            "steps": _derive_steps(check_row),
            "output_contract": {
                "format": "single_line",
                "fields": ["status", "summary"],
                "status_domain": ["PASS", "FAIL", "SKIPPED", "ERROR"]
            }
        }

    return plan_obj
