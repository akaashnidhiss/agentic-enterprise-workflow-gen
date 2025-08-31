# utils/plan_logging.py
import os, json, datetime
from typing import List, Dict, Any, Optional

PLAN_LOG_PATH = "cached_mem/plan_prompt_log.jsonl"

def _ensure_dir(p: str) -> None:
    os.makedirs(os.path.dirname(p), exist_ok=True)

def log_plan_interaction(
    *,
    check_id: str,
    model: str,
    prompt_msgs: List[Dict[str, Any]],
    response_text: str,
    inputs: Optional[Dict[str, Any]] = None,
    path: str = PLAN_LOG_PATH,
) -> str:
    """
    Appends a single JSON line with planner prompt/response for later UI consumption.
    Returns the file path.
    """
    rec = {
        "ts": datetime.datetime.utcnow().isoformat() + "Z",
        "check_id": check_id,
        "model": model,
        "prompt_msgs": prompt_msgs,
        "response_text": response_text,
        "inputs": inputs or {},
    }
    _ensure_dir(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return path

def tail_plan_logs(n: int = 50, path: str = PLAN_LOG_PATH) -> list[dict]:
    """
    Convenience for the UI or debugging: read last N entries from the JSONL file.
    """
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()
    out = []
    for ln in lines[-n:]:
        ln = ln.strip()
        if ln:
            try:
                out.append(json.loads(ln))
            except Exception:
                pass
    return out
