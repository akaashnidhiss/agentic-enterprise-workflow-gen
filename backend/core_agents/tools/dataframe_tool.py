# tools/dataframe_tool.py
import os
import sys
import re
import subprocess
from pathlib import Path
from typing import Dict, Any
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

def _strip_fences(code: str) -> str:
    s = code.strip()
    if s.startswith("```"):
        s = re.sub(r"^```[\w+-]*\n", "", s)  # drop opening fence
        if s.endswith("```"):
            s = s[:-3]
    return s

def _find_project_root(start: Path) -> Path:
    """
    Walk up from `start` looking for a folder that has a 'data' subdir.
    Returns the first match; falls back to `start`.
    """
    for p in [start] + list(start.parents):
        if (p / "data").is_dir():
            return p
    return start

def _run_python_repl(code: str, desired_root: str | None = None) -> Dict[str, Any]:
    # Resolve a stable cwd that contains ./data
    here = Path(__file__).resolve().parent
    root = Path(desired_root).resolve() if desired_root else _find_project_root(here)
    env = os.environ.copy()
    # env["DATA_DIR"] = str(root / "data")  # let code use os.getenv("DATA_DIR")
    env["DATA_DIR"] = os.environ['LOCAL_DATA_DIR']

    clean = _strip_fences(code)

    cmd = [sys.executable, "-c", clean]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=90,
            cwd=str(root),     # <— key change
            env=env,
        )
        return {
            "stdout": (proc.stdout or "").strip(),
            "stderr": (proc.stderr or "").strip(),
            "exit_code": proc.returncode,
            "python_repl": code,       # keep original for logging
            "cwd": str(root),
            "data_dir": env["DATA_DIR"]
        }
    except Exception as e:
        return {
            "stdout": "",
            "stderr": f"{type(e).__name__}: {e}",
            "exit_code": -1,
            "python_repl": code,
            "cwd": str(root),
            "data_dir": env.get("DATA_DIR", "")
        }

class PandasExecInput(BaseModel):
    code: str = Field(
        ...,
        description=(
            "Pure Python code that uses pandas to read CSVs. Prefer "
            "DATA_DIR = os.getenv('DATA_DIR') to build file paths."
        ),
    )

def make_pandas_exec_tool(allowed_root: str | None = None):
    def _run(code: str) -> Dict[str, Any]:
        return _run_python_repl(code, desired_root=allowed_root)
    return StructuredTool.from_function(
        name="pandas_exec",
        description=(
            "Execute provided Python (pandas) code. CWD is set to a folder that contains ./data. "
            "An env var DATA_DIR is provided for stable paths."
        ),
        func=_run,
        args_schema=PandasExecInput,
        return_direct=False,
    )
