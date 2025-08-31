"""
Coordinates the planning and execution agents.

- plan_agent.plan(...) creates structured steps and an output contract (no code execution).
- execute_agent.execute(...) generates code, runs it in a sandboxed subprocess, and returns
  the code blocks used and the observed result.

This file stays thin so your "agentic framework" can evolve independently.
"""

from typing import Dict, Any, List
from .plan_agent import plan
from .execute_agent import execute


def orchestrate(prompt: str, check_row: Dict[str, Any], schema_cols: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Returns:
      {
        "plan": { ... planning artifact ... },
        "execution": {
            "python_repls": [ { "python_repl": "...", "stdout": "...", "exit_code": 0 } ],
            "result": { "status": "PASS/FAIL/SKIPPED/ERROR", "summary": "...", "raw_stdout": "..." }
        }
      }
    """
    # 1) Plan
    planning_artifact = plan(
        prompt=prompt,
        check_row=check_row,
        schema_cols=schema_cols,
    )

    # 2) Execute
    execution_artifact = execute(
        prompt=prompt,
        check_row=check_row,
        schema_cols=schema_cols,
        plan_artifact=planning_artifact,
    )

    return {
        "plan": planning_artifact,
        "execution": execution_artifact,
    }
