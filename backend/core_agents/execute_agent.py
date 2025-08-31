# execute_agent.py
"""
LangChain-powered execution agent.

- Tools (imported from tools/):
  1) pandas_exec: runs pandas code and RETURNS the code + stdout (so we can store).
  2) save_text: writes final summary to a file.

- Contract: execute(...) returns:
    {
      "python_repls": [ { "python_repl": "...", "stdout": "...", "exit_code": 0 } ],
      "result": { "status": "...", "summary": "...", "raw_stdout": "..." }
    }
"""

from typing import Dict, Any, List, Optional
import json
import re
import os

import openai
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain.agents import create_react_agent, AgentExecutor
from langchain_core.prompts import PromptTemplate

from .tools.dataframe_tool import make_pandas_exec_tool
from .tools.save_text import make_save_text_tool

from dotenv import load_dotenv
load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")

# MODEL_NAME = "gpt-5-mini"
MODEL_NAME = "gpt-4.1-mini"


# -------- Utilities reused from your current script --------

def _parse_status(stdout: str) -> str:
    up = (stdout or "").upper()
    for tok in ("PASS", "FAIL", "SKIPPED", "ERROR"):
        if tok in up:
            return tok
    return "UNKNOWN"


def _status_line(stdout: str) -> str:
    """
    Return the last non-empty line (usually the PASS/FAIL... line).
    """
    if not stdout:
        return ""
    lines = [ln.strip() for ln in stdout.splitlines() if ln.strip()]
    return lines[-1] if lines else ""


# -------- LangChain agent construction --------
def _build_agent(model_name: str = "gpt-4o-mini", temperature: float = 0.0) -> AgentExecutor:
    """
    Creates a ReAct agent with the required prompt variables:
    {tools}, {tool_names}, {agent_scratchpad}, {input}
    """
    llm = ChatOpenAI(model=model_name)

    pandas_exec = make_pandas_exec_tool(allowed_root=".")
    save_text = make_save_text_tool()
    tools = [pandas_exec, save_text]

    # ReAct-style prompt (MUST contain tools, tool_names, agent_scratchpad, input)
    template = """You are a careful data QA engineer who uses Python (pandas) to validate checks.

You have access to the following tools:
{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: Return a short explanation AND include a JSON object with keys:
  - tool_result: the FULL JSON you observed from the *last* pandas_exec call
  - saved: the JSON you observed from save_text if you used it, else null

Begin!

Question: {input}
Thought:{agent_scratchpad}"""

    prompt = PromptTemplate.from_template(template)

    # IMPORTANT: ReAct agent expects {tools}, {tool_names}, {agent_scratchpad} placeholders above.
    agent = create_react_agent(
        llm=llm,
        tools=tools,
        prompt=prompt,
        # tools_renderer left default (text descriptions)
        # stop_sequence left default (adds "\nObservation" stop token)
    )

    # We want tool traces so we can grab pandas_exec outputs programmatically
    executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=False,
        handle_parsing_errors=True,
        return_intermediate_steps=True,
    )
    return executor

# -------- Public execute(...) – same return contract as before --------
def execute(prompt: str,
            check_row: Dict[str, Any],
            schema_cols: Dict[str, List[str]],
            plan_artifact: Dict[str, Any],
            model_name: str = "gpt-4.1-mini",
            save_path: Optional[str] = "./cached_mem/last_result.txt") -> Dict[str, Any]:
    """
    Runs a ReAct agent that generates pandas code and executes it through the pandas_exec tool.
    Returns:
      {
        "python_repls": [ { "python_repl": "...", "stdout": "...", "exit_code": 0 } ],
        "result": { "status": "...", "summary": "...", "raw_stdout": "..." }
      }
    """
    agent = _build_agent(model_name=model_name, temperature=0.0)

    # Build the single {input} string the ReAct prompt expects
    input_str = (
        "Goal:\n"
        f"{prompt}\n\n"
        "Context:\n"
        f"- check_row: {json.dumps(check_row, ensure_ascii=False)}\n"
        f"- schema_cols: {json.dumps(schema_cols, ensure_ascii=False)}\n"
        f"- plan_artifact: {json.dumps(plan_artifact, ensure_ascii=False)}\n"
        f"- save_path (optional): {save_path or ''}\n\n"
        "Instructions:\n"
        "1) Write concrete pandas code using only declared columns and files in ./data.\n"
        "2) Call the tool `pandas_exec` with the FULL code as a single string. "
        "   The code MUST print exactly one line containing PASS/FAIL/SKIPPED/ERROR and a short summary.\n"
        "3) If save_path is provided, call `save_text` with that one-line summary.\n"
        "4) In your Final Answer, include the FULL JSON returned by the *last* `pandas_exec` call as 'tool_result', "
        "   and any save_text JSON as 'saved' (or null).\n"
    )

    result = agent.invoke({"input": input_str})

    # Grab tool outputs cleanly from intermediate_steps
    # Each step is (AgentAction, observation). observation is whatever the tool returned.
    python_repls: List[Dict[str, Any]] = []
    last_pandas: Optional[Dict[str, Any]] = None
    saved_json: Optional[Dict[str, Any]] = None

    for action, observation in result.get("intermediate_steps", []):
        tool_name = getattr(action, "tool", "")
        if tool_name == "pandas_exec" and isinstance(observation, dict):
            last_pandas = observation
            python_repls.append({
                "python_repl": observation.get("python_repl", ""),
                "stdout": observation.get("stdout", ""),
                "exit_code": int(observation.get("exit_code", -1)),
            })
        elif tool_name == "save_text" and isinstance(observation, dict):
            saved_json = observation

    # If nothing captured (unexpected), fall back to final text
    final_text = result.get("output", "")
    if not python_repls:
        python_repls.append({
            "python_repl": "# (Code not recovered from agent run)",
            "stdout": (final_text or "").strip(),
            "exit_code": -1,
        })

    last_stdout = python_repls[-1]["stdout"]
    status = _parse_status(last_stdout)
    summary = _status_line(last_stdout) or (final_text or "no output").strip()

    return {
        "python_repls": python_repls,
        "result": {
            "status": status,
            "summary": summary,
            "raw_stdout": last_stdout,
            # (optional) debug fields if you want them later
            # "saved": saved_json,
            # "final_text": final_text,
        }
    }