# Pandas Codegen Agent — Planner → Executor → UI

**Goal:** Turn changing **business rules** into reliable **Pandas data manipulation code**, cache that code as a **runnable artifact**, and **re-run** it until requirements or schemas change. When the inputs change, the agent **replans** and **regenerates** the transformation (an “Agentic RPA using Pandas”).

This prototype demonstrates a full pass-through system:

* **Planner**: turns a check (business requirement) + table context into a **structured plan**.
* **Executor**: turns the plan into **Python/Pandas** steps and runs them on local tables, saving artifacts.
* **UI (Streamlit)**: reads **contracts** and **logs** to visualize checks, runs, plans, code, and outputs.

---

## Why this exists

1. **Business rules change.** Hard-coded ETL breaks.
2. **We cache “code as runnable”.** Once compiled, users can re-run without re-planning.
3. **We re-plan only when needed.** If checks or schemas change, the agent rebuilds the plan and code.

---

## Architecture (high level)

```
        checks/checks.json         cached_mem/schema_cols.json    cached_mem/ai_table_summaries.json
                │                                │                                   │
                └──────────────┬─────────────────┴───────────────────────────────────┘
                               ▼
                       ┌────────────────┐         (prompt utils inject schema + table hints)
                       │   PLANNER      │  <-- cached_mem / prompt utilities
                       │  (use_agent)   │
                       └───────┬────────┘
                               │ plan artifact (JSON)
                               ▼
                       ┌────────────────┐         (custom pandas execution tool)
                       │   EXECUTOR     │  → generates & runs Pandas code,
                       │  (use_agent)   │    saves Excel/CSV artifacts as needed
                       └───────┬────────┘
                               │ execution artifact (python_repls, result)
                               ▼
        cached_mem/workflows.json    cached_mem/checks_last.csv*   cached_mem/plan_prompt_log.jsonl*
                   │                               │                         │
                   └─────────────── UI (streamlit.py) ───────────────────────┘
```

\* `checks_last.csv` and `plan_prompt_log.jsonl` are evolving—see **Current JSON shapes** and **Roadmap**.

---

## Key design choices

* **Contracts first:** UI reads files written by agents—no hidden state.
* **Prompt contexting:** cached memory injects **schema columns** (and, in the fuller design, **AI table summaries** and directory hints) to improve planning accuracy.
* **Deterministic caches:** stable CSV hashing and schema snapshots to detect change.
* **Observability:** plans, executions, and LLM prompt/response logs are persisted for audit.
* **Separation of concerns:** `use_agent` encapsulates plan/execute; `agent.py` handles change detection + caching; `streamlit.py` is read-only visualization + a “Run Agent Workflow” trigger.

---

## How the agent decides (plan → execute → cache)

1. **Change detection (`agent.py`):**

   * Hash-stable snapshot of `checks.json` → triggers recompilation if changed.
   * Snapshot of **schema columns** from `data/*.csv` → triggers recompilation for impacted checks.

2. **Planner (inside `use_agent`):**

   * Receives `check_row` + `schema_cols` (and, in the fuller design, **dir listing** + **AI table summaries** from `cached_mem/ai_table_summaries.json`).
   * Produces a **structured plan** (instructions / pseudo code / I/O intent).

3. **Executor (inside `use_agent`):**

   * Generates **deterministic Pandas code** (can touch multiple tables).
   * Runs code and returns:

     ```json
     {
       "python_repls": [{ "python_repl": "...", "stdout": "...", "exit_code": 0 }],
       "result": { "status": "PASS|FAIL|ERROR|SKIPPED", "summary": "..." }
     }
     ```
   * Can save artifacts (CSV/Excel) for downstream consumption.

4. **Caching:**

   * Both `plan` and `execution` artifacts are written to `cached_mem/workflows.json`.
   * Re-use cached code until **checks or schema change**.

5. **Observability (planned):**

   * `cached_mem/plan_prompt_log.jsonl` will append every planner prompt/response for UI audit.

---

## Streamlit UI (read-only dashboard + trigger)

`streamlit.py` renders:

* **Checks**: from `checks/checks.json` (filters, metadata).
* **Recent runs**: from `cached_mem/checks_last.csv` (to be evolved into a rollup).
* **Run detail** (per check): shows status badge, summary, Pandas code, `stdout`, and full plan artifact.
* **Planner conversation** (when `plan_prompt_log.jsonl` is present): shows prompt/response and inputs.
* **Schemas** and **AI summaries** under expanders.

It also includes a **Run Agent Workflow** button that calls `backend.agent.main()` to recompile when inputs change.

---
## Repository layout

```
backend/
├─ agent.py                      # orchestrates detect→plan→execute→cache
├─ use_agent.py                  # (not shown here) returns {"plan": {...}, "execution": {...}}
├─ checks/
│  └─ checks.json                # registry of checks (business rules)
├─ cached_mem/
│  ├─ schema_cols.json           # current table -> [columns]
│  ├─ checks_last.csv            # (prototype) baseline of checks.json for change detection
│  ├─ workflows.json             # compiled plan/execution artifacts per check
│  ├─ ai_table_summaries.json    # (planned) table blurbs for prompt context
│  └─ plan_prompt_log.jsonl      # (planned) planner LLM I/O ledger
└─ data/
   ├─ users.csv
   ├─ events.csv
   └─ orders.csv

streamlit.py                     # UI for contracts & runs (run from repo root)
```

---

## Current JSON shapes (prototype)

### 1) `checks/checks.json`

Minimal example:

```json
[
  {
    "check_id": "CHK-002",
    "check_name": "Unknown users in events",
    "target_table": "events,users",
    "calculation_hint": "Flag events rows whose user_id is missing from users.",
    "severity": "medium",
    "enabled": true,
    "owner": "data-quality@company.com",
    "tags": ["integrity", "join"]
  }
]
```

### 2) `cached_mem/schema_cols.json`

Generated from the CSV files in `backend/data`:

```json
{
  "users": ["user_id", "email", "signup_at"],
  "events": ["event_id", "user_id", "name", "start_time"],
  "orders": ["order_id", "user_id", "event_id", "quantity", "price", "created_at"]
}
```

### 3) `cached_mem/workflows.json` (prototype shape used by `agent.py`)

Keyed by `<check_id>::<check_name>`, value is a list with **plan** and **execution** entries:

```json
{
  "CHK-002::Unknown users in events": [
    {
      "type": "plan",
      "artifact": { "...": "planner artifact JSON" },
      "compiled_at": "2025-08-31T10:40:00Z",
      "compiled_against": {
        "checks_hash": "sha256...",
        "schema_cols": { "events": [...], "users": [...] }
      }
    },
    {
      "type": "execution",
      "artifact": {
        "python_repls": [
          { "python_repl": "import pandas as pd\n...", "stdout": "PASS: 0 unknown users", "exit_code": 0 }
        ],
        "result": { "status": "PASS", "summary": "PASS: 0 unknown users" }
      },
      "compiled_at": "2025-08-31T10:40:00Z",
      "compiled_against": {
        "checks_hash": "sha256...",
        "schema_cols": { "events": [...], "users": [...] }
      }
    }
  ]
}
```

### 4) `cached_mem/checks_last.csv` (prototype)

Currently used as a **normalized baseline** of `checks.json` for change detection (not a “recent runs” rollup yet).
A future migration can introduce a separate `checks_rollup.csv` for UI’s “Recent runs”.

---

## Quickstart

### 1) Install

```bash
pip install -r requirements.txt
# or minimal:
pip install streamlit pandas
# (If you plan to read/write Excel artifacts)
pip install openpyxl
```

### 2) Project structure

Place your CSV tables in `backend/data/` and your checks in `backend/checks/checks.json`.

### 3) Run the UI

```bash
streamlit run streamlit.py
```

* The sidebar `Backend base dir` should point to `backend/` (default).
* Click **Run Agent Workflow** to compile plans/executions when inputs change.

### 4) CLI (optional)

```bash
python -m backend.agent
# or
python backend/agent.py
```

---

## Authoring checks

Minimal check:

```json
{
  "check_id": "CHK-010",
  "check_name": "Orders without users",
  "target_table": "orders,users",
  "calculation_hint": "Find orders where user_id is not present in users.",
  "severity": "high",
  "enabled": true,
  "owner": "data-quality@company.com",
  "tags": ["integrity","join"]
}
```

* **`target_table`** can be a comma-separated string or a list.
* The planner receives these targets plus `schema_cols` to prevent hallucinated columns.

---

## Extending the execution tool (multi-table, Excel artifacts)

* The executor can read multiple tables (`users.csv`, `events.csv`, `orders.csv`) into Pandas, join/filter/aggregate, and **save** outputs (CSV or Excel) for downstream systems.
* To enable Excel:

  * Use `pd.read_excel` / `DataFrame.to_excel(...)` in your execution step.
  * Ensure `openpyxl` is installed.


---

## License

MIT

---

## Notes for maintainers

* Python ≥ 3.10 recommended.
* If you switch data files to Excel, update `load_csv` in `agent.py` to use `read_excel` (and rename accordingly), and add `openpyxl` to requirements.
* The `use_agent` abstraction is intentionally thin so you can swap in a real LLM planner/executor without touching `agent.py` or the UI.
