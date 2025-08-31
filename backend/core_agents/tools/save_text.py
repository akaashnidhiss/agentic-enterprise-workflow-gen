# tools/save_text.py
import os, json
from typing import Dict, Optional
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

class SaveTextInput(BaseModel):
    path: str = Field(..., description="Relative file path (e.g., './cached_mem/result.txt').")
    text: Optional[str] = Field(
        None,
        description="Text content to write. If omitted, the tool will try to parse JSON from `path`.",
    )

def _save_text(path: str, text: Optional[str] = None) -> Dict[str, str]:
    # If agent mistakenly passed a JSON string into `path`, recover it.
    if text is None:
        try:
            obj = json.loads(path)
            # Accept {"path": "...", "text": "..."} or {"text": "..."} forms
            if isinstance(obj, dict):
                path = obj.get("path", path)
                text = obj.get("text", text)
        except Exception:
            pass

    if not text:
        raise ValueError("save_text: `text` is required (either as argument or inside JSON).")

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return {"saved_to": path, "bytes": str(len(text))}

def make_save_text_tool():
    return StructuredTool.from_function(
        name="save_text",
        description="Save provided text to a file (UTF-8). Call with {'path': '...', 'text': '...'}",
        func=_save_text,
        return_direct=False,
    )
