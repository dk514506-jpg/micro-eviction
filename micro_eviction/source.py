"""mev.source — read-only access to the Hermes session store (state.db)."""
from __future__ import annotations
import sqlite3
from pathlib import Path

DEFAULT_DB = Path.home() / ".hermes" / "state.db"

def _connect(db_path: str | Path) -> sqlite3.Connection:
    uri = f"file:{Path(db_path).resolve()}?mode=ro"
    return sqlite3.connect(uri, uri=True)

def load_session_texts(session_id: str, db_path: str | Path = DEFAULT_DB) -> dict[int, str]:
    with _connect(db_path) as con:
        rows = con.execute(
            "SELECT id, content FROM messages WHERE session_id=? ORDER BY id", (session_id,)
        ).fetchall()
    return {int(r[0]): (r[1] or "") for r in rows}

def load_turns(session_id: str, db_path: str | Path = DEFAULT_DB) -> list[dict]:
    """Real user turns: role=user, no display_kind, content not starting with '['."""
    from . import store_dispatch
    _cc = store_dispatch.loader_for(db_path)
    if _cc is not None:
        return _cc[0](session_id, db_path)
    with _connect(db_path) as con:
        rows = con.execute(
            "SELECT id, timestamp, content FROM messages "
            "WHERE session_id=? AND role='user' "
            "AND (display_kind IS NULL OR display_kind='') "
            "AND content NOT LIKE '[%' ORDER BY id",
            (session_id,),
        ).fetchall()
    return [{"msg_id": int(r[0]), "ts": float(r[1]), "text": r[2] or ""} for r in rows]

def turn_ordinal(msg_id: int, turns: list[dict]) -> int:
    """1-based ordinal of the last turn boundary <= msg_id (0 if none)."""
    ord_ = 0
    for i, t in enumerate(turns, 1):
        if t["msg_id"] <= msg_id:
            ord_ = i
        else:
            break
    return ord_

def build_spine(session_id: str, texts: dict[int, str], turns: list[dict]) -> list[dict]:
    """Thin narrative spine: one entry per real user turn (mechanical, no LLM)."""
    spine: list[dict] = []
    ids = sorted(texts)
    for i, t in enumerate(turns, 1):
        nxt = turns[i]["msg_id"] if i < len(turns) else (ids[-1] + 1 if ids else 0)
        tail = ""
        # tail = last non-empty message content strictly before next turn
        for mid in sorted((m for m in ids if t["msg_id"] < m < nxt), reverse=True):
            c = texts[mid]
            if c.strip():
                tail = " ".join(c.split())
                break
        spine.append({
            "kind": "spine",
            "turn_id": i,
            "msg_id": t["msg_id"],
            "ts": t["ts"],
            "head": " ".join(t["text"].split())[:140],
            "tail": tail[:140],
        })
    return spine
