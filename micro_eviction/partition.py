"""mev.partition — anchor partition against compaction boundaries (W2a / D2).

Classifies fidelity anchors into compacted-reachable (all anchor strings appear
in the compacted region of the target session) vs tail-only (some anchor string
exists only in the not-yet-compacted active tail). The compacted region is
every message with active=0 or _compressed_summary=1 — the surface the eviction
hook serves.
"""
from __future__ import annotations
import sqlite3
from pathlib import Path

DEFAULT_DB = Path.home() / ".hermes" / "state.db"


def load_messages(session_id: str, db_path: str | Path = DEFAULT_DB) -> list[dict]:
    from . import store_dispatch
    _cc = store_dispatch.loader_for(db_path)
    if _cc is not None:
        return _cc[1](session_id, db_path)
    uri = f"file:{Path(db_path).resolve()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as con:
        rows = con.execute(
            "SELECT id, content, active, compacted, _compressed_summary "
            "FROM messages WHERE session_id=? ORDER BY id",
            (session_id,),
        ).fetchall()
    return [
        {
            "id": int(r[0]),
            "content": r[1] or "",
            "active": int(r[2]),
            "compacted": int(r[3]),
            "csum": int(r[4]),
        }
        for r in rows
    ]


def split_regions(messages: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split messages into (compacted_region, tail). Tail = active non-summary."""
    compacted = []
    tail = []
    for m in messages:
        is_tail = m["active"] == 1 and m["csum"] == 0 and m["compacted"] == 0
        (tail if is_tail else compacted).append(m)
    return compacted, tail


def compaction_boundaries(messages: list[dict]) -> list[dict]:
    """Return the _compressed_summary=1 messages as boundary events."""
    return [
        {"boundary_id": m["id"], "ts": None, "active": m["active"],
         "compacted": m["compacted"], "head": m["content"][:120]}
        for m in messages
        if m["csum"] == 1
    ]


def partition_anchors(
    session_id: str,
    anchors_items: dict[str, list[str]],
    db_path: str | Path = DEFAULT_DB,
) -> dict:
    """Partition anchors into compacted-reachable vs tail-only.

    An anchor is compacted-reachable iff EVERY literal string in its anchor
    list appears (case-insensitive) in the concatenated text of the compacted
    region. Tail-only means at least one literal appears only in the active tail.
    """
    messages = load_messages(session_id, db_path)
    compacted, tail = split_regions(messages)
    comp_text = "\n".join(m["content"] for m in compacted).lower()
    tail_text = "\n".join(m["content"] for m in tail).lower()

    result = {
        "session_id": session_id,
        "criterion": (
            "compacted-reachable iff every anchor literal appears case-insensitively "
            "in the concatenated text of the compacted region (active=0 or "
            "_compressed_summary=1); tail-only otherwise"
        ),
        "compaction_boundaries": compaction_boundaries(messages),
        "compacted_region_msgs": len(compacted),
        "tail_region_msgs": len(tail),
        "compacted_reachable": [],
        "tail_only": [],
        "detail": {},
    }

    for aid in sorted(anchors_items, key=lambda x: int(x[1:])):
        alist = anchors_items[aid]
        all_in_comp = all(a.lower() in comp_text for a in alist)
        all_in_tail = all(a.lower() in tail_text for a in alist)
        missing_from_comp = [a for a in alist if a.lower() not in comp_text]
        cls = "compacted-reachable" if all_in_comp else "tail-only"
        result[cls.replace("-", "_")].append(aid)
        result["detail"][aid] = {
            "classification": cls,
            "all_in_compacted": all_in_comp,
            "all_in_tail": all_in_tail,
            "missing_from_compacted": missing_from_comp,
        }

    result["compacted_reachable_count"] = len(result["compacted_reachable"])
    result["tail_only_count"] = len(result["tail_only"])
    result["falsifier_triggered"] = result["compacted_reachable_count"] < 5
    result["falsifier_text"] = (
        f"compacted-reachable={result['compacted_reachable_count']} < 5: "
        "bar is structurally meaningless; report and STOP (charge pre-registered falsifier)"
        if result["falsifier_triggered"]
        else "compacted-reachable >= 5: bar measurable"
    )
    return result


def load_compacted_texts(
    session_id: str, db_path: str | Path = DEFAULT_DB
) -> dict[int, str]:
    """Return {msg_id: content} for the compacted region only."""
    messages = load_messages(session_id, db_path)
    compacted, _ = split_regions(messages)
    return {m["id"]: m["content"] for m in compacted}
