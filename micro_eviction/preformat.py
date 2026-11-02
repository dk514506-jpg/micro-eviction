"""mev.preformat — minimal-column loader for marker-rule surfaces (B-set).

The marker-rule surface (B-SET FROZEN contract) derives compaction from the
LAST '[CONTEXT COMPACTION' / '[PRIOR CONTEXT' / handoff marker rather than from
schema columns, so it is portable across schemas. The retired root state.db is
post-reformat (active/compacted/_compressed_summary columns present); the truly
pre-reformat copies are the .bak files. This loader is retained and
fixture-locked so genuinely pre-reformat .bak surfaces can still be split.

The new-schema path in partition.py is untouched; this module is the only
loader used for marker-rule surfaces.
"""
from __future__ import annotations
import sqlite3
from pathlib import Path

DEFAULT_DB = Path.home() / ".hermes" / "state.db"

MARKER_HANDOFF = "compacted into the summary below"
MARKER_PREFIXES = ("[CONTEXT COMPACTION", "[PRIOR CONTEXT")


def is_compaction_marker(content: str | None) -> bool:
    if not content:
        return False
    if content.startswith(MARKER_PREFIXES):
        return True
    return MARKER_HANDOFF in content[:200]


def load_messages(session_id: str, db_path: str | Path = DEFAULT_DB) -> list[dict]:
    """Minimal-column load (id, content only) — works on any schema."""
    uri = f"file:{Path(db_path).resolve()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as con:
        rows = con.execute(
            "SELECT id, content FROM messages WHERE session_id=? ORDER BY id",
            (session_id,),
        ).fetchall()
    return [{"id": int(r[0]), "content": r[1] or ""} for r in rows]


def split_regions(messages: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """Marker-rule split: (surface, tail, markers).

    surface = every message up to and including the LAST compaction marker.
    tail = messages after that marker. markers = the marker messages themselves
    (each is a compaction boundary the eviction hook fires on).
    """
    markers = [m for m in messages if is_compaction_marker(m["content"])]
    if not markers:
        return list(messages), [], []
    last_marker_id = markers[-1]["id"]
    surface = [m for m in messages if m["id"] <= last_marker_id]
    tail = [m for m in messages if m["id"] > last_marker_id]
    return surface, tail, markers


def compaction_boundaries(markers: list[dict]) -> list[dict]:
    return [
        {"boundary_id": m["id"], "ts": None, "head": m["content"][:120]}
        for m in markers
    ]


def load_session_texts(session_id: str, db_path: str | Path = DEFAULT_DB) -> dict[int, str]:
    return {m["id"]: m["content"] for m in load_messages(session_id, db_path)}


def load_turns(session_id: str, db_path: str | Path = DEFAULT_DB) -> list[dict]:
    """User turns without display_kind (column may be absent on pre-reformat copies)."""
    uri = f"file:{Path(db_path).resolve()}?mode=ro"
    with sqlite3.connect(uri, uri=True) as con:
        rows = con.execute(
            "SELECT id, timestamp, content FROM messages "
            "WHERE session_id=? AND role='user' "
            "AND content NOT LIKE '[%' ORDER BY id",
            (session_id,),
        ).fetchall()
    out = []
    for r in rows:
        ts = r[1]
        out.append({
            "msg_id": int(r[0]),
            "ts": float(ts) if ts is not None else 0.0,
            "text": r[2] or "",
        })
    return out


def partition_anchors(
    session_id: str,
    anchors_items: dict[str, list[str]],
    db_path: str | Path = DEFAULT_DB,
) -> dict:
    """Marker-rule partition. Criterion unchanged: compacted-reachable iff
    EVERY anchor literal appears (case-insensitively) in the concatenated text
    of the surface (up to and including the last compaction marker)."""
    messages = load_messages(session_id, db_path)
    surface, tail, markers = split_regions(messages)
    comp_text = "\n".join(m["content"] for m in surface).lower()
    tail_text = "\n".join(m["content"] for m in tail).lower()

    result = {
        "session_id": session_id,
        "surface_mode": "marker-rule",
        "schema_note": ("retired root state.db is post-reformat (active/compacted/"
                        "_compressed_summary columns present); the truly pre-reformat "
                        "copies are the .bak files (E/F5 completion, 2026-10-28)"),
        "criterion": (
            "compacted-reachable iff every anchor literal appears case-insensitively "
            "in the concatenated text of the compaction surface (all messages up to "
            "and including the LAST marker: '[CONTEXT COMPACTION' / '[PRIOR CONTEXT' / "
            "'compacted into the summary below' in first 200 chars); tail-only otherwise"
        ),
        "compaction_boundaries": compaction_boundaries(markers),
        "compacted_region_msgs": len(surface),
        "tail_region_msgs": len(tail),
        "last_marker_id": markers[-1]["id"] if markers else None,
        "compacted_reachable": [],
        "tail_only": [],
        "detail": {},
    }

    for aid in _sorted_ids(anchors_items):
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


def _sorted_ids(anchors_items: dict) -> list[str]:
    """Sort B1..B10 / A1..A15 style ids (tolerates decorated names like B1_helix)."""
    def key(x: str) -> tuple:
        num = ""
        i = 1
        while i < len(x) and x[i].isdigit():
            num += x[i]
            i += 1
        return (x[0], int(num) if num else 0, x)
    return sorted(anchors_items, key=key)


def load_compacted_texts(
    session_id: str, db_path: str | Path = DEFAULT_DB
) -> dict[int, str]:
    messages = load_messages(session_id, db_path)
    surface, _, _ = split_regions(messages)
    return {m["id"]: m["content"] for m in surface}
