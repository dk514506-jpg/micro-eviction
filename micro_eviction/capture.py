"""mev.capture — Stage 0: proposal gate + dedup + append-only ledger + fidelity scoring.

Model proposes records; this module is the mechanical gate (schema, provenance
binding, dedup). Nothing is ever injected from here.
"""
from __future__ import annotations
import hashlib, re
from . import io
from .schema import validate_proposal

# ---------------------------------------------------------------------------
# FROZEN topic table — first match wins; unmatched -> ad-hoc (never groups).
# topic_hash = sha256(type + '|' + topic_id)[:16]
# ---------------------------------------------------------------------------
TOPIC_RULES: list[tuple[str, re.Pattern]] = [
    ("retune-rule", re.compile(r"re-?tun(e|ed|es|ing)\b.*\b(threshold|measurements?|bar)|"
                               r"(threshold|pre-?registered).{0,90}re-?tun(e|ed|ing)", re.I | re.S)),
    ("self-report-rule", re.compile(r"self-?report", re.I)),
    ("byte-identical", re.compile(r"byte-?identical", re.I)),
    ("raw-text-checker", re.compile(r"splitlines|\\r\\n|raw text", re.I)),
    ("sweep-order", re.compile(r"reverse_alpha", re.I)),
    ("digestion-choice", re.compile(r"digestion|MiMo-flash.{0,60}slices|slices-only", re.I | re.S)),
    ("gg27-symlink", re.compile(r"\bGG27\b|135th", re.I)),
    ("sweep-numbers", re.compile(r"485,090|3,658 claims|490,001|3,266,678|corpus baseline", re.I)),
    ("staging-sha", re.compile(r"aggregate hash|staging receipts|findings set sealed", re.I)),
    ("auditor-false-positive", re.compile(r"133 of 134|locator convention", re.I)),
    ("gh23-gap", re.compile(r"\bGH23\b|prose-limitation", re.I)),
    ("gen2-structural", re.compile(r"directed seed|Gen-1\b|structural gene", re.I)),
    ("ev2-adoption", re.compile(r"TOURN-EV2|fusion genome", re.I)),
    ("charge-v2", re.compile(r"V2 full-arc|full-arc charge|CHARGE-DRAFT-V2|V2 full-arc charge", re.I)),
    ("charge-v1", re.compile(r"v1 (micro-)?eviction charge|capture-only scope|MICRO-EVICTION-CHARGE-DRAFT-2026", re.I)),
]

def topic_id(content: str) -> str:
    for tid, pat in TOPIC_RULES:
        if pat.search(content):
            return tid
    return "adhoc:" + content_hash_raw(content)[:12]

def content_hash_raw(s: str) -> str:
    return hashlib.sha256(io.norm_content(s).encode("utf-8")).hexdigest()

def topic_hash(rtype: str, content: str) -> str:
    return hashlib.sha256(f"{rtype}|{topic_id(content)}".encode("utf-8")).hexdigest()[:16]

def make_record_id(session_id: str, turn_id: int, rtype: str, content: str) -> str:
    basis = f"{session_id}|{turn_id}|{rtype}|{io.norm_content(content)}"
    return "rec_" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:16]

# ---------------------------------------------------------------------------
# capture pipeline
# ---------------------------------------------------------------------------
def capture(session_id: str, proposals: list[dict], ledger_dir: str,
            session_texts: dict[int, str], turns: list[dict]) -> dict:
    """Gate proposals and append accepted records to ledgers/<session_id>.jsonl.

    Returns a capture report. Rejections never touch the ledger; exact-content
    duplicates are collapsed at capture (kept-first).

    LEGACY (V3 slice amendment): batch mode is bulk-import only. The live path
    calls only the slice extractor (extract_slices / capture_slice); this
    function is excluded from live ingestion. (S2 marker per plan §1.2 step 8.)
    """
    ledger = io.Path(ledger_dir) / f"{session_id}.jsonl"
    existing = io.load_jsonl(ledger)
    seen = {io.norm_content(r["content"]) for r in existing}
    accepted = 0
    collapsed = 0
    rejected: list[dict] = []

    # spine (mechanical) — appended once
    spine_path = io.Path(ledger_dir) / f"{session_id}.spine.jsonl"
    if not spine_path.exists():
        from .source import build_spine
        for entry in build_spine(session_id, session_texts, turns):
            io.append_jsonl(spine_path, entry)

    for prop in proposals:
        errs = validate_proposal(prop, session_texts)
        if errs:
            rejected.append({"content_head": prop.get("content", "")[:60], "errors": errs})
            continue
        key = io.norm_content(prop["content"])
        if key in seen:
            collapsed += 1
            continue
        turn_id = _turn_for(prop, turns)
        rec = {
            "record_id": make_record_id(session_id, turn_id, prop["type"], prop["content"]),
            "session_id": session_id,
            "turn_id": turn_id,
            "type": prop["type"],
            "content": prop["content"],
            "provenance": {
                "source_exchange_bounds": [list(r) for r in prop["bounds"]],
                "artifact_sha256_12": prop.get("artifact_sha256_12"),
            },
            "scope": prop["scope"],
            "confidence": float(prop["confidence"]),
            "mutable": bool(prop["mutable"]),
            "superseded_by": None,
        }
        io.append_jsonl(ledger, rec)
        seen.add(key)
        accepted += 1

    total_after = len(existing) + accepted
    return {
        "session_id": session_id,
        "ledger": str(ledger),
        "proposed": len(proposals),
        "accepted": accepted,
        "rejected": len(rejected),
        "rejected_detail": rejected,
        "collapsed_dups": collapsed,
        "records_after": total_after,
    }

def _turn_for(prop: dict, turns: list[dict]) -> int:
    from .source import turn_ordinal
    start = prop["bounds"][0][0]
    return turn_ordinal(start, turns)

def measure_residual_dups(ledger_path: str) -> dict:
    """Fraction of records sharing a normalized-content hash within one ledger."""
    recs = io.load_jsonl(ledger_path)
    if not recs:
        return {"records": 0, "residual_dups": 0, "residual_pct": 0.0}
    from collections import Counter
    c = Counter(io.norm_content(r["content"]) for r in recs)
    dups = sum(v - 1 for v in c.values() if v > 1)
    return {"records": len(recs), "residual_dups": dups,
            "residual_pct": round(100.0 * dups / len(recs), 2)}

def score_fidelity(records: list[dict], anchors: dict, session_texts: dict[int, str]) -> dict:
    """Frozen matcher: item captured iff some single record holds ALL anchors in
    content AND the concatenated text of its provenance bounds."""
    results: dict[str, bool] = {}
    detail: dict[str, str | None] = {}
    for item, alist in anchors["items"].items():
        hit: str | None = None
        for rec in records:
            cl = rec["content"].lower()
            if not all(a.lower() in cl for a in alist):
                continue
            bounds_text = []
            ok_bounds = True
            for a, b in rec["provenance"]["source_exchange_bounds"]:
                for mid in range(a, b + 1):
                    if mid not in session_texts:
                        ok_bounds = False
                        break
                    bounds_text.append(session_texts[mid])
                if not ok_bounds:
                    break
            if not ok_bounds:
                continue
            bl = "\n".join(bounds_text).lower()
            if all(a.lower() in bl for a in alist):
                hit = rec["record_id"]
                break
        results[item] = hit is not None
        detail[item] = hit
    n = sum(results.values())
    total = len(results)
    return {"captured": n, "total": total,
            "fidelity_pct": round(100.0 * n / total, 2) if total else 0.0,
            "missing": [k for k, v in results.items() if not v],
            "by_item": detail}

def audit_routing(records: list[dict]) -> list[dict]:
    """Routing audit: global-scoped records carrying campaign-local tokens are flagged
    (auditable, never silently re-routed)."""
    flags = []
    pat = re.compile(r"campaigns/bench/sift|sweep-134/|TOURN-|wave-?20", re.I)
    for r in records:
        if r["scope"] == "global" and pat.search(r["content"]):
            flags.append({"record_id": r["record_id"], "scope": r["scope"],
                          "reason": "campaign-local token in global-scoped record"})
    return flags

# ===========================================================================
# V3 slice amendment (S2) — ADDITIVE ONLY; every symbol above is byte-unchanged.
# Paper-extraction-technique slicing (plan §1.1 / §1.2, R4/R16):
#   * slice_record_id  — pinned content-derived record_id (sha256 over
#     session|bounds_start|bounds_end|type|norm_content). make_record_id above
#     stays byte-unchanged (sealed-ledger derivation).
#   * extract_slices   — the frozen slice-boundary rule (bounded, anchored
#     per-slice extraction with source_exchange_bounds, one slice per completed
#     real user turn; multi-turn / whole-transcript slices are PROHIBITED).
#   * capture_slice    — slice record builder (created_at stamped at capture,
#     authoring_tier, provenance carriers, VERBATIM whole-capture rule); with
#     ledger_dir it durably appends records then writes the per-session cursor
#     (io.safe_write, at-least-once; record_id idempotency makes replay safe).
# ===========================================================================

class WholeTranscriptRefused(ValueError):
    """A multi-turn / multi-range slice was offered — plan §1.2 step 4 forbids
    whole-transcript passes; one slice per completed real user turn only."""


def slice_record_id(session_id: str, bounds: list, rtype: str, content: str) -> str:
    """Pinned derivation (plan §1.1): rec_ + sha256_hex(basis)[:16] where
    basis = f"{session_id}|{bounds_start}|{bounds_end}|{rtype}|{norm_content}".
    bounds_start/bounds_end = min/max message id across the record's
    provenance.source_exchange_bounds ranges (decimal strings).
    """
    starts = [int(rng[0]) for rng in bounds]
    ends = [int(rng[1]) for rng in bounds]
    basis = f"{session_id}|{min(starts)}|{max(ends)}|{rtype}|{io.norm_content(content)}"
    return "rec_" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:16]


def slice_capture_sha(session_id: str, slice_obj: dict) -> str:
    """Capture-run sha binding for slice records (provenance binding carrier)."""
    b = slice_obj["bounds"]
    starts = [int(rng[0]) for rng in b]
    ends = [int(rng[1]) for rng in b]
    basis = f"slicecapture|{session_id}|{min(starts)}|{max(ends)}|{slice_obj.get('slice_kind', '')}"
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def _completed_turn_spans(turns: list[dict], max_msg_id: int) -> list[tuple[int, int, int]]:
    """(turn_ordinal, turn_start, turn_end) for COMPLETED turns only —
    a turn is complete once the next real user turn exists (plan §1.2 step 3b);
    turn i spans [turns[i].msg_id, turns[i+1].msg_id - 1]."""
    spans = []
    for i in range(len(turns) - 1):
        start = int(turns[i]["msg_id"])
        end = int(turns[i + 1]["msg_id"]) - 1
        spans.append((i + 1, start, end))
    return spans


def _slice_items(message_list: list[dict]) -> tuple[list[dict], list[str]]:
    """Bounded per-slice item extraction: one item per non-empty line of the
    slice's message text; anchors = LITERAL_RE hits preserved verbatim
    (paper-extraction technique: bounded, anchored)."""
    from .schema import LITERAL_RE
    items: list[dict] = []
    anchors: list[str] = []
    seen: set[str] = set()
    for m in message_list:
        if m.get("csum"):
            continue  # boundary-marker rows carry no extractable items
        for line in (m.get("content") or "").splitlines():
            text = line.strip()
            if not text:
                continue
            lits = LITERAL_RE.findall(text)
            for lit in lits:
                if lit not in seen:
                    seen.add(lit)
                    anchors.append(lit)
            items.append({"content": text, "anchors": list(lits),
                          "message_id": int(m["id"])})
    return items, anchors


def extract_slices(session_id: str, *, db_path=None,
                   last_boundary_id: int = 0,
                   last_processed_max_msg_id: int = 0,
                   last_turn_ordinal: int = 0,
                   messages: list | None = None,
                   turns: list | None = None) -> list[dict]:
    """Frozen slice-boundary rule (plan §1.2), authored at the eviction boundary.

    1. Message state per partition.load_messages; regions per split_regions.
    2. Boundary events per partition.compaction_boundaries.
    3. Slice set selection (compaction-exit wins): (a) compacted-region
       messages with id > last_boundary_id; (b) only if (a) is empty —
       messages with id > last_processed_max_msg_id that have left the active
       tail. Both empty -> no-op (empty slice list; nothing ingested).
    4. One slice per completed real user turn FULLY CONTAINED in the selected
       message set; source_exchange_bounds = [[turn_start, turn_end]].
       Multi-turn slices are PROHIBITED (never a whole-transcript pass).
    """
    from . import partition, source
    if messages is None:
        if db_path is None:
            messages = partition.load_messages(session_id)
        else:
            messages = partition.load_messages(session_id, db_path)
    if turns is None:
        if db_path is None:
            turns = source.load_turns(session_id)
        else:
            turns = source.load_turns(session_id, db_path)

    compacted, tail = partition.split_regions(messages)
    tail_ids = {m["id"] for m in tail}
    selected_a = {m["id"] for m in compacted if m["id"] > last_boundary_id}
    selected_b = {m["id"] for m in messages
                  if m["id"] > last_processed_max_msg_id and m["id"] not in tail_ids}
    if selected_a:
        selected, slice_kind = selected_a, "compaction-exit"
    elif selected_b:
        selected, slice_kind = selected_b, "delta"
    else:
        return []

    max_msg_id = max((m["id"] for m in messages), default=0)
    by_id = {m["id"]: m for m in messages}
    boundary_ids = [b["boundary_id"] for b in partition.compaction_boundaries(messages)]
    boundary_id = max(boundary_ids) if boundary_ids else last_boundary_id

    out = []
    for ordinal, t_start, t_end in _completed_turn_spans(turns, max_msg_id):
        span_ids = {m["id"] for m in messages if t_start <= m["id"] <= t_end}
        if not span_ids or not span_ids.issubset(selected):
            continue
        message_list = [by_id[i] for i in sorted(span_ids) if i in by_id]
        items, anchors = _slice_items(message_list)
        if not items:
            continue
        out.append({
            "session_id": session_id,
            "turn_id": ordinal,
            "bounds": [[t_start, t_end]],
            "slice_kind": slice_kind,
            "boundary_id": boundary_id,
            "message_ids": sorted(span_ids),
            "anchors": anchors,
            "items": items,
        })
    return out


def capture_slice(session_id: str, slice_obj: dict, *, turns: list | None = None,
                  now: float | None = None, floor_theta: float = 1.0,
                  classify_fn=None, t2_state=None,
                  ledger_dir: str | None = None) -> list[dict]:
    """Slice record builder (plan §1.2 steps 5-7) — created_at stamped at
    capture (R16), record_id per slice_record_id, provenance carriers
    (anchors + source_exchange_bounds carried per slice), VERBATIM
    whole-capture rule (T1; never queued for T2). Non-carriers go through the
    mechanical classifier; below floor_theta they are queued for the T2 pass
    (per-slice, never batch; measured tokens against the per-boundary budget
    held by t2_state). With ledger_dir, records are durably appended to
    <ledger_dir>/<session_id>.jsonl and the per-session cursor is written
    AFTERWARDS via io.safe_write (at-least-once).
    """
    import json
    import time as _time
    bounds = [list(r) for r in slice_obj["bounds"]]

    # Whole-transcript refusal (plan §1.2 step 4): one completed turn per slice.
    if len(bounds) != 1:
        raise WholeTranscriptRefused(
            f"multi-range bounds are whole-transcript passes: {bounds!r}")
    if turns is not None:
        spans = {(s, e) for _, s, e in _completed_turn_spans(turns, 0)}
        if (bounds[0][0], bounds[0][1]) not in spans:
            raise WholeTranscriptRefused(
                f"bounds {bounds!r} are not exactly one completed real user turn")

    if classify_fn is None:
        from .t2_classifier import classify_mechanical as classify_fn

    stamp = _time.time() if now is None else float(now)
    capture_sha = slice_capture_sha(session_id, slice_obj)
    queued_idx: list[int] = []
    built: list[dict] = []

    for idx, item in enumerate(slice_obj["items"]):
        content = item["content"]
        rtype, conf, tier, carrier_kind = classify_fn(content, bounds)
        queued = tier == "T2" and conf < floor_theta
        if queued:
            queued_idx.append(idx)
        built.append({
            "record_id": slice_record_id(session_id, bounds, rtype, content),
            "session_id": session_id,
            "turn_id": slice_obj["turn_id"],
            "type": rtype,
            "content": content,
            "bounds": bounds,
            "provenance": {
                "source_exchange_bounds": bounds,
                "artifact_sha256_12": None,
                "anchors": list(item.get("anchors") or []),
                "source_message_id": item.get("message_id"),
            },
            "scope": "global" if rtype in ("rule", "lesson", "decision") else "task",
            "confidence": float(conf),
            "mutable": rtype != "artifact",
            "superseded_by": None,
            "created_at": stamp,
            "authoring_tier": tier,
            "topic_hash": topic_hash(rtype, content),
            "capture_sha": capture_sha,
            "supersedes": [],
            "carrier_kind": carrier_kind,
        })

    # T2 pass: per-SLICE, never batch; measured tokens vs the per-boundary cap.
    t2_note = None
    if queued_idx and t2_state is not None and getattr(t2_state, "llm_fn", None):
        from .t2_classifier import T2BudgetExhausted
        contents = [built[i]["content"] for i in queued_idx]
        try:
            labels, usage = t2_state.run_pass(contents)
            for i, label in zip(queued_idx, labels):
                rec = built[i]
                rec["type"] = label
                rec["record_id"] = slice_record_id(session_id, bounds, label, rec["content"])
                rec["topic_hash"] = topic_hash(label, rec["content"])
                rec["confidence"] = 1.0
                rec["t2_llm_used"] = True
            t2_note = {"ok": True, **usage}
        except T2BudgetExhausted as exc:
            for i in queued_idx:
                built[i]["t2_llm_used"] = False
                built[i]["t2_truncated"] = True
            t2_note = {"ok": False, "reason": f"budget: {exc}"}
        except Exception as exc:  # loud, journaled; mechanical type stands
            for i in queued_idx:
                built[i]["t2_llm_used"] = False
                built[i]["t2_error"] = str(exc)[:200]
            t2_note = {"ok": False, "reason": f"error: {exc}"}
    elif queued_idx:
        for i in queued_idx:
            built[i]["t2_llm_used"] = False

    for rec in built:
        rec.setdefault("t2_llm_used", False)
    if t2_state is not None:
        t2_state.observe_items([
            {"queued_for_t2": i in queued_idx} for i in range(len(built))
        ])

    if ledger_dir is not None and built:
        io.allow(ledger_dir)
        ledger = io.Path(ledger_dir) / f"{session_id}.jsonl"
        for rec in built:
            io.append_jsonl(ledger, rec)
        cursor_path = io.Path(ledger_dir) / f".slice_cursor_{session_id}.json"
        cursor = {
            "last_boundary_id": max(int(slice_obj.get("boundary_id") or 0),
                                    0),
            "last_processed_max_msg_id": int(slice_obj["bounds"][0][1]),
            "last_turn_ordinal": int(slice_obj["turn_id"]),
            "ts": stamp,
        }
        io.safe_write(cursor_path, json.dumps(cursor, sort_keys=True) + "\n")
    return built
