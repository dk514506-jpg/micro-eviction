"""mev.lifecycle — Stage 1: promotion gates, calcification, retraction/falsification.

Promotion is a LEDGER STATE TRANSITION: transitions append to
ledgers/lifecycle-transitions.jsonl; calcified rules append to calcified.jsonl.
Nothing here ever writes to any live memory/skill store.

R1/W5 (A-F1): rule promotion additionally requires the mechanical logical-core
gate (topic_identity.mechanical_promotion_gate). Calcified content is
consensus-derived with per-member quoting (topic_identity.consensus_content).
"""
from __future__ import annotations
import hashlib
from pathlib import Path
from . import io
from .capture import topic_hash
from .topic_identity import mechanical_promotion_gate, consensus_content
from . import directives as dirmod

# promotion criteria constants (PRE-REGISTERED in charge; frozen here)
PATTERN_MIN_PER_LEDGER = 2      # >=2 events of matching type+topic-hash within one session ledger
RULE_MIN_LEDGERS = 2            # pattern corroborated across >=2 session ledgers
RULE_MIN_PER_LEDGER = 2         # strict reading (frozen in RUN-LOG): >=2 members in each ledger

def group_records(records: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for r in records:
        th = topic_hash(r["type"], r["content"])
        groups.setdefault(th, []).append(r)
    return groups

def load_all_records(ledger_dir: str) -> list[dict]:
    out: list[dict] = []
    for p in sorted(Path(ledger_dir).glob("*.jsonl")):
        if p.name in ("lifecycle-transitions.jsonl", "conflict-queue.jsonl"):
            continue
        if p.name.endswith(".spine.jsonl") or p.name.endswith("transitions.jsonl"):
            continue
        for row in io.load_jsonl(p):
            # only record-shaped rows (ledgers may sit next to other jsonl artifacts)
            if "content" in row and "record_id" in row:
                out.append(row)
    return out

def evaluate_promotions(records: list[dict]) -> dict:
    """Re-derive promotion states from records alone (pure function).

    Returns {group_topic_hash: {state, type, members, sessions, counts}}.
    States: event -> pattern -> rule (calcification is a separate gate).
    R1/W5: rule promotion requires the mechanical logical-core gate; a cluster
    that shares only a keyword (not a logical core + substantive phrase) is
    capped at pattern regardless of count-based criteria.
    """
    groups = group_records(records)
    out: dict[str, dict] = {}
    for th, members in groups.items():
        by_session: dict[str, list[dict]] = {}
        for m in members:
            by_session.setdefault(m["session_id"], []).append(m)
        pattern_sessions = [s for s, ms in by_session.items() if len(ms) >= PATTERN_MIN_PER_LEDGER]
        state = "event"
        if pattern_sessions:
            state = "pattern"
            strict = [s for s in pattern_sessions if len(by_session[s]) >= RULE_MIN_PER_LEDGER]
            if len(strict) >= RULE_MIN_LEDGERS:
                state = "rule"
        # W5 mechanical gate: rule requires shared logical core + substantive phrase
        if state == "rule" and not mechanical_promotion_gate(members):
            state = "pattern"
        out[th] = {
            "state": state,
            "type": members[0]["type"],
            "members": [m["record_id"] for m in members],
            "sessions": sorted(by_session),
            "counts": {s: len(ms) for s, ms in by_session.items()},
        }
    return out

def calcification_eligible(th: str, eval_result: dict, records: list[dict],
                           grants: list[dict]) -> tuple[bool, str, dict | None]:
    """A rule group calcifies iff state=='rule' AND (a) a member carries sha-bound
    artifact provenance, or (b) an explicit grant exists for this topic_hash."""
    g = eval_result.get(th)
    if not g or g["state"] != "rule":
        return False, "not-a-rule", None
    members = [r for r in records if r["record_id"] in g["members"]]
    for m in members:
        sha = m["provenance"].get("artifact_sha256_12")
        if sha:
            return True, "artifact-sha", m
    for gnt in grants:
        if gnt.get("topic_hash") == th:
            return True, "grant", gnt
    return False, "no-sha-no-grant", None

def append_promotions(transitions_path: str, records: list[dict], grants: list[dict]) -> dict:
    """Compute promotions + calcification and append transitions (idempotent: skip
    transitions already present). Returns a summary of what was appended."""
    evalr = evaluate_promotions(records)
    existing = io.load_jsonl(transitions_path)
    have = {(t.get("topic_hash"), t.get("to")) for t in existing if t.get("kind") == "promotion"}
    appended: list[dict] = []
    for th, g in sorted(evalr.items()):
        # pattern + rule transitions
        for to_state in ("pattern", "rule"):
            if g["state"] == to_state or (to_state == "pattern" and g["state"] == "rule"):
                if (th, to_state) not in have:
                    row = {"kind": "promotion", "topic_hash": th, "type": g["type"],
                           "to": to_state, "members": g["members"], "sessions": g["sessions"],
                           "counts": g["counts"]}
                    io.append_jsonl(transitions_path, row)
                    appended.append(row); have.add((th, to_state))
        # calcification transition
        ok, via, evidence = calcification_eligible(th, evalr, records, grants)
        if ok and (th, "calcified") not in have:
            row = {"kind": "promotion", "topic_hash": th, "type": g["type"],
                   "to": "calcified", "via": via,
                   "members": g["members"], "sessions": g["sessions"],
                   "counts": g["counts"],
                   "evidence": (evidence.get("record_id") if isinstance(evidence, dict) and "record_id" in evidence
                                else (evidence.get("grant_id") if isinstance(evidence, dict) else None))}
            io.append_jsonl(transitions_path, row)
            appended.append(row); have.add((th, "calcified"))
    return {"appended": len(appended), "rows": appended, "groups": len(evalr)}

def write_calcified_store(calcified_path: str, records: list[dict],
                          transitions_path: str) -> list[dict]:
    """Materialize calcified rules from calcified transitions into calcified.jsonl
    (append-only; skips topic_hashes already present).
    R1/W5: rule_content is consensus-derived with per-member quoting
    (record_id-tagged), never one member's text alone."""
    have = {c["topic_hash"] for c in io.load_jsonl(calcified_path)}
    out: list[dict] = []
    for t in io.load_jsonl(transitions_path):
        if t.get("kind") != "promotion" or t.get("to") != "calcified":
            continue
        th = t["topic_hash"]
        if th in have:
            continue
        members = [r for r in records if r["record_id"] in t["members"]]
        # W5: consensus-derived content (shared core + per-member quoting)
        cons = consensus_content(members)
        rep = max(members, key=lambda r: len(r["content"]))
        entry = {
            "topic_hash": th, "type": t["type"], "via": t.get("via"),
            "rule_content": cons["rule_content"],
            "consensus_core": cons["consensus_core"],
            "member_quotes": cons["member_quotes"],
            "scope": rep["scope"],
            "members": t["members"], "sessions": t["sessions"],
            "provenance": rep["provenance"], "superseded_by": None,
            "falsified": False,
        }
        io.append_jsonl(calcified_path, entry)
        have.add(th); out.append(entry)
    return out

def active_calcified(calcified_path: str, directives: list[dict] | None = None,
                     directives_path: str | None = None) -> list[dict]:
    """Downstream view: calcified rules not superseded, not falsified, not
    queued, and not directive-hit (D7: a retracted rule is excluded from every
    downstream view and every future injection)."""
    queued: set = set()
    for c in io.load_jsonl(Path(calcified_path).parent / "ledgers" / "conflict-queue.jsonl"):
        queued.update({c.get("a"), c.get("b"), c.get("topic_hash")})
    if directives is None:
        dpath = (Path(directives_path) if directives_path
                 else dirmod.derive_directives_path(calcified_path))
        directives = dirmod.load_directives(dpath)
    return [c for c in io.load_jsonl(calcified_path)
            if not c.get("superseded_by") and not c.get("falsified")
            and c["topic_hash"] not in queued
            and not dirmod.is_retracted(directives, c)]

# --- retraction -------------------------------------------------------------
def _emit_retraction_directive(calcified_path: str, d_type: str, row: dict,
                               reason: str, source: str,
                               replacement_topic_hash: str | None = None,
                               directives_path: str | None = None) -> dict:
    """D7: supersede/falsify emits a typed, sha-linked retraction directive."""
    dpath = (Path(directives_path) if directives_path
             else dirmod.derive_directives_path(calcified_path))
    d = dirmod.build_directive(
        directive_type=d_type,
        target_topic_hash=row["topic_hash"],
        rule_content=row.get("rule_content", row.get("content", "")),
        target_record_ids=list(row.get("members") or
                               ([row["record_id"]] if row.get("record_id") else [])),
        reason=reason, source=source,
        replacement_topic_hash=replacement_topic_hash)
    return dirmod.emit_directive(dpath, d)

def supersede(calcified_path: str, old_th: str, new_th: str, reason: str,
              directives_path: str | None = None) -> dict:
    """Supersede an old calcified rule with a later conflicting one, and emit a
    retraction directive (D7). No hit => nothing written (untouched)."""
    rows = io.load_jsonl(calcified_path)
    hit = False
    hit_row = None
    for r in rows:
        if r["topic_hash"] == old_th and not r.get("superseded_by"):
            r["superseded_by"] = new_th
            r["supersede_reason"] = reason
            hit = True
            hit_row = r
    directive = None
    if hit:
        io.safe_write(calcified_path, "".join(
            __import__("json").dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in rows))
        directive = _emit_retraction_directive(
            calcified_path, "supersede", hit_row, reason,
            "lifecycle.supersede", replacement_topic_hash=new_th,
            directives_path=directives_path)
    return {"superseded": hit, "old": old_th, "new": new_th, "reason": reason,
            "directive": directive}

def falsify(calcified_path: str, th: str, evidence: str,
            directives_path: str | None = None) -> dict:
    """Falsify a calcified rule and emit a retraction directive (D7).
    No hit => nothing written (untouched)."""
    rows = io.load_jsonl(calcified_path)
    hit = False
    hit_row = None
    for r in rows:
        if r["topic_hash"] == th and not r.get("falsified"):
            r["falsified"] = True
            r["falsification_evidence"] = evidence
            hit = True
            hit_row = r
    directive = None
    if hit:
        io.safe_write(calcified_path, "".join(
            __import__("json").dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in rows))
        directive = _emit_retraction_directive(
            calcified_path, "falsify", hit_row, evidence, "lifecycle.falsify",
            directives_path=directives_path)
    return {"falsified": hit, "topic_hash": th, "evidence": evidence,
            "directive": directive}

def queue_conflict(queue_path: str, th_a: str, th_b: str, note: str) -> dict:
    """Conflicts queue for review rather than auto-resolving (Engraphis precedent)."""
    existing = {(c.get("a"), c.get("b")) for c in io.load_jsonl(queue_path)}
    if (th_a, th_b) in existing or (th_b, th_a) in existing:
        return {"queued": False, "reason": "already-queued"}
    row = {"a": th_a, "b": th_b, "note": note, "status": "pending-review"}
    io.append_jsonl(queue_path, row)
    return {"queued": True, **row}
