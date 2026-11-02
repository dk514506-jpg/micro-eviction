"""mev.directives — D7 retraction directives (typed records, sha-linked).

Dallas ruling 2026-10-28: supersede/falsify emits a retraction directive that
the assemble/compaction path consumes. A directive-hit rule is excluded from
every downstream view and every future injection. Already-injected context is
immutable by nature — retraction reaches forward only (next injection excludes;
affected sessions carry a retracted-after-injection marker).

Directive stream (append-only): <calcified parent>/ledgers/retraction-directives.jsonl
Each record is typed (kind=retraction-directive, type=supersede|falsify) and
sha-linked to the rule it retracts (target_topic_hash + target_content_sha256_12).
"""
from __future__ import annotations
import hashlib, re
from datetime import datetime, timezone
from pathlib import Path
from . import io

DIRECTIVE_KIND = "retraction-directive"
DIRECTIVE_TYPES = frozenset({"supersede", "falsify"})
REQUIRED_FIELDS = ("kind", "directive_id", "type", "target_topic_hash",
                   "target_content_sha256_12", "target_record_ids", "reason",
                   "source", "issued_at")
DIRECTIVE_ID_RE = re.compile(r"^rd_[0-9a-f]{12}$")
HEX12_RE = re.compile(r"^[0-9a-f]{12}$")
MARKER_KIND = "retraction-marker"
MARKER_NAME = "retracted-after-injection"


def derive_directives_path(calcified_path: str | Path) -> Path:
    """Single derivation rule shared by emit + consume (beside the rule store)."""
    return Path(calcified_path).parent / "ledgers" / "retraction-directives.jsonl"


def content_sha12(text: str) -> str:
    return io.content_sha(text)[:12]


def compute_directive_id(directive_type: str, target_topic_hash: str,
                         target_content_sha256_12: str) -> str:
    blob = f"{directive_type}|{target_topic_hash}|{target_content_sha256_12}"
    return "rd_" + hashlib.sha256(blob.encode("utf-8")).hexdigest()[:12]


def build_directive(directive_type: str, target_topic_hash: str, rule_content: str,
                    target_record_ids: list, reason: str, source: str,
                    *, replacement_topic_hash: str | None = None) -> dict:
    """Typed, sha-linked directive record (the sha binds to the rule text)."""
    sha12 = content_sha12(rule_content)
    return {
        "kind": DIRECTIVE_KIND,
        "directive_id": compute_directive_id(directive_type, target_topic_hash, sha12),
        "type": directive_type,
        "target_topic_hash": target_topic_hash,
        "target_content_sha256_12": sha12,
        "target_record_ids": list(target_record_ids),
        "reason": reason,
        "source": source,
        "issued_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "replacement_topic_hash": replacement_topic_hash,
    }


def validate_directive(d: dict) -> list[str]:
    """Typed-record schema gate; empty list == accepted."""
    errs: list[str] = []
    if not isinstance(d, dict):
        return ["directive is not a dict"]
    for f in REQUIRED_FIELDS:
        if f not in d:
            errs.append(f"missing field: {f}")
    if errs:
        return errs
    if d["kind"] != DIRECTIVE_KIND:
        errs.append(f"invalid kind: {d['kind']!r}")
    if d["type"] not in DIRECTIVE_TYPES:
        errs.append(f"invalid type: {d['type']!r}")
    if not isinstance(d["directive_id"], str) or not DIRECTIVE_ID_RE.match(d["directive_id"]):
        errs.append(f"invalid directive_id: {d['directive_id']!r}")
    if not isinstance(d["target_topic_hash"], str) or not d["target_topic_hash"].strip():
        errs.append("empty target_topic_hash")
    sha = d["target_content_sha256_12"]
    if not isinstance(sha, str) or not HEX12_RE.match(sha):
        errs.append(f"invalid target_content_sha256_12: {sha!r}")
    ids = d["target_record_ids"]
    if not isinstance(ids, list) or any(not isinstance(x, str) for x in ids):
        errs.append(f"invalid target_record_ids: {ids!r}")
    if not isinstance(d["reason"], str) or not d["reason"].strip():
        errs.append("empty reason")
    if not isinstance(d["source"], str) or not d["source"].strip():
        errs.append("empty source")
    if not isinstance(d["issued_at"], str) or not d["issued_at"].strip():
        errs.append("empty issued_at")
    if d["type"] == "supersede" and not (d.get("replacement_topic_hash") or "").strip():
        errs.append("supersede directive missing replacement_topic_hash")
    # sha-link integrity: the id must re-derive from (type, topic, content sha)
    if not errs and d["directive_id"] != compute_directive_id(
            d["type"], d["target_topic_hash"], d["target_content_sha256_12"]):
        errs.append("directive_id does not re-derive from sha-link fields")
    return errs


def emit_directive(directives_path: str | Path, directive: dict) -> dict:
    """Append one validated directive (idempotent on directive_id)."""
    errs = validate_directive(directive)
    if errs:
        return {"emitted": False, "duplicate": False, "errors": errs,
                "directive_id": directive.get("directive_id"),
                "path": str(directives_path)}
    existing = load_directives(directives_path)
    if any(x.get("directive_id") == directive["directive_id"] for x in existing):
        return {"emitted": False, "duplicate": True, "errors": [],
                "directive_id": directive["directive_id"],
                "path": str(directives_path)}
    io.append_jsonl(directives_path, directive)
    return {"emitted": True, "duplicate": False, "errors": [],
            "directive_id": directive["directive_id"],
            "path": str(directives_path)}


def load_directives(directives_path: str | Path) -> list[dict]:
    return [d for d in io.load_jsonl(directives_path)
            if d.get("kind") == DIRECTIVE_KIND]


def hit(directives: list[dict], *, topic_hash: str | None = None,
        record_ids: list | None = None, content: str | None = None) -> dict | None:
    """Return the first directive that hits this rule, else None."""
    csha = content_sha12(content) if content else None
    for d in directives:
        if topic_hash and d.get("target_topic_hash") == topic_hash:
            return d
        ids = set(d.get("target_record_ids") or [])
        if record_ids and ids.intersection(record_ids):
            return d
        if csha and d.get("target_content_sha256_12") == csha:
            return d
    return None


def is_retracted(directives: list[dict], rule: dict) -> bool:
    """A rule (calcified entry or record row) is directive-hit if any directive
    sha-links to it: topic_hash match, member record_id match, or content sha."""
    if not directives:
        return False
    rid = rule.get("record_id")
    members = rule.get("members")
    record_ids = ([rid] if rid else []) + list(members or [])
    content = rule.get("rule_content", rule.get("content"))
    return hit(directives, topic_hash=rule.get("topic_hash"),
               record_ids=record_ids, content=content) is not None


def mark_retracted_after_injection(injection_log_path: str | Path,
                                   directives: list[dict]) -> dict:
    """Forward-only marker for content already inside a session's context:
    affected sessions carry a retracted-after-injection marker in the injection
    log. Idempotent on (session_id, directive_id).

    The machine marker NEVER guesses: a log row without record_ids (legacy,
    pre-W4 shape) is reported needs_manual and left unmarked — attribution is a
    human step (see RETRACTION-MANUAL-PROCEDURE-*.md)."""
    rows = io.load_jsonl(injection_log_path)
    have = {(r.get("session_id"), r.get("directive_id"))
            for r in rows if r.get("kind") == MARKER_KIND}
    appended: list[dict] = []
    needs_manual: list[dict] = []
    for row in rows:
        if row.get("kind") == MARKER_KIND:
            continue
        sid = row.get("session_id")
        rec_ids = list(row.get("record_ids") or [])
        for d in directives:
            if not rec_ids:
                needs_manual.append({"session_id": sid,
                                     "reason": "row without record_ids (manual procedure)",
                                     "directive_id": d.get("directive_id")})
                continue
            overlap = sorted(set(d.get("target_record_ids") or []).intersection(rec_ids))
            if not overlap:
                continue
            key = (sid, d.get("directive_id"))
            if key in have:
                continue
            marker = {"kind": MARKER_KIND, "marker": MARKER_NAME,
                      "session_id": sid, "directive_id": d["directive_id"],
                      "type": d["type"], "target_topic_hash": d["target_topic_hash"],
                      "record_ids": overlap,
                      "marked_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            io.append_jsonl(injection_log_path, marker)
            appended.append(marker)
            have.add(key)
    return {"marked": appended, "needs_manual": needs_manual,
            "path": str(injection_log_path)}
