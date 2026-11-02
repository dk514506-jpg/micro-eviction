"""mev.injection — Stage 2: scope-routed SANDBOX assembly only.

Assembles (a) narrative spine, (b) task-scoped rules matching the session's
campaign, (c) active calcified global rules — each with record_id provenance.
Writes ONLY under injection-sandbox/. Budget: <=2000 chars/resume payload;
over-budget => status FAIL (report, not retune).
"""
from __future__ import annotations
import json
from pathlib import Path
from . import io
from . import directives as dirmod
from .lifecycle import active_calcified

BUDGET_CHARS = 2000  # PRE-REGISTERED (charge): 2,000 chars/session resume

def assemble(session_id: str, campaign: str, ledger_dir: str, calcified_path: str,
             spine_heads: int = 1, spine_tails: int = 2,
             directives_path: str | None = None) -> dict:
    """Build the sandbox payload. Returns payload dict with status PASS/FAIL.

    D7: retraction directives are consumed here — a directive-hit rule is
    excluded from every future injection (and from the downstream view).
    """
    dpath = (Path(directives_path) if directives_path
             else dirmod.derive_directives_path(calcified_path))
    dirs = dirmod.load_directives(dpath)

    session_records = []
    for p in sorted(Path(ledger_dir).glob("*.jsonl")):
        if p.name.endswith(".spine.jsonl"):
            continue
        if "lifecycle" in p.name or "conflict" in p.name or "directive" in p.name:
            continue
        for row in io.load_jsonl(p):
            # record-shaped rows only (E: KeyError 'scope' on grants/other jsonl)
            if "record_id" in row and "scope" in row and "type" in row and "content" in row:
                session_records.append(row)

    # (a) narrative spine
    spine_path = Path(ledger_dir) / f"{session_id}.spine.jsonl"
    spine = io.load_jsonl(spine_path)
    picked = (spine[:spine_heads] + spine[-spine_tails:]) if len(spine) > spine_heads + spine_tails else spine
    seen_turns: set[int] = set()
    lines: list[str] = []
    for s in picked:
        if s["turn_id"] in seen_turns:
            continue
        seen_turns.add(s["turn_id"])
        lines.append(f"[spine T{s['turn_id']}] {s['head']}")

    # (b) task-scoped rules matching the session's campaign
    # frozen composition: session_id == resuming session OR campaign slug in content
    # D7: directive-hit records excluded
    excluded_ids: set[str] = set()
    def _not_retracted(rule: dict) -> bool:
        d = dirmod.hit(dirs, topic_hash=rule.get("topic_hash"),
                       record_ids=[rule["record_id"]] if rule.get("record_id") else list(rule.get("members") or []),
                       content=rule.get("rule_content", rule.get("content")))
        if d is not None:
            excluded_ids.add(d["directive_id"])
            return False
        return True
    task = [r for r in session_records
            if r["scope"] == "task" and r["type"] in ("rule", "decision")
            and (r["session_id"] == session_id or campaign.lower() in r["content"].lower())
            and _not_retracted(r)]
    # budget-greedy: newest first (stable by record_id)
    for r in sorted(task, key=lambda x: x["record_id"], reverse=True):
        lines.append(f"[task {r['record_id']}] {r['content']}")

    # (c) active calcified global rules — tagged with record_id provenance
    for c in io.load_jsonl(calcified_path):
        d_hit = dirmod.hit(dirs, topic_hash=c.get("topic_hash"),
                           record_ids=list(c.get("members") or []),
                           content=c.get("rule_content", c.get("content")))
        if d_hit is not None:
            excluded_ids.add(d_hit["directive_id"])
    active = active_calcified(calcified_path, directives=dirs)
    calc_lines = [c for c in active if c.get("scope") == "global"]
    for c in calc_lines:
        rid = c["members"][0] if c.get("members") else c["topic_hash"]
        lines.append(f"[calcified {rid}] {c['rule_content']}")

    payload = "\n".join(lines)
    status = "PASS" if len(payload) <= BUDGET_CHARS else "FAIL-budget"
    return {"session_id": session_id, "campaign": campaign, "status": status,
            "chars": len(payload), "budget": BUDGET_CHARS,
            "lines": len(lines), "payload": payload,
            "excluded_by_directive": sorted(excluded_ids),
            "directives_consulted": sorted(d["directive_id"] for d in dirs),
            "record_ids": [r["record_id"] for r in task] +
                          [(c["members"][0] if c.get("members") else c["topic_hash"])
                           for c in calc_lines]}

def run_sandbox(session_id: str, campaign: str, ledger_dir: str, calcified_path: str,
                sandbox_dir: str, log_path: str) -> dict:
    """Assemble + write ONLY to sandbox dir + append log line. Never raises on
    budget fail — status is recorded (report, not retune)."""
    res = assemble(session_id, campaign, ledger_dir, calcified_path)
    sandbox = Path(sandbox_dir)
    io.safe_write(sandbox / "resume_view.md",
                  res["payload"] + f"\n\n<!-- status={res['status']} chars={res['chars']} -->\n")
    io.append_jsonl(log_path, {k: res[k] for k in
                               ("session_id", "campaign", "status", "chars", "budget", "lines",
                                "record_ids")})
    return res
