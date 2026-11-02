"""mev.canary — no-injection-outside-sandbox canary (hard gate across all stages).

Snapshots a fixed set of real memory/context paths, then proves they are
byte-identical after a stage run. Any diff = gate FAIL.
"""
from __future__ import annotations
import hashlib, json, os
from pathlib import Path

REAL_PATHS: list[str] = [
    str(Path.home() / ".hermes" / "memories"),
    str(Path.home() / ".hermes" / "config.yaml"),
    str(Path.home() / ".local" / "share" / "mimocode" / "memory"),
    str(Path.home() / ".claude" / "skills"),
    str(Path.home() / ".agents" / "skills"),
    str(Path.home() / ".config" / "mimocode" / "skills"),
]

def _digest_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()[:16]

def snapshot(paths: list[str] | None = None) -> dict:
    """Map of path -> content digest (files) or tree digest (dirs); 'missing' if absent."""
    out: dict[str, str] = {}
    for root in paths or REAL_PATHS:
        p = Path(root)
        if not p.exists():
            out[root] = "missing"
        elif p.is_file():
            out[root] = "file:" + _digest_file(p)
        else:
            entries = []
            for f in sorted(p.rglob("*")):
                if f.is_file():
                    entries.append(f"{f.relative_to(p)}:{_digest_file(f)}")
            out[root] = "tree:" + hashlib.sha256("|".join(entries).encode()).hexdigest()[:16]
    return out

def compare(before: dict, after: dict) -> dict:
    diffs = {k: {"before": before.get(k), "after": after.get(k)}
             for k in set(before) | set(after) if before.get(k) != after.get(k)}
    return {"clean": not diffs, "diffs": diffs}

# Paths the injection path must never touch (memory/skill stores). Diffs here are
# always violations. config.yaml is also snapshotted but may churn from the live
# hermes gateway; those diffs are attributed as external, never as builder writes.
INJECTION_SENSITIVE = {p for p in REAL_PATHS if "config.yaml" not in p}

def gate(before: dict, after: dict, report_path: str) -> bool:
    """Write canary report + return gate verdict.

    gate_pass = (no snapshot diff on an injection-sensitive path) AND
                (no builder write, per WRITE_AUDIT, landing under any real path)
                AND (no diff on any real path attributed to our write audit).
    A diff on ~/.hermes/config.yaml with no matching audited write is external
    churn (the live gateway) and does not fail the gate; it is still recorded.
    The primary hard guard is io.safe_write (refuses outside allowed roots);
    this snapshot is the belt-and-suspenders evidence.
    """
    from . import io
    res = compare(before, after)
    res["paths"] = sorted(set(before) | set(after))
    audited = sorted(set(io.WRITE_AUDIT))
    res["builder_writes"] = audited
    violations = {}
    # any diff on an injection-sensitive real path = violation (regardless of attribution)
    for k, v in res["diffs"].items():
        if k in INJECTION_SENSITIVE or any(k.rstrip("/") == s.rstrip("/") for s in INJECTION_SENSITIVE):
            violations[k] = v
    # any snapshot diff whose path is in our write audit = violation
    for k, v in res["diffs"].items():
        if any(k == w or w.startswith(k.rstrip("/") + "/") for w in audited):
            violations.setdefault(k, v)
    # also: any builder write that landed under a real path is a violation outright
    for w in audited:
        if any(w == r or w.startswith(r.rstrip("/") + "/") for r in (before.keys())):
            violations.setdefault(w, {"before": "real-path", "after": "builder-wrote"})
    res["violations"] = violations
    res["clean"] = not res["diffs"]
    res["external_churn"] = {k: v for k, v in res["diffs"].items() if k not in violations}
    res["gate_pass"] = not violations
    io.safe_write(report_path, json.dumps(res, indent=2, sort_keys=True, default=str) + "\n")
    return bool(res["gate_pass"])
