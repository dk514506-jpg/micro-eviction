"""mev.stores_walker — D7(c): apply retraction corrections to store files.

The walker actively corrects store copies of retracted rules. SANDBOX STORES
ONLY in this charge: any root under a live memory/skill store path is refused
(hard scope guard). All writes go through io.safe_write.

Store shapes handled:
  *.jsonl — calcified-shape rule rows (topic_hash + rule_content/members):
            set falsified / superseded_by + retraction_directive_id (idempotent).
  *.md    — knowledge-store lines carrying a target token (topic_hash or
            record_id): insert an annotation comment above each match
            (idempotent per directive_id).

Evidence ledgers (append-only session ledgers) are never rewritten here —
their retraction is enforced at consumption time (assemble / active views).
"""
from __future__ import annotations
import json, re
from pathlib import Path
from . import io
from .canary import REAL_PATHS
from .directives import hit

ANNOTATION_PREFIX = "<!-- mev-retraction:"


class StoreScopeError(PermissionError):
    """Refused: stores root is a live store path (or inside one)."""


def _resolved(p: str | Path) -> Path:
    return Path(p).resolve()


def assert_sandbox_scope(stores_root: str | Path) -> Path:
    """Hard scope guard: refuse any overlap with a live store path — the root
    must neither be (or live under) a live store, nor CONTAIN one (an ancestor
    root would rglob straight into live store trees)."""
    root = _resolved(stores_root)
    for live in REAL_PATHS:
        live_p = _resolved(live)
        if root == live_p or root.is_relative_to(live_p) or live_p.is_relative_to(root):
            raise StoreScopeError(
                f"stores root {root} overlaps live store {live_p} — "
                f"sandbox stores only in this charge")
    return root


def _correct_jsonl_row(row: dict, directives: list[dict]) -> bool:
    """Apply every matching directive's correction to a calcified-shape row.
    Idempotent per directive_id; True if the row changed."""
    applied = list(row.get("retraction_directive_ids") or
                   ([row["retraction_directive_id"]] if row.get("retraction_directive_id") else []))
    changed = False
    for d in directives:
        if d["directive_id"] in applied:
            continue
        if d["type"] == "falsify":
            row["falsified"] = True
        else:
            row["superseded_by"] = d.get("replacement_topic_hash")
            row["supersede_reason"] = d.get("reason")
        applied.append(d["directive_id"])
        changed = True
    if changed:
        row["retraction_directive_ids"] = applied
        row["retraction_directive_id"] = applied[0]
    return changed


def _row_matches(row: dict, d: dict) -> bool:
    return hit([d], topic_hash=row.get("topic_hash"),
               record_ids=[row["record_id"]] if row.get("record_id") else list(row.get("members") or []),
               content=row.get("rule_content", row.get("content"))) is not None


def _token_in(token: str, line: str) -> bool:
    """Whole-token match (record ids must not match as prefixes of longer ids)."""
    return re.search(r"(?<![0-9A-Za-z_])" + re.escape(token) + r"(?![0-9A-Za-z_])",
                     line) is not None


def _annotation_line(d: dict) -> str:
    return (f"{ANNOTATION_PREFIX} {d['directive_id']} type={d['type']} "
            f"topic={d['target_topic_hash']} -->\n")


def apply_corrections(stores_root: str | Path, directives: list[dict]) -> dict:
    """Walk store files under stores_root and apply directive corrections.

    Returns a report dict (files_scanned, rows_corrected, lines_annotated,
    files_unchanged, skipped). Idempotent: a second run changes nothing.
    """
    root = assert_sandbox_scope(stores_root)
    report = {"stores_root": str(root), "files_scanned": 0, "rows_corrected": 0,
              "lines_annotated": 0, "files_unchanged": 0, "skipped": [],
              "corrected_files": []}
    if not root.exists():
        return report
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        report["files_scanned"] += 1
        if path.suffix == ".jsonl":
            changed = False
            out_rows: list[dict] = []
            for row in io.load_jsonl(path):
                matched = [d for d in directives if _row_matches(row, d)]
                if matched and _correct_jsonl_row(row, matched):
                    report["rows_corrected"] += 1
                    changed = True
                out_rows.append(row)
            if changed:
                io.safe_write(path, "".join(
                    json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n"
                    for r in out_rows))
                report["corrected_files"].append(str(path))
            else:
                report["files_unchanged"] += 1
        elif path.suffix == ".md":
            text = path.read_text(encoding="utf-8")
            lines = text.splitlines(keepends=True)
            changed = False
            out_lines: list[str] = []
            # Consecutive INPUT annotation lines accumulate (union) — stacked
            # annotations from prior runs suppress re-annotation of the content
            # line below. Annotations emitted in THIS run do not block a later
            # distinct content line (the set is cleared at each content line).
            prev_input_annotation: set[str] = set()
            for line in lines:
                if line.startswith(ANNOTATION_PREFIX):
                    m = re.match(r"<!-- mev-retraction: (rd_[0-9a-f]{12})", line)
                    if m:
                        prev_input_annotation.add(m.group(1))
                    out_lines.append(line)
                    continue
                for d in directives:
                    if d["directive_id"] in prev_input_annotation:
                        continue
                    tokens = [t for t in
                              [d.get("target_topic_hash"), *(d.get("target_record_ids") or [])]
                              if t]
                    if tokens and any(_token_in(t, line) for t in tokens):
                        out_lines.append(_annotation_line(d))
                        report["lines_annotated"] += 1
                        changed = True
                out_lines.append(line)
                prev_input_annotation = set()
            if changed:
                io.safe_write(path, "".join(out_lines))
                report["corrected_files"].append(str(path))
            else:
                report["files_unchanged"] += 1
        else:
            report["skipped"].append(str(path))
    return report
