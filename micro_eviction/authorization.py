"""mev.authorization — J-G7 fold: runtime go-live grant, read from the charge.

The charge's status section is the single source of truth. A verbatim Dallas
grant recorded there by the orchestrator (strict syntax below) authorizes
injection beyond the sandbox; absent that verbatim grant the pipeline terminates
at the sandbox. Nothing here is baked in — the grant is read at runtime from the
charge file, never from a hardcoded constant.

Grant syntax (status section of the charge, one line):
    Verbatim grant (Dallas): <verbatim grant text>
"""
from __future__ import annotations
import re
from pathlib import Path

GRANT_RE = re.compile(
    r"^[ \t]*(?:\*\*)?Verbatim grant \(Dallas\):(?:\*\*)?[ \t]*(?P<grant>\S.*)$",
    re.MULTILINE)


class ChargeError(RuntimeError):
    """Charge file missing or has no readable Status section."""


def read_status_section(charge_path: str | Path) -> str:
    """Return the charge's status section VERBATIM (exact file bytes, joined).

    Section start: first line beginning with 'Status:' after any frontmatter.
    Section end: the next blank line, or the next markdown heading.
    """
    text = Path(charge_path).read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)
    # skip YAML frontmatter
    i = 0
    if lines and lines[0].strip() == "---":
        for j in range(1, len(lines)):
            if lines[j].strip() == "---":
                i = j + 1
                break
    start = None
    for k in range(i, len(lines)):
        if lines[k].lstrip().startswith("Status:"):
            start = k
            break
    if start is None:
        raise ChargeError(f"no Status: section in {charge_path}")
    out = [lines[start]]
    for k in range(start + 1, len(lines)):
        s = lines[k]
        if not s.strip() or s.lstrip().startswith("#"):
            break
        out.append(s)
    return "".join(out)


def live_grant(charge_path: str | Path) -> dict:
    """Runtime authorization check: parse the charge's status section verbatim.

    Returns {grant, sandbox_only, status_verbatim, charge_path}. grant is the
    verbatim Dallas text when recorded, else None (=> sandbox-only).
    """
    status = read_status_section(charge_path)
    m = GRANT_RE.search(status)
    grant = m.group("grant").strip().lstrip("*").strip() if m else None
    return {"grant": grant,
            "sandbox_only": grant is None,
            "status_verbatim": status,
            "charge_path": str(Path(charge_path).resolve())}
