"""mev.io — append-only ledger IO + write-path guard (all builder writes go through here).

R1/W3 (D4): append_jsonl writes each record as a single O_APPEND os.write (one
syscall = atomic on ext4) when the encoded line fits the 64 KiB size cap; larger
records fall back to temp-file + os.replace. load_jsonl gains a tolerant-
quarantine mode (malformed lines reported + quarantined to a sidecar + skipped);
strict mode is retained and remains the only mode gates use.
"""
from __future__ import annotations
import hashlib, json, os, tempfile
from pathlib import Path

class CorruptLedgerError(RuntimeError):
    """Raised when a ledger line fails strict JSON parsing — detection, never repair."""

MAX_ATOMIC_RECORD_BYTES = 64 * 1024  # 64 KiB: single os.write cap for O_APPEND

# Root under which every builder write is allowed (set once at process start).
ALLOWED_WRITE_ROOTS: list[Path] = []
WRITE_AUDIT: list[str] = []

def allow(root: str | Path) -> None:
    p = Path(root).resolve()
    if p not in ALLOWED_WRITE_ROOTS:
        ALLOWED_WRITE_ROOTS.append(p)

def safe_write(path: str | Path, text: str, *, append: bool = False) -> Path:
    p = Path(path).resolve()
    if not any(str(p).startswith(str(r) + os.sep) or p == r for r in ALLOWED_WRITE_ROOTS):
        raise PermissionError(f"write outside allowed roots refused: {p}")
    p.parent.mkdir(parents=True, exist_ok=True)
    if append:
        with open(p, "a", encoding="utf-8") as f:
            f.write(text)
    else:
        # atomic replace for full writes
        fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=".tmp-")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, p)
    WRITE_AUDIT.append(str(p))
    return p

def append_jsonl(path: str | Path, obj: dict) -> Path:
    """Append one record as a single line. W3/D4: single O_APPEND os.write (one
    syscall, atomic on ext4) for lines <= MAX_ATOMIC_RECORD_BYTES; larger lines
    use temp-file + os.replace to guarantee atomicity at the file level."""
    p = Path(path).resolve()
    if not any(str(p).startswith(str(r) + os.sep) or p == r for r in ALLOWED_WRITE_ROOTS):
        raise PermissionError(f"write outside allowed roots refused: {p}")
    p.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n"
    data = line.encode("utf-8")
    if len(data) <= MAX_ATOMIC_RECORD_BYTES:
        fd = os.open(str(p), os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            os.write(fd, data)  # single syscall = atomic append on ext4
        finally:
            os.close(fd)
    else:
        # oversized record: read existing + append in a temp file + atomic rename
        existing = p.read_bytes() if p.exists() else b""
        fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=".tmp-")
        with os.fdopen(fd, "wb") as f:
            f.write(existing + data)
        os.replace(tmp, p)
    WRITE_AUDIT.append(str(p))
    return p

def load_jsonl(path: str | Path, *, tolerant: bool = False) -> list[dict]:
    """Load a JSONL ledger. Strict (default): any malformed line raises
    CorruptLedgerError (mutant gate — the only mode gates use).
    Tolerant (W3/D4 quarantine mode): a malformed line is reported, quarantined
    to <path>.quarantine (sidecar beside the ledger), and skipped; intact
    records are all returned."""
    p = Path(path)
    if not p.exists():
        return []
    out: list[dict] = []
    quarantined: list[dict] = []
    with open(p, encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            if not line.strip():
                if tolerant:
                    quarantined.append({"line_no": i, "raw": line.rstrip("\n"),
                                        "reason": "blank line"})
                    continue
                raise CorruptLedgerError(f"{p}:{i}: blank line in append-only ledger")
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError as e:
                if tolerant:
                    quarantined.append({"line_no": i, "raw": line.rstrip("\n"),
                                        "reason": f"malformed JSON ({e})"})
                    continue
                raise CorruptLedgerError(f"{p}:{i}: malformed JSON ({e})") from None
    if quarantined:
        sidecar = p.with_name(p.name + ".quarantine")
        # A2 (coherence addendum): the quarantine sidecar write goes through the
        # same guarded mechanism as every other builder write (safe_write ->
        # WRITE_AUDIT / allow-root containment), never a raw open().
        for q in quarantined:
            safe_write(sidecar, json.dumps(q, ensure_ascii=False) + "\n", append=True)
    return out

def norm_content(s: str) -> str:
    s = s.lower().strip()
    while s and s[-1] in ".;,!":
        s = s[:-1].rstrip()
    return " ".join(s.split())

def content_sha(s: str) -> str:
    return hashlib.sha256(norm_content(s).encode("utf-8")).hexdigest()
