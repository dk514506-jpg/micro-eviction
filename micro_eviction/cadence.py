"""mev.cadence — S7 entropy cadence: the recurring eviction rhythm.

CHARGE-S7-ENTROPY-CADENCE-V1 (v1.2.1) §2 + §6. Plan of record: V3-ARCHITECT-PLAN
§8 (INDEX/SCHEDULE/CALENDAR), §10 row S7, §2 R16/R17/R8.

The cadence is a SCHEDULER + LONG-TAIL INDEX, not a store. It drives the
existing measured pipeline — it introduces no new storage engine, no new write
destination, and no new adjudication authority:

    detect eviction-eligible capture products (the corrected eligibility rule:
    capture products ARE eviction targets) -> stage through the existing
    EngraphisSink ingest path -> emit a pass receipt -> derive the long-tail
    index from those receipts.

Governing mechanics implemented here (map to the charge clauses):

  * A1 single-writer law — :func:`driver_lock` is ONE file-based advisory flock
    on ``ledgers/.driver.lock``; :mod:`memos_dispatcher` acquires the same lock
    around its cursor writes, so cadence and the dispatcher can never be
    concurrent drivers.
  * A2 encounter signal — a pass records retrieval hits + corroboration events
    in its receipt; the long tail counts encounters from receipts only.
  * A3 rebuildable index — ``longtail/index.jsonl`` is a pure function of
    (pass receipts, retirements, capture-row metadata). :func:`rebuild_index`
    deletes-and-rebuilds byte-identically.
  * A4 failure modes — bounded flock with a stale-holder report; idempotent
    re-runs (pass-receipt dedup); interval-drift correction (one pass services
    the overdue slot, missed slots are named, never replayed); at-cap is the A11
    HOLD.
  * A6 immune filter — the predicate is ``records_registry.is_immune`` (the §6
    named referent); immune records are never candidates and never indexed.
  * A9/A13 — cadence manages long-tail aging, drain candidacy (R8-gated) and
    never-re-encountered decay CANDIDACY. The DECAYED transition itself flows
    through the sanctioned staging-drain path only; cadence never deletes or
    rewrites staging payload.
  * A11 at-cap = HOLD, matching live R8 (:mod:`memos_dispatcher` semantics are
    imported, never forked): freeze the schedule cursor, zero enqueues, demand a
    drain review. HOLD is neither a skip nor a FAIL.
  * A12 receipts — every pass is receipt-bearing; the six R10 keys are carried
    and the record validates against the registry's ``pass_receipt`` form.

INERT ON IMPORT. Every write goes through :mod:`micro_eviction.io` containment.
The only destinations are the plan-sanctioned ``longtail/`` artifacts, the
``reports/pass_receipts/`` receipts, and the RUN-LOG journal.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sys
import time
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from . import directives as dirmod
from . import io as mev_io
from . import records_registry as registry_mod

# ── seat identity (dispatch rule: every new S7 file carries it) ──────────────
SEAT_ID = "ENGINEER-deepseek-v4.1-flash-s7-build"
SEAT_STAGE = "V3-S7"

DEFAULT_CAMPAIGN_ROOT = Path(
    os.environ.get("MEV_CAMPAIGN_ROOT")
    or Path(__file__).resolve().parent.parent
)

DAY_S = 86400.0
DEFAULT_INTERVAL_S = int(DAY_S)
INTERVAL_ENV = "MEV_CADENCE_INTERVAL_S"

LONGTAIL_DIR = "longtail"
INDEX_NAME = "index.jsonl"
SCHEDULE_NAME = "schedule.json"
CALENDAR_NAME = "calendar.jsonl"
RECEIPTS_REL = Path("reports") / "pass_receipts"
FAILED_SUBDIR = "failed"
LOCK_RELPATH = Path("ledgers") / ".driver.lock"
RUNLOG_NAME = "RUN-LOG.md"

SCHEDULE_SCHEMA_VERSION = 1
GENERATOR = "micro_eviction/cadence.py"

# long-tail states (the registry form enum is the authority; kept in step here)
RESIDENT = "resident"
AGING = "aging"
RISING = "rising"
DRAIN_CANDIDATE = "drain_candidate"
DECAYED = "decayed"
RETIRED = "retired"
STATES = (RESIDENT, AGING, RISING, DRAIN_CANDIDATE, DECAYED, RETIRED)

OUTCOMES = ("STAGED", "NOOP", "DEDUPED", "HOLD_AT_CAP", "FAILED", "PARTIAL")
# Outcomes whose receipt is terminal for dedup. A HOLD is a demand signal, not a
# terminal pass: after the drain review the same slot must be able to fire again.
DEDUP_OUTCOMES = frozenset({"STAGED", "NOOP", "DEDUPED", "PARTIAL"})

DEFAULT_CONFIG = {
    "interval_s": DEFAULT_INTERVAL_S,
    "aging_after_days": 180,
    "retention_days": 365,
    "rise_encounters": 2,
}

STAGE = "S7"


# ── errors ───────────────────────────────────────────────────────────────────

class CadenceError(RuntimeError):
    """Base class for loud cadence refusals."""


class DriverLockBusy(CadenceError):
    """The shared advisory driver lock is held by another driver."""

    def __init__(self, path, holder: Optional[dict] = None, *, stale: bool = False):
        self.path = str(path)
        self.holder = holder or {}
        self.stale = stale
        who = self.holder.get("seat") or self.holder.get("pid") or "unknown holder"
        suffix = " (STALE holder — the lock is not auto-broken)" if stale else ""
        super().__init__(
            f"driver lock busy at {path}: held by {who}{suffix}; "
            "cadence and the memos dispatcher share one advisory lock (A1)")


class SingleInstanceRefused(CadenceError):
    """A pass was requested while another driver owns the lock."""

    def __init__(self, busy: DriverLockBusy):
        self.busy = busy
        super().__init__(f"single-instance refusal: {busy}")


class RegistryUnavailable(CadenceError):
    """The forms registry could not be read or provisioned (poison case)."""

    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(f"registry unavailable: {detail}")


class CadenceUnavailable(CadenceError):
    """A required pipeline component is not available (service/limits/...)."""


class RetireRefused(CadenceError):
    """A retire was requested without gate review or without a drain candidacy."""


class SpineVerificationFailed(CadenceError):
    """The spine/ledger citation no longer verifies — retire is refused."""


# ── paths (plan §8: the sanctioned destinations) ─────────────────────────────

def longtail_dir(campaign_root) -> Path:
    return Path(campaign_root) / LONGTAIL_DIR


def index_path(campaign_root) -> Path:
    return longtail_dir(campaign_root) / INDEX_NAME


def schedule_path(campaign_root) -> Path:
    return longtail_dir(campaign_root) / SCHEDULE_NAME


def calendar_path(campaign_root) -> Path:
    return longtail_dir(campaign_root) / CALENDAR_NAME


def receipts_dir(campaign_root) -> Path:
    return Path(campaign_root) / RECEIPTS_REL


def failed_receipts_dir(campaign_root) -> Path:
    return receipts_dir(campaign_root) / FAILED_SUBDIR


def lock_path(campaign_root) -> Path:
    return Path(campaign_root) / LOCK_RELPATH


def runlog_path(campaign_root) -> Path:
    return Path(campaign_root) / RUNLOG_NAME


def utc_iso(ts: Optional[float] = None) -> str:
    moment = (datetime.fromtimestamp(float(ts), timezone.utc) if ts is not None
              else datetime.now(timezone.utc))
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def sha256_12(text: str | bytes) -> str:
    data = text.encode("utf-8") if isinstance(text, str) else text
    return hashlib.sha256(data).hexdigest()[:12]


# ── the shared advisory driver lock (A1 / A4) ────────────────────────────────
#
# ONE lock file, ONE acquisition primitive. cadence holds it for a pass;
# memos_dispatcher holds the same one around its cursor read/write. flock is
# released by the kernel if a process dies, so a leftover file is not a lock —
# but the holder record is kept so a bounded wait can name who blocked it, and
# so a caller can see that the holder looks stale.

class DriverLock:
    """Handle returned by :func:`driver_lock` (release is idempotent)."""

    def __init__(self, path: Path, fd: int, holder: dict):
        self.path = Path(path)
        self.fd = fd
        self.holder = dict(holder)
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            released = dict(self.holder)
            released["state"] = "released"
            released["released_at"] = time.time()
            _write_holder(self.fd, released)
        finally:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.release()
        return False


def _write_holder(fd: int, holder: dict) -> None:
    data = (json.dumps(holder, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
    os.lseek(fd, 0, os.SEEK_SET)
    os.ftruncate(fd, 0)
    os.write(fd, data)


def read_lock_holder(path) -> dict:
    """Best-effort read of the holder record; empty dict when unreadable."""
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except (OSError, ValueError):
        return {}
    try:
        doc = json.loads(raw)
    except ValueError:
        return {}
    return doc if isinstance(doc, dict) else {}


@contextmanager
def driver_lock(campaign_root, *, timeout_s: float = 0.0,
                stale_after_s: float = 3600.0, poll_s: float = 0.05,
                sleep_fn: Optional[Callable[[float], None]] = None,
                seat: Optional[str] = None):
    """Acquire the shared driver lock, or raise :class:`DriverLockBusy`.

    ``timeout_s`` bounds the wait (default 0 = refuse immediately: the
    single-instance law). ``stale_after_s`` does not break anything by itself —
    an over-age holder is reported as ``stale=True`` so the operator can see it;
    silently stealing a lock is exactly the failure mode the law forbids.
    """
    path = lock_path(campaign_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    sleep = sleep_fn or time.sleep
    fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
    deadline = time.monotonic() + max(0.0, float(timeout_s))
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except (BlockingIOError, OSError):
            holder = read_lock_holder(path)
            acquired_at = holder.get("acquired_at")
            stale = (isinstance(acquired_at, (int, float))
                     and (time.time() - float(acquired_at)) > float(stale_after_s))
            if time.monotonic() >= deadline:
                os.close(fd)
                raise DriverLockBusy(path, holder, stale=stale)
            sleep(poll_s)
    holder = {
        "pid": os.getpid(),
        "seat": seat or SEAT_ID,
        "stage": SEAT_STAGE,
        "state": "held",
        "acquired_at": time.time(),
        "acquired_at_iso": utc_iso(),
        "path": str(path),
    }
    try:
        _write_holder(fd, holder)
    except OSError:
        pass
    lock = DriverLock(path, fd, holder)
    try:
        yield lock
    finally:
        lock.release()


# ── config / schedule ────────────────────────────────────────────────────────

def default_config(*, interval_s: Optional[int] = None) -> dict:
    cfg = dict(DEFAULT_CONFIG)
    env = os.environ.get(INTERVAL_ENV)
    if env:
        cfg["interval_s"] = int(float(env))
    if interval_s is not None:
        cfg["interval_s"] = int(float(interval_s))
    return cfg


def load_schedule(campaign_root) -> dict:
    p = schedule_path(campaign_root)
    if not p.is_file():
        return {}
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise CadenceError(f"unreadable schedule {p}: {exc}") from exc
    return doc if isinstance(doc, dict) else {}


def load_config(campaign_root, *, interval_s: Optional[int] = None,
                overrides: Optional[dict] = None) -> dict:
    """Config-driven interval/thresholds: explicit > schedule.json > env > default."""
    cfg = default_config(interval_s=interval_s)
    try:
        sched = load_schedule(campaign_root)
    except CadenceError:
        sched = {}
    if isinstance(sched.get("interval_s"), (int, float)) and interval_s is None:
        cfg["interval_s"] = int(sched["interval_s"])
    thresholds = sched.get("thresholds")
    if isinstance(thresholds, dict):
        for key in ("aging_after_days", "retention_days", "rise_encounters"):
            if isinstance(thresholds.get(key), (int, float)):
                cfg[key] = thresholds[key]
    for key, value in (overrides or {}).items():
        if key in cfg and isinstance(value, (int, float)):
            cfg[key] = value
    return cfg


def _schedule_doc(config: dict, *, fire_at: float, next_fire_at: float,
                  pass_id: Optional[str], outcome: Optional[str],
                  passes: int, now: float, extra: Optional[dict] = None) -> dict:
    doc = {
        "schema_version": SCHEDULE_SCHEMA_VERSION,
        "generator": GENERATOR,
        "seat": SEAT_ID,
        "interval_s": int(config["interval_s"]),
        "thresholds": {
            "aging_after_days": config["aging_after_days"],
            "retention_days": config["retention_days"],
            "rise_encounters": config["rise_encounters"],
        },
        "last_fire_at": float(fire_at),
        "next_fire_at": float(next_fire_at),
        "last_pass_id": pass_id,
        "last_outcome": outcome,
        "passes": int(passes),
        "updated_at": utc_iso(now),
    }
    if extra:
        doc.update(extra)
    return doc


def write_schedule(campaign_root, doc: dict) -> Path:
    p = schedule_path(campaign_root)
    mev_io.allow(p.parent)
    mev_io.safe_write(p, json.dumps(doc, sort_keys=True, indent=1,
                                    ensure_ascii=False) + "\n")
    return p


# ── capture surface scan (read-only; the registry declares the surface) ─────

def _iter_jsonl_surface(campaign_root, source: dict) -> Iterable[tuple]:
    """Yield (path, line_no, row) for a registry-declared jsonl_glob surface."""
    root = Path(campaign_root)
    globs = source.get("glob")
    if isinstance(globs, str):
        globs = [globs]
    if not globs:
        return
    exclude_suffixes = tuple(source.get("exclude_suffixes") or ())
    exclude_names = set(source.get("exclude_names") or ())
    for pattern in globs:
        for path in sorted(root.glob(pattern)):
            if not path.is_file():
                continue
            if path.name in exclude_names:
                continue
            if exclude_suffixes and path.name.endswith(exclude_suffixes):
                continue
            with open(path, encoding="utf-8") as handle:
                for lineno, line in enumerate(handle, 1):
                    if not line.strip():
                        raise mev_io.CorruptLedgerError(
                            f"{path}:{lineno}: blank line in append-only ledger")
                    try:
                        row = json.loads(line)
                    except ValueError as exc:
                        raise mev_io.CorruptLedgerError(
                            f"{path}:{lineno}: malformed JSON ({exc})") from None
                    yield path, lineno, row


def _spine_ref(campaign_root, path: Path, line_no: int) -> dict:
    """Payload-free citation to the ledger row and its sha-spine (R18/R17)."""
    root = Path(campaign_root)
    ref = {"ledger": os.path.relpath(path, root), "line": int(line_no)}
    if path.name.endswith(".jsonl"):
        spine = path.with_name(path.name[:-len(".jsonl")] + ".spine.jsonl")
        if spine.is_file():
            ref["spine"] = os.path.relpath(spine, root)
            ref["spine_sha256_12"] = sha256_12(spine.read_bytes())
        else:
            ref["spine"] = None
    else:
        ref["spine"] = None
    return ref


def scan_capture_surface(campaign_root, registry) -> list[dict]:
    """Read the capture form's declared surface. Never writes."""
    spec = registry.get("capture")
    source = spec.get("source") or {}
    if source.get("kind") != "jsonl_glob":
        raise CadenceUnavailable(
            f"capture form declares source kind {source.get('kind')!r}; "
            "cadence only scans a jsonl_glob surface")
    require = list(source.get("require") or ())
    observations: list[dict] = []
    for path, line_no, row in _iter_jsonl_surface(campaign_root, source):
        if not isinstance(row, dict):
            continue
        if any(key not in row for key in require):
            continue
        observations.append({
            "record": row,
            "source": {"path": os.path.relpath(path, Path(campaign_root)),
                       "line": line_no},
            "spine_ref": _spine_ref(campaign_root, path, line_no),
        })
    return observations


def ledger_shas(campaign_root, registry) -> dict:
    """sha256_12 per scanned ledger — the pass input basis (dedup + upstream)."""
    spec = registry.get("capture")
    source = spec.get("source") or {}
    globs = source.get("glob")
    globs = [globs] if isinstance(globs, str) else list(globs or [])
    exclude_suffixes = tuple(source.get("exclude_suffixes") or ())
    exclude_names = set(source.get("exclude_names") or ())
    out: dict[str, str] = {}
    root = Path(campaign_root)
    for pattern in globs:
        for path in sorted(root.glob(pattern)):
            if not path.is_file() or path.name in exclude_names:
                continue
            if exclude_suffixes and path.name.endswith(exclude_suffixes):
                continue
            out[os.path.relpath(path, root)] = sha256_12(path.read_bytes())
    return out


def _eligible(observations: list[dict], registry) -> tuple[list[dict], Counter]:
    """Apply the corrected eligibility rule + immune filter (A6)."""
    candidates: list[dict] = []
    skipped: Counter = Counter()
    for obs in observations:
        record = obs["record"]
        if registry_mod.is_immune(record):
            skipped["immune"] += 1
            continue
        created_at = record.get("created_at")
        if isinstance(created_at, bool) or not isinstance(created_at, (int, float)):
            skipped["no_created_at"] += 1
            continue
        if not record.get("capture_sha"):
            skipped["unbound_capture_sha"] += 1
            continue
        candidates.append(obs)
    return candidates, skipped


# ── encounter signal (A2) ────────────────────────────────────────────────────

def normalize_encounter_signal(signal: Optional[dict]) -> dict:
    """Canonical encounter signal: retrieval hits + corroboration events.

    Only identifiers travel — the receipt records WHAT was re-encountered, never
    the payload. Unknown keys are refused so the signal stays measured.
    """
    signal = signal or {}
    if not isinstance(signal, dict):
        raise CadenceError("encounter signal must be a dict")
    unknown = set(signal) - {"retrieval_hits", "corroboration_events"}
    if unknown:
        raise CadenceError(f"encounter signal has unknown keys: {sorted(unknown)}")
    out = {}
    for key in ("retrieval_hits", "corroboration_events"):
        values = signal.get(key) or []
        if not isinstance(values, (list, tuple)):
            raise CadenceError(f"encounter signal {key} must be a list")
        out[key] = sorted({str(v) for v in values if str(v).strip()})
    out["digest"] = sha256_12(_canonical(out))
    return out


# ── long-tail derivation (A3): index is a pure function of receipts ─────────

def derive_state(*, age_s: Optional[float], encounters: int, config: dict) -> str:
    """Long-tail state from age-since-capture and measured encounters.

    Rising wins over aging/decay: a re-encountered item is a promotion candidate
    even at the retention boundary (R16 re-promotion reach), and promotion stays
    dream-pass/Dallas territory — cadence only DEMANDS the review.
    """
    if encounters >= int(config["rise_encounters"]):
        return RISING
    if age_s is None:
        return RESIDENT
    if age_s >= float(config["retention_days"]) * DAY_S:
        return DECAYED if encounters == 0 else DRAIN_CANDIDATE
    if age_s >= float(config["aging_after_days"]) * DAY_S:
        return AGING
    return RESIDENT


def load_pass_receipts(campaign_root) -> list[dict]:
    """Every form-valid pass receipt on disk (failure markers live elsewhere)."""
    out: list[dict] = []
    d = receipts_dir(campaign_root)
    if not d.is_dir():
        return out
    for path in sorted(d.glob("pass_*.json")):
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(doc, dict) and doc.get("pass_id"):
            doc.setdefault("_path", str(path))
            out.append(doc)
    return out


def load_retirements(campaign_root) -> dict:
    """Retirement events from the CALENDAR (the sanctioned drain-decision log)."""
    out: dict[str, dict] = {}
    for row in mev_io.load_jsonl(calendar_path(campaign_root)):
        if isinstance(row, dict) and row.get("kind") == "retire" and row.get("record_id"):
            out[row["record_id"]] = row
    return out


def _capture_meta(campaign_root, registry) -> dict:
    meta: dict[str, dict] = {}
    for obs in scan_capture_surface(campaign_root, registry):
        record = obs["record"]
        rid = record.get("record_id")
        if isinstance(rid, str) and rid not in meta:
            meta[rid] = {"record": record, "spine_ref": obs["spine_ref"]}
    return meta


def build_index(campaign_root, *, now: float, config: dict, registry,
                receipts: list[dict], retirements: Optional[dict] = None) -> dict:
    """Derive index rows + text. Pure: same inputs + clock -> same bytes."""
    retirements = retirements or {}
    meta = _capture_meta(campaign_root, registry)
    agg: dict[str, dict] = {}

    def _slot(rid: str) -> dict:
        return agg.setdefault(rid, {
            "first_seen_pass": None, "staged_at": None,
            "retrieval_hits": 0, "corroboration_events": 0,
            "last_encounter_fire_at": None,
        })

    for receipt in sorted(receipts, key=lambda r: (float(r.get("fire_at") or 0.0),
                                                   str(r.get("pass_id") or ""))):
        pass_id = receipt.get("pass_id")
        fire_at = receipt.get("fire_at")
        window = receipt.get("window") or {}
        for rid in window.get("staged_ids") or []:
            entry = _slot(str(rid))
            if entry["first_seen_pass"] is None:
                entry["first_seen_pass"] = pass_id
            if entry["staged_at"] is None and isinstance(fire_at, (int, float)):
                entry["staged_at"] = float(fire_at)
        signal = receipt.get("encounter_signal") or {}
        for key in ("retrieval_hits", "corroboration_events"):
            for rid in signal.get(key) or []:
                entry = _slot(str(rid))
                if entry["first_seen_pass"] is None:
                    entry["first_seen_pass"] = pass_id
                entry[key] += 1
                if isinstance(fire_at, (int, float)):
                    prev = entry["last_encounter_fire_at"]
                    entry["last_encounter_fire_at"] = (
                        float(fire_at) if prev is None else max(prev, float(fire_at)))

    rows: list[dict] = []
    excluded_immune: list[str] = []
    meta_missing: list[str] = []
    for rid in sorted(agg, key=lambda r: ((meta.get(r) or {}).get("record", {})
                                          .get("created_at") or 0, r)):
        entry = agg[rid]
        known = meta.get(rid)
        excluded_immune_flag = False
        created_at = None
        topic_hash = None
        rtype = None
        spine_ref = None
        if known is not None:
            record = known["record"]
            if registry_mod.is_immune(record):
                excluded_immune_flag = True
            created_at = record.get("created_at")
            topic_hash = record.get("topic_hash")
            rtype = record.get("type")
            spine_ref = known["spine_ref"]
        if excluded_immune_flag:
            excluded_immune.append(rid)
            continue
        if known is None:
            meta_missing.append(rid)
        age_basis = created_at if isinstance(created_at, (int, float)) else entry["staged_at"]
        age_s = (float(now) - float(age_basis)) if isinstance(age_basis, (int, float)) else None
        encounters = entry["retrieval_hits"] + entry["corroboration_events"]
        retirement = retirements.get(rid)
        state = RETIRED if retirement else derive_state(
            age_s=age_s, encounters=encounters, config=config)
        row = {
            "record_id": rid,
            "topic_hash": topic_hash,
            "type": rtype,
            "created_at": created_at,
            "staged_at": entry["staged_at"],
            "first_seen_pass": entry["first_seen_pass"],
            "encounters": {
                "retrieval_hits": entry["retrieval_hits"],
                "corroboration_events": entry["corroboration_events"],
                "total": encounters,
            },
            "last_encounter_fire_at": entry["last_encounter_fire_at"],
            "age_s": None if age_s is None else round(age_s, 3),
            "age_days": None if age_s is None else round(age_s / DAY_S, 3),
            "state": state,
            "drain_review_demanded": state in (DRAIN_CANDIDATE, DECAYED),
            "promotion_review_demanded": state == RISING,
            "spine_ref": spine_ref or {"ledger": None, "line": None, "spine": None},
            "payload": None,
        }
        if known is None:
            row["meta_missing"] = True
        if retirement:
            row["retired_at"] = retirement.get("ts")
            row["directives"] = list(retirement.get("directives") or [])
        rows.append(row)

    text = "".join(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n"
                   for row in rows)
    counts = Counter(row["state"] for row in rows)
    report = {
        "path": str(index_path(campaign_root)),
        "entries": len(rows),
        "states": {state: int(counts.get(state, 0)) for state in STATES},
        "rising": [r["record_id"] for r in rows if r["state"] == RISING],
        "aging": [r["record_id"] for r in rows if r["state"] == AGING],
        "drain_candidates": [r["record_id"] for r in rows
                             if r["state"] in (DRAIN_CANDIDATE, DECAYED)],
        "decayed": [r["record_id"] for r in rows if r["state"] == DECAYED],
        "retired": [r["record_id"] for r in rows if r["state"] == RETIRED],
        "excluded_immune": sorted(excluded_immune),
        "meta_missing": sorted(meta_missing),
        "sha256_12": sha256_12(text.encode("utf-8")),
    }
    return {"rows": rows, "text": text, "report": report}


def _provision(registry) -> dict:
    """Provision the forms estate (S6 defaults first, then the S7 cadence forms)."""
    registry.ensure_default_forms()
    return registry.ensure_cadence_forms()


def _validate_index_rows(registry, rows: list[dict]) -> None:
    for row in rows:
        violations = registry.validate_record("longtail_index", row)
        if violations:
            raise CadenceError(
                f"index row {row.get('record_id')} violates the longtail_index "
                f"form: {'; '.join(violations)}")


def write_index(campaign_root, text: str) -> Path:
    p = index_path(campaign_root)
    mev_io.allow(p.parent)
    mev_io.safe_write(p, text)
    return p


def rebuild_index(campaign_root, *, now: Optional[float] = None,
                  config: Optional[dict] = None, registry=None,
                  write: bool = True) -> dict:
    """Delete-and-rebuild the long tail from receipts (A3 byte-compare path)."""
    now = time.time() if now is None else float(now)
    reg = registry or registry_mod.RecordsRegistry(str(campaign_root))
    _provision(reg)
    cfg = config or load_config(campaign_root)
    built = build_index(campaign_root, now=now, config=cfg, registry=reg,
                        receipts=load_pass_receipts(campaign_root),
                        retirements=load_retirements(campaign_root))
    _validate_index_rows(reg, built["rows"])
    if write:
        write_index(campaign_root, built["text"])
    return built["report"]


def read_index(campaign_root) -> list[dict]:
    return mev_io.load_jsonl(index_path(campaign_root))


# ── pass receipts (A8/A12) ──────────────────────────────────────────────────

def pass_receipt_path(campaign_root, pass_id: str) -> Path:
    return receipts_dir(campaign_root) / f"{pass_id}.json"


def load_pass_receipt(campaign_root, pass_id: str) -> Optional[dict]:
    p = pass_receipt_path(campaign_root, pass_id)
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def write_pass_receipt(campaign_root, receipt: dict, registry) -> Path:
    """Validate against the registry form, then atomic-write. Loud on violation."""
    violations = registry.validate_record("pass_receipt", receipt)
    if violations:
        raise CadenceError("pass receipt violates the pass_receipt form: "
                           + "; ".join(violations))
    p = pass_receipt_path(campaign_root, receipt["pass_id"])
    mev_io.allow(p.parent)
    mev_io.safe_write(p, json.dumps(receipt, sort_keys=True, indent=1,
                                    ensure_ascii=False) + "\n")
    return p


def write_failure_marker(campaign_root, *, marker: str, error: str,
                         now: float, fire_at: Optional[float] = None) -> Path:
    """The explicit failure marker (never a silent no-op).

    This is deliberately NOT a form-valid pass receipt: when the registry is the
    unavailable component, a registry-validated receipt cannot exist. It lands in
    ``reports/pass_receipts/failed/`` so the ``pass_*.json`` form surface never
    sweeps it.
    """
    d = failed_receipts_dir(campaign_root)
    path = d / f"pass_FAILED_{int(now)}_{marker}.json"
    doc = {
        "status": "FAILED",
        "marker": marker,
        "error": str(error),
        "ts": utc_iso(now),
        "fire_at": fire_at,
        "seat": SEAT_ID,
        "stage": SEAT_STAGE,
        "form_valid": False,
        "reason": "the forms registry (or a required pipeline component) was "
                  "unavailable; a pass receipt cannot be registry-validated",
    }
    mev_io.allow(d)
    mev_io.safe_write(path, json.dumps(doc, sort_keys=True, indent=1,
                                       ensure_ascii=False) + "\n")
    return path


# ── at-cap referent (A11: imported from live R8, never forked) ───────────────

def _staging_at_cap(staging_state) -> tuple[bool, str, dict]:
    root = str(Path(__file__).resolve().parent.parent)
    if root not in sys.path:
        sys.path.insert(0, root)
    try:
        import memos_dispatcher  # the live R8 implementation
    except Exception as exc:  # noqa: BLE001 — named, never silently replaced
        raise CadenceUnavailable(
            f"R8 cap referent unavailable (memos_dispatcher): "
            f"{type(exc).__name__}: {exc}") from exc
    return memos_dispatcher.staging_at_cap(staging_state)


def staging_limits() -> dict:
    """Caps flow from engraphis_sink.STAGING_LIMITS — the single referent."""
    root = str(Path(__file__).resolve().parent.parent)
    if root not in sys.path:
        sys.path.insert(0, root)
    try:
        import engraphis_sink
    except Exception as exc:  # noqa: BLE001
        raise CadenceUnavailable(f"STAGING_LIMITS unavailable: {exc}") from exc
    return dict(engraphis_sink.STAGING_LIMITS)


def open_staging_readonly(store_path):
    """Read-only staging handle (ro-URI + query_only), via the S-live provider."""
    root = str(Path(__file__).resolve().parent.parent)
    if root not in sys.path:
        sys.path.insert(0, root)
    import staging_provider  # campaign-root module
    return staging_provider.open_readonly(store_path)


# ── the scheduler ───────────────────────────────────────────────────────────

class CadenceScheduler:
    """Interval scheduler for receipt-bearing eviction passes.

    ``now_fn`` is the clock seam: tests drive it synthetically, nothing sleeps in
    this class. ``service_factory`` returns the sanctioned MemoryService (the
    sink's ingest path); cadence never constructs a second writer.
    """

    def __init__(self, campaign_root=None, *, interval_s: Optional[int] = None,
                 thresholds: Optional[dict] = None,
                 now_fn: Optional[Callable[[], float]] = None,
                 store_path=None, workspace: Optional[str] = None,
                 registry=None, service_factory: Optional[Callable[[], Any]] = None,
                 ownership: Any = None, seat: Optional[str] = None,
                 runlog=None, lock_timeout_s: float = 0.0,
                 sleep_fn: Optional[Callable[[float], None]] = None):
        self.campaign_root = Path(campaign_root or DEFAULT_CAMPAIGN_ROOT)
        self._now_fn = now_fn or time.time
        self.store_path = store_path
        self._workspace = workspace
        self._registry_obj = registry
        self.service_factory = service_factory
        self.ownership_arg = ownership
        self.seat = seat or SEAT_ID
        self.runlog = Path(runlog) if runlog else runlog_path(self.campaign_root)
        self.lock_timeout_s = lock_timeout_s
        self.sleep_fn = sleep_fn
        self.config = load_config(self.campaign_root, interval_s=interval_s,
                                  overrides=thresholds)

    # ── small accessors ──

    def now(self, value: Optional[float] = None) -> float:
        return float(self._now_fn()) if value is None else float(value)

    @property
    def workspace(self) -> str:
        if self._workspace:
            return self._workspace
        root = str(self.campaign_root)
        if root not in sys.path:
            sys.path.insert(0, root)
        import staging_provider
        return staging_provider.WORKSPACE

    def registry(self):
        if self._registry_obj is None:
            self._registry_obj = registry_mod.RecordsRegistry(str(self.campaign_root))
        return self._registry_obj

    def ensure_forms(self) -> dict:
        return _provision(self.registry())

    def ownership(self) -> Any:
        if self.ownership_arg is not None:
            return (self.ownership_arg() if callable(self.ownership_arg)
                    else self.ownership_arg)
        import memos_push
        return memos_push.resolve_induction_ownership()

    def service(self):
        if self.service_factory is not None:
            return self.service_factory()
        if self.store_path:
            raise CadenceUnavailable(
                "no sanctioned staging service configured: cadence does not "
                "construct an Engraphis writer of its own; pass service_factory "
                "(the MemoryService seam) for a live run")
        raise CadenceUnavailable("no staging service configured")

    # ── due / drift correction ──

    def due(self, now: Optional[float] = None) -> dict:
        now = self.now(now)
        interval = int(self.config["interval_s"])
        schedule = load_schedule(self.campaign_root)
        nxt = schedule.get("next_fire_at")
        last = schedule.get("last_fire_at")
        if not isinstance(nxt, (int, float)):
            return {"due": True, "bootstrap": True, "fire_at": now,
                    "next_fire_at": now + interval, "missed_passes": 0,
                    "interval_s": interval, "last_fire_at": last}
        if now < float(nxt):
            return {"due": False, "bootstrap": False, "fire_at": float(nxt),
                    "next_fire_at": float(nxt), "missed_passes": 0,
                    "interval_s": interval,
                    "seconds_until": float(nxt) - now,
                    "last_fire_at": last}
        missed = int((now - float(nxt)) // interval)
        return {"due": True, "bootstrap": False, "fire_at": float(nxt),
                "next_fire_at": float(nxt) + (missed + 1) * interval,
                "missed_passes": missed, "interval_s": interval,
                "last_fire_at": last}

    def tick(self, now: Optional[float] = None) -> dict:
        return self.due(now)

    # ── one pass ──

    def run_pass(self, *, now: Optional[float] = None, force: bool = False,
                 encounter_signal: Optional[dict] = None,
                 staging_state: Any = None, dry_run: bool = False,
                 service: Any = None) -> dict:
        """Run one pass UNDER the shared driver lock (A1 single-instance law).

        A concurrent driver means REFUSAL: nothing is detected, staged, written
        or advanced. The refusal is explicit (payload + journal), never a silent
        second run.
        """
        try:
            with driver_lock(self.campaign_root, timeout_s=self.lock_timeout_s,
                             sleep_fn=self.sleep_fn, seat=self.seat):
                return self._run_pass(now=now, force=force,
                                      encounter_signal=encounter_signal,
                                      staging_state=staging_state,
                                      dry_run=dry_run, service=service)
        except DriverLockBusy as exc:
            moment = self.now(now)
            self._journal([
                f"## [{utc_iso(moment)}] cadence (S7) — PASS REFUSED: "
                f"DRIVER LOCK BUSY (A1)",
                f"- lock: `{exc.path}` holder: {exc.holder} stale={exc.stale}",
                "- single-instance law: cadence and the memos dispatcher share one"
                " advisory lock; no detection, no staging, no cursor movement",
            ])
            return {"outcome": "REFUSED", "reason": "driver_lock_busy",
                    "lock": {"path": exc.path, "holder": exc.holder,
                             "stale": exc.stale},
                    "pass_id": None, "ts": utc_iso(moment)}

    def _run_pass(self, *, now: Optional[float] = None, force: bool = False,
                  encounter_signal: Optional[dict] = None,
                  staging_state: Any = None, dry_run: bool = False,
                  service: Any = None) -> dict:
        """The locked body of one receipt-bearing pass."""
        now = self.now(now)
        due = self.due(now)
        if not due["due"] and not force:
            return {"outcome": "NOT_DUE", "due": due, "pass_id": None,
                    "ts": utc_iso(now)}
        fire_at = float(due["fire_at"]) if due["due"] else float(now)
        next_fire_at = float(due["next_fire_at"]) if due["due"] else float(now) + int(
            self.config["interval_s"])
        missed = int(due.get("missed_passes") or 0)

        # ── registry poison gate (explicit marker, never a silent pass) ──
        try:
            registry = self.registry()
            _provision(registry)
        except (registry_mod.RegistryCorrupt, registry_mod.RegistryError) as exc:
            marker = write_failure_marker(
                self.campaign_root, marker="REGISTRY_UNAVAILABLE",
                error=f"{type(exc).__name__}: {exc}", now=now, fire_at=fire_at)
            self._journal([f"## [{utc_iso(now)}] cadence (S7) — PASS REFUSED: "
                           f"REGISTRY_UNAVAILABLE",
                           f"- error: {type(exc).__name__}: {exc}",
                           f"- failure marker: `{marker}`",
                           "- no detection, no staging, no index write"])
            return {"outcome": "FAILED", "marker": "REGISTRY_UNAVAILABLE",
                    "error": f"{type(exc).__name__}: {exc}",
                    "marker_path": str(marker), "pass_id": None,
                    "ts": utc_iso(now)}

        signal = normalize_encounter_signal(encounter_signal)
        registry_spec = registry.get("pass_receipt")  # existence check (poison)
        del registry_spec

        try:
            observations = scan_capture_surface(self.campaign_root, registry)
            shas = ledger_shas(self.campaign_root, registry)
        except mev_io.CorruptLedgerError as exc:
            marker = write_failure_marker(
                self.campaign_root, marker="LEDGER_CORRUPT",
                error=f"{type(exc).__name__}: {exc}", now=now, fire_at=fire_at)
            self._journal([f"## [{utc_iso(now)}] cadence (S7) — PASS REFUSED: "
                           f"LEDGER_CORRUPT",
                           f"- error: {exc}", f"- failure marker: `{marker}`"])
            return {"outcome": "FAILED", "marker": "LEDGER_CORRUPT",
                    "error": str(exc), "marker_path": str(marker),
                    "pass_id": None, "ts": utc_iso(now)}

        candidates, skipped = _eligible(observations, registry)

        if dry_run:
            return {"outcome": "DRY_RUN", "pass_id": None, "ts": utc_iso(now),
                    "due": due, "window": {"records_scanned": len(observations),
                                           "eligible": len(candidates),
                                           "skipped": dict(skipped)},
                    "candidates": [{"record_id": o["record"]["record_id"],
                                    "created_at": o["record"].get("created_at"),
                                    "source": o["source"]}
                                   for o in candidates],
                    "encounter_signal": signal, "ledgers": shas}

        at_cap, cap_basis, cap_state = _staging_at_cap(staging_state)
        cap_state = {"records": cap_state.get("records"),
                     "bytes": cap_state.get("bytes")}
        slot_key = self._slot_key(fire_at=fire_at,
                                  interval=int(self.config["interval_s"]),
                                  shas=shas, signal=signal)
        pass_id = self._pass_id(slot_key=slot_key, cap_state=cap_state,
                                at_cap=at_cap, cap_basis=cap_basis)

        # A4 idempotent re-runs: a terminal receipt for this SLOT (not just this
        # pass id) means the pass already ran. Cap state is part of the pass id
        # precisely so a HOLD never dedups the post-drain retry.
        prior_terminal = next(
            (r for r in load_pass_receipts(self.campaign_root)
             if r.get("slot_key") == slot_key
             and r.get("outcome") in DEDUP_OUTCOMES), None)
        prior_same = load_pass_receipt(self.campaign_root, pass_id)
        if prior_terminal is not None:
            self._advance_schedule(now=now, fire_at=fire_at, next_fire_at=next_fire_at,
                                   pass_id=prior_terminal.get("pass_id"),
                                   outcome="DEDUPED",
                                   schedule=load_schedule(self.campaign_root))
            self._journal([f"## [{utc_iso(now)}] cadence (S7) — PASS DEDUPED",
                           f"- slot: `{slot_key}` (fire_at {fire_at})",
                           f"- terminal receipt already on disk: "
                           f"`{prior_terminal.get('pass_id')}` "
                           f"({prior_terminal.get('outcome')})",
                           "- no detection result rewrites, no staging, no index"
                           " rewrite; schedule realigned (drift-corrected)"])
            return {"outcome": "DEDUPED", "pass_id": prior_terminal.get("pass_id"),
                    "slot_key": slot_key,
                    "dedup_of": prior_terminal.get("pass_id"),
                    "ts": utc_iso(now), "fire_at": fire_at,
                    "next_fire_at": next_fire_at, "missed_passes": missed,
                    "receipt": prior_terminal}

        base_window = {"records_scanned": len(observations),
                       "eligible": len(candidates),
                       "staged": 0, "staged_ids": [],
                       "skipped": dict(skipped),
                       "ledgers": shas}

        # ── A11 at-cap = HOLD (freeze; zero enqueues; demand drain review) ──
        if at_cap:
            return self._hold(now=now, fire_at=fire_at, next_fire_at=next_fire_at,
                              missed=missed, pass_id=pass_id, signal=signal,
                              window=base_window, cap_state=cap_state,
                              cap_basis=cap_basis, registry=registry,
                              prior=prior_same, slot_key=slot_key)

        # ── stage through the EXISTING sink path ──
        try:
            service_obj = service if service is not None else self.service()
        except CadenceUnavailable as exc:
            marker = write_failure_marker(
                self.campaign_root, marker="STAGING_SERVICE_UNAVAILABLE",
                error=str(exc), now=now, fire_at=fire_at)
            self._journal([f"## [{utc_iso(now)}] cadence (S7) — PASS FAILED: "
                           f"STAGING_SERVICE_UNAVAILABLE",
                           f"- error: {exc}", f"- failure marker: `{marker}`"])
            return {"outcome": "FAILED", "marker": "STAGING_SERVICE_UNAVAILABLE",
                    "error": str(exc), "marker_path": str(marker),
                    "pass_id": pass_id, "ts": utc_iso(now)}

        staged_ids: list[str] = []
        sink_skipped: Counter = Counter()
        failures: list[dict] = []
        outcome = "NOOP"
        try:
            ownership = self.ownership()
        except Exception as exc:  # noqa: BLE001 — loud, named
            ownership = None
            failures.append({"stage": "ownership",
                             "error": f"{type(exc).__name__}: {exc}"})

        if ownership is not None:
            import engraphis_sink  # campaign-root module (sanctioned writer)
            for obs in candidates:
                record = obs["record"]
                journal: list = []
                sink = engraphis_sink.EngraphisSink(
                    service_obj, workspace=self.workspace,
                    capture_sha=record.get("capture_sha"),
                    ownership=ownership, staging_state=cap_state, journal=journal)
                try:
                    result = sink.ingest_record(record)
                except engraphis_sink.ProvenanceBindingError as exc:
                    sink_skipped["provenance_binding"] += 1
                    failures.append({"stage": "sink", "record_id":
                                     record.get("record_id"),
                                     "error": f"{type(exc).__name__}: {exc}"})
                    continue
                except engraphis_sink.OwnershipAmbiguousError as exc:
                    marker = write_failure_marker(
                        self.campaign_root, marker="OWNERSHIP_AMBIGUOUS",
                        error=f"{type(exc).__name__}: {exc}", now=now,
                        fire_at=fire_at)
                    self._journal([f"## [{utc_iso(now)}] cadence (S7) — PASS "
                                   f"FAILED: OWNERSHIP_AMBIGUOUS",
                                   f"- error: {exc}",
                                   f"- failure marker: `{marker}`"])
                    return {"outcome": "FAILED", "marker": "OWNERSHIP_AMBIGUOUS",
                            "error": f"{type(exc).__name__}: {exc}",
                            "marker_path": str(marker), "pass_id": pass_id,
                            "ts": utc_iso(now)}
                except engraphis_sink.SinkError as exc:
                    sink_skipped["sink_error"] += 1
                    failures.append({"stage": "sink", "record_id":
                                     record.get("record_id"),
                                     "error": f"{type(exc).__name__}: {exc}"})
                    continue

                op = result.get("op")
                if op == "hold":
                    cap_state = {"records": result.get("state", {}).get("records"),
                                 "bytes": result.get("state", {}).get("bytes")}
                    base_window["staged"] = len(staged_ids)
                    base_window["staged_ids"] = list(staged_ids)
                    hold_receipt_outcome = "HOLD_AT_CAP"
                    return self._hold(
                        now=now, fire_at=fire_at, next_fire_at=next_fire_at,
                        missed=missed, pass_id=pass_id, signal=signal,
                        window=base_window, cap_state=cap_state,
                        cap_basis="declared", registry=registry, prior=None,
                        slot_key=slot_key,
                        forced_outcome=hold_receipt_outcome,
                        extra_skipped=dict(sink_skipped))
                if op == "ingest":
                    staged_ids.append(record.get("record_id"))
                    cap_state = {"records": sink.staging_state["records"],
                                 "bytes": sink.staging_state["bytes"]}
                elif op == "skip":
                    sink_skipped["idempotent"] += 1
                elif op == "trust_flip":
                    sink_skipped["trust_flip"] += 1

        base_window["staged"] = len(staged_ids)
        base_window["staged_ids"] = list(staged_ids)
        for key, value in sink_skipped.items():
            base_window["skipped"][key] = base_window["skipped"].get(key, 0) + value

        if failures and staged_ids:
            outcome = "PARTIAL"
        elif failures:
            outcome = "FAILED" if ownership is None else "NOOP"
        else:
            outcome = "STAGED" if staged_ids else "NOOP"

        receipt = self._make_receipt(
            pass_id=pass_id, slot_key=slot_key, now=now, fire_at=fire_at,
            next_fire_at=next_fire_at, missed=missed, signal=signal,
            window=base_window, cap_state=cap_state, cap_basis=cap_basis,
            at_cap=False, outcome=outcome, shas=shas, registry=registry,
            failures=failures)
        receipt["verdict"] = "FAILED" if outcome == "FAILED" else "PASS"
        built = self._finalize(receipt, registry, now=now)
        self._calendar_pass(receipt)
        self._demand_reviews(built["report"], pass_id=pass_id, now=now)
        self._journal(self._pass_journal(receipt, built["report"]))
        self._advance_schedule(now=now, fire_at=fire_at, next_fire_at=next_fire_at,
                               pass_id=pass_id, outcome=outcome,
                               schedule=load_schedule(self.campaign_root))
        return {"outcome": outcome, "pass_id": pass_id, "ts": utc_iso(now),
                "fire_at": fire_at, "next_fire_at": next_fire_at,
                "missed_passes": missed, "window": base_window,
                "encounter_signal": signal, "longtail": built["report"],
                "receipt": receipt, "failures": failures}

    def _finalize(self, receipt: dict, registry, *, now: float) -> dict:
        """Derive the index (receipt included), then persist receipt -> index.

        The index text is computed with the pending receipt in hand, so the bytes
        written here are exactly what a later ``rebuild_index`` derives from disk
        (the A3 byte-compare property).
        """
        built = build_index(self.campaign_root, now=now, config=self.config,
                            registry=registry,
                            receipts=load_pass_receipts(self.campaign_root) + [receipt],
                            retirements=load_retirements(self.campaign_root))
        _validate_index_rows(registry, built["rows"])
        receipt["longtail"] = built["report"]
        if not receipt.get("gate_output"):
            window = receipt.get("window") or {}
            receipt["gate_output"] = (
                f"cadence pass {receipt['pass_id']}: {receipt['outcome']} — "
                f"scanned={window.get('records_scanned')} "
                f"eligible={window.get('eligible')} staged={window.get('staged')} "
                f"longtail={built['report']['entries']} "
                f"states={built['report']['states']}")
        write_pass_receipt(self.campaign_root, receipt, registry)
        write_index(self.campaign_root, built["text"])
        return built

    # ── hold path (A11) ──

    def _hold(self, *, now: float, fire_at: float, next_fire_at: float,
              missed: int, pass_id: str, signal: dict, window: dict,
              cap_state: dict, cap_basis: str, registry, prior: Optional[dict],
              slot_key: str, forced_outcome: str = "HOLD_AT_CAP",
              extra_skipped: Optional[dict] = None) -> dict:
        skipped = dict(window.get("skipped") or {})
        skipped.update(extra_skipped or {})
        window = dict(window)
        window["skipped"] = skipped
        limits = staging_limits()
        window["staged"] = int(window.get("staged") or 0)
        if prior is not None:
            # a hold for this exact slot+cap state already exists: no second receipt
            self._advance_hold(now=now)
            return {"outcome": forced_outcome, "pass_id": pass_id,
                    "deduped": True, "ts": utc_iso(now), "fire_at": fire_at,
                    "next_fire_at": next_fire_at, "window": window,
                    "cap_state": cap_state, "cap_basis": cap_basis,
                    "drain_review_demanded": True, "receipt": prior}

        receipt = self._make_receipt(
            pass_id=pass_id, slot_key=slot_key, now=now, fire_at=fire_at,
            next_fire_at=next_fire_at, missed=missed, signal=signal,
            window=window, cap_state=cap_state, cap_basis=cap_basis,
            at_cap=True, outcome=forced_outcome, shas=window.get("ledgers") or {},
            registry=registry, failures=[])
        receipt["verdict"] = "HOLD"
        receipt["drain_review_demanded"] = True
        receipt["gate_output"] = (
            f"cadence pass {pass_id}: HOLD_AT_CAP — staging at "
            f"{cap_state.get('records')}/{limits['records']} records, "
            f"{cap_state.get('bytes')}/{limits['bytes']} bytes "
            f"(basis {cap_basis}); zero further enqueues, schedule cursor FROZEN, "
            "drain review demanded (R8/A11)")
        # The index is derived data, not a cursor: keep it consistent with the
        # receipts (a mid-pass hold may already have staged rows) WITHOUT
        # advancing the schedule. The freeze is the cursor + zero further enqueues.
        self._finalize(receipt, registry, now=now)

        ts = time.time()
        events = [
            {"kind": "staging_cap_hold", "ts": ts, "workspace": self.workspace,
             "state": cap_state, "limits": limits, "basis": cap_basis,
             "pass_id": pass_id, "fire_at": fire_at,
             "unseen_rows_held": int(window.get("eligible") or 0),
             "via": "cadence pass (R8/A11)"},
            {"kind": "drain_review_demanded", "ts": ts,
             "workspace": self.workspace, "via": "r8_cap", "pass_id": pass_id,
             "demand": "drain review — staging drains ONLY via gate review (R8); "
                       "the cadence pass HOLDS, never spills and never skips"},
        ]
        for event in events:
            mev_io.append_jsonl(calendar_path(self.campaign_root), event)
        self._journal([
            f"## [{utc_iso(now)}] cadence (S7) — STAGING CAP HOLD (R8/A11)",
            f"- pass_id: `{pass_id}` fire_at={fire_at} (schedule cursor FROZEN)",
            f"- staging_state: records={cap_state.get('records')} "
            f"bytes={cap_state.get('bytes')} limits={limits['records']} "
            f"records / {limits['bytes']} bytes basis={cap_basis}",
            "- drain review DEMANDED — staging drains ONLY via gate review;"
            " zero enqueues, never spills, never skips",
        ])
        self._advance_hold(now=now)
        return {"outcome": forced_outcome, "pass_id": pass_id,
                "ts": utc_iso(now), "fire_at": fire_at,
                "next_fire_at": next_fire_at, "window": window,
                "cap_state": cap_state, "cap_basis": cap_basis,
                "drain_review_demanded": True, "receipt": receipt,
                "schedule_frozen": True}

    # ── receipt / schedule / event helpers ──

    def _slot_key(self, *, fire_at: float, interval: int, shas: dict,
                  signal: dict) -> str:
        """Dedup identity of a pass SLOT: scheduled time + inputs + thresholds.

        Deliberately excludes cap state: a crash between the receipt write and the
        schedule write re-runs the same slot, sees this key, and dedups instead of
        double-staging. Cap state enters the pass id, so a HOLD (which must be
        retried after the drain review) is a different id and never dedups.
        """
        basis = {
            "fire_at": round(float(fire_at), 6),
            "interval_s": int(interval),
            "ledgers": dict(sorted(shas.items())),
            "encounter_digest": signal.get("digest"),
            "thresholds": {k: self.config[k] for k in
                           ("aging_after_days", "retention_days",
                            "rise_encounters")},
        }
        return "slot_" + hashlib.sha256(_canonical(basis).encode("utf-8")).hexdigest()[:16]

    def _pass_id(self, *, slot_key: str, cap_state: dict, at_cap: bool,
                 cap_basis: str) -> str:
        basis = {
            "slot": slot_key,
            "cap": {"records": cap_state.get("records"),
                    "bytes": cap_state.get("bytes"),
                    "at_cap": bool(at_cap), "basis": cap_basis},
        }
        return "pass_" + hashlib.sha256(_canonical(basis).encode("utf-8")).hexdigest()[:16]

    def _make_receipt(self, *, pass_id, slot_key, now, fire_at, next_fire_at,
                      missed, signal, window, cap_state, cap_basis, at_cap,
                      outcome, shas, registry, failures) -> dict:
        receipt = {
            "pass_id": pass_id,
            "slot_key": slot_key,
            "stage": STAGE,
            "gate_cmd": (f"micro_eviction.cadence.CadenceScheduler.run_pass "
                         f"(interval={self.config['interval_s']}s)"),
            "verdict": "PASS",
            "ts": utc_iso(now),
            "upstream_shas": {
                "registry": (registry_mod.structural_hash(registry.forms())
                             if registry is not None else None),
                "ledgers": dict(sorted((shas or {}).items())),
                "schedule": (sha256_12(schedule_path(self.campaign_root).read_bytes())
                             if schedule_path(self.campaign_root).is_file()
                             else None),
            },
            "gate_output": "",
            "outcome": outcome,
            "fire_at": float(fire_at),
            "interval_s": int(self.config["interval_s"]),
            "next_fire_at": float(next_fire_at),
            "missed_passes": int(missed),
            "window": window,
            "encounter_signal": signal,
            "longtail": {},
            "cap_state": cap_state,
            "at_cap": bool(at_cap),
            "cap_basis": cap_basis,
            "seat": self.seat,
        }
        if failures:
            receipt["failures"] = failures
        return receipt

    def _advance_schedule(self, *, now: float, fire_at: float,
                          next_fire_at: float, pass_id: str, outcome: str,
                          schedule: dict) -> None:
        doc = _schedule_doc(self.config, fire_at=fire_at,
                            next_fire_at=next_fire_at, pass_id=pass_id,
                            outcome=outcome,
                            passes=int(schedule.get("passes") or 0) + 1,
                            now=now,
                            extra={k: schedule[k] for k in ("last_hold_at",)
                                   if k in schedule})
        write_schedule(self.campaign_root, doc)

    def _advance_hold(self, *, now: float) -> None:
        """Record the hold WITHOUT advancing next_fire_at (cursor freeze)."""
        schedule = load_schedule(self.campaign_root)
        interval = int(self.config["interval_s"])
        nxt = schedule.get("next_fire_at")
        fire_at = float(schedule.get("last_fire_at") or now)
        if not isinstance(nxt, (int, float)):
            nxt = fire_at + interval
        doc = _schedule_doc(self.config, fire_at=fire_at, next_fire_at=float(nxt),
                            pass_id=schedule.get("last_pass_id"),
                            outcome="HOLD_AT_CAP",
                            passes=int(schedule.get("passes") or 0), now=now,
                            extra={"last_hold_at": utc_iso(now),
                                   "hold_count": int(schedule.get("hold_count") or 0) + 1})
        write_schedule(self.campaign_root, doc)

    def _calendar_pass(self, receipt: dict) -> None:
        row = {
            "kind": "cadence_pass",
            "ts": time.time(),
            "pass_id": receipt.get("pass_id"),
            "fire_at": receipt.get("fire_at"),
            "outcome": receipt.get("outcome"),
            "interval_s": receipt.get("interval_s"),
            "staged": (receipt.get("window") or {}).get("staged"),
            "missed_passes": receipt.get("missed_passes"),
            "via": "cadence",
        }
        mev_io.append_jsonl(calendar_path(self.campaign_root), row)

    def _demand_reviews(self, report: dict, *, pass_id: str, now: float) -> None:
        ts = time.time()
        if report.get("drain_candidates"):
            mev_io.append_jsonl(calendar_path(self.campaign_root), {
                "kind": "drain_review_demanded", "ts": ts,
                "via": "retention_boundary", "pass_id": pass_id,
                "record_ids": list(report["drain_candidates"]),
                "demand": "retention boundary reached — drain candidacy is "
                          "review-gated (R8/R16); the DECAYED transition runs only "
                          "through the sanctioned staging-drain path (A13)"})
        if report.get("rising"):
            mev_io.append_jsonl(calendar_path(self.campaign_root), {
                "kind": "promotion_review_demanded", "ts": ts,
                "via": "reencounter", "pass_id": pass_id,
                "record_ids": list(report["rising"]),
                "demand": "re-encountered items rise toward promotion; promotion "
                          "remains dream-pass/Dallas authority (never autonomous)"})

    def _pass_journal(self, receipt: dict, report: dict) -> list[str]:
        window = receipt.get("window") or {}
        return [
            f"## [{receipt['ts']}] cadence (S7) — PASS {receipt['outcome']}",
            f"- pass_id: `{receipt['pass_id']}` fire_at={receipt['fire_at']} "
            f"next_fire_at={receipt['next_fire_at']} "
            f"missed={receipt.get('missed_passes')}",
            f"- window: scanned={window.get('records_scanned')} "
            f"eligible={window.get('eligible')} staged={window.get('staged')} "
            f"skipped={window.get('skipped')}",
            f"- longtail: entries={report.get('entries')} states={report.get('states')}"
            f" immune_excluded={len(report.get('excluded_immune') or [])}",
        ]

    def _journal(self, lines: list[str]) -> None:
        try:
            mev_io.allow(self.runlog.parent)
            mev_io.safe_write(self.runlog, "\n".join(lines) + "\n", append=True)
        except OSError:
            pass


# ── retire (R17: payload retained, spine + directives retained) ─────────────

def retire_entry(campaign_root, record_id: str, *, gate_review,
                 reviewer: str, now: Optional[float] = None,
                 config: Optional[dict] = None, registry=None,
                 runlog=None, seat: Optional[str] = None) -> dict:
    """Review-gated retire bookkeeping — NO deletion, NO new writer (A13/R17).

    The payload stays where it is (staging / ledger). What changes is the INDEX
    state (``retired``) plus a CALENDAR retirement event; the spine citation is
    verified byte-identically before the event is written, and the record's
    retraction directives are carried in the event so they remain queryable.
    """
    if not gate_review:
        raise RetireRefused(
            "retire is review-gated: pass the gate review evidence (R8: staging "
            "drains ONLY via gate review; never silent)")
    root = Path(campaign_root)
    now = time.time() if now is None else float(now)
    reg = registry or registry_mod.RecordsRegistry(str(root))
    _provision(reg)
    cfg = config or load_config(root)

    built = build_index(root, now=now, config=cfg, registry=reg,
                        receipts=load_pass_receipts(root),
                        retirements=load_retirements(root))
    # Verify against the CITATION OF RECORD (the persisted index), not against a
    # fresh derivation of the spine: the point of the check is that the spine the
    # estate already published still holds. Falls back to the fresh build when no
    # index exists yet.
    published = read_index(root)
    cited_rows = published if published else built["rows"]
    row = next((r for r in cited_rows if r["record_id"] == record_id), None)
    if row is None:
        raise RetireRefused(f"{record_id} is not in the long-tail index")
    if row["state"] not in (DRAIN_CANDIDATE, DECAYED):
        raise RetireRefused(
            f"{record_id} state is {row['state']!r}; retire requires a drain "
            "candidacy (drain_candidate/decayed)")

    spine_ref = row.get("spine_ref") or {}
    ledger_rel = spine_ref.get("ledger")
    if ledger_rel:
        ledger = root / ledger_rel
        if not ledger.is_file():
            raise SpineVerificationFailed(f"ledger missing: {ledger}")
        line_no = int(spine_ref.get("line") or 0)
        if line_no < 1:
            raise SpineVerificationFailed("spine_ref carries no line number")
        lines = ledger.read_text(encoding="utf-8").splitlines()
        if line_no > len(lines):
            raise SpineVerificationFailed(
                f"ledger line {line_no} beyond EOF ({len(lines)})")
        try:
            cited = json.loads(lines[line_no - 1])
        except ValueError as exc:
            raise SpineVerificationFailed(f"cited line is not JSON: {exc}") from None
        if cited.get("record_id") != record_id:
            raise SpineVerificationFailed(
                f"cited line no longer carries {record_id} "
                f"(found {cited.get('record_id')!r})")
    spine_rel = spine_ref.get("spine")
    if spine_rel:
        spine = root / spine_rel
        if not spine.is_file():
            raise SpineVerificationFailed(f"spine missing: {spine}")
        actual = sha256_12(spine.read_bytes())
        if actual != spine_ref.get("spine_sha256_12"):
            raise SpineVerificationFailed(
                f"spine {spine_rel} sha256_12 {actual} != indexed "
                f"{spine_ref.get('spine_sha256_12')}")

    directives_path = dirmod.derive_directives_path(root / "calcified.jsonl")
    directives = [d for d in dirmod.load_directives(directives_path)
                  if record_id in set(d.get("target_record_ids") or [])]

    event = {
        "kind": "retire",
        "ts": utc_iso(now),
        "record_id": record_id,
        "state_before": row["state"],
        "reviewer": reviewer,
        "gate_review": (gate_review if isinstance(gate_review, str)
                        else str(gate_review)),
        "pass_basis": {"fire_at": now},
        "spine_sha256_12": spine_ref.get("spine_sha256_12"),
        "directives": [d.get("directive_id") for d in directives],
        "payload": "retained (staging + session ledger untouched — R17/A13)",
        "via": "cadence retire (review-gated)",
    }
    mev_io.append_jsonl(calendar_path(root), event)

    journal = Path(runlog) if runlog else runlog_path(root)
    try:
        mev_io.allow(journal.parent)
        mev_io.safe_write(journal, "\n".join([
            f"## [{utc_iso(now)}] cadence (S7) — RETIRE {record_id}",
            f"- reviewer: {reviewer} gate_review={event['gate_review']}",
            f"- spine: {spine_ref.get('spine')} "
            f"sha256_12={spine_ref.get('spine_sha256_12')} (verified)",
            f"- directives retained: {event['directives']}",
            "- payload RETAINED: no delete, no move, no new writer (A13/R17)",
        ]) + "\n", append=True)
    except OSError:
        pass

    report = rebuild_index(root, now=now, config=cfg, registry=reg)
    return {"retired": True, "record_id": record_id, "event": event,
            "state": RETIRED, "longtail": report, "path": str(calendar_path(root))}


# ── CLI (the gate command) ──────────────────────────────────────────────────

def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser(
        description="micro-eviction entropy cadence (S7): one receipt-bearing pass.")
    ap.add_argument("--campaign-root", default=str(DEFAULT_CAMPAIGN_ROOT))
    ap.add_argument("--interval", type=float, default=None,
                    help="interval seconds (config-driven; default schedule/env)")
    ap.add_argument("--force", action="store_true", help="fire an off-slot pass")
    ap.add_argument("--dry-run", action="store_true",
                    help="detect + report; write nothing")
    ap.add_argument("--tick", action="store_true",
                    help="report due/next-fire only (no pass)")
    ap.add_argument("--staging-state", default=None,
                    help='JSON {"records":N,"bytes":B} (omitted = fail closed HOLD)')
    ap.add_argument("--rebuild-index", action="store_true",
                    help="rebuild the long-tail index from receipts and compare")
    args = ap.parse_args(argv)

    scheduler = CadenceScheduler(args.campaign_root, interval_s=args.interval)
    if args.tick:
        print(json.dumps(scheduler.tick(), indent=1, sort_keys=True))
        return 0
    if args.rebuild_index:
        print(json.dumps(rebuild_index(args.campaign_root), indent=1, sort_keys=True))
        return 0
    state = json.loads(args.staging_state) if args.staging_state else None
    result = scheduler.run_pass(force=args.force, dry_run=args.dry_run,
                                staging_state=state)
    print(json.dumps({k: v for k, v in result.items() if k != "receipt"},
                     indent=1, sort_keys=True, default=str))
    return 0 if result.get("outcome") not in ("FAILED",) else 1


__all__ = [
    "SEAT_ID", "SEAT_STAGE", "DEFAULT_CAMPAIGN_ROOT", "DAY_S",
    "DEFAULT_INTERVAL_S", "INTERVAL_ENV", "LONGTAIL_DIR", "INDEX_NAME",
    "SCHEDULE_NAME", "CALENDAR_NAME", "RECEIPTS_REL", "LOCK_RELPATH",
    "STATES", "OUTCOMES", "DEDUP_OUTCOMES", "DEFAULT_CONFIG",
    "CadenceError", "DriverLockBusy", "SingleInstanceRefused",
    "RegistryUnavailable", "CadenceUnavailable", "RetireRefused",
    "SpineVerificationFailed",
    "longtail_dir", "index_path", "schedule_path", "calendar_path",
    "receipts_dir", "failed_receipts_dir", "lock_path", "runlog_path",
    "driver_lock", "read_lock_holder", "DriverLock",
    "default_config", "load_config", "load_schedule", "write_schedule",
    "scan_capture_surface", "ledger_shas", "normalize_encounter_signal",
    "derive_state", "build_index", "rebuild_index", "read_index",
    "load_pass_receipts", "load_pass_receipt", "write_pass_receipt",
    "write_failure_marker", "pass_receipt_path", "staging_limits",
    "open_staging_readonly", "CadenceScheduler", "retire_entry", "main",
]


if __name__ == "__main__":
    sys.exit(main())
