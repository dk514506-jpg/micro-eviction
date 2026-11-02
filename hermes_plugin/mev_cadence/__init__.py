"""mev-cadence — Hermes plugin: micro-eviction every-turn cadence.

CHARGE-MEV-PLUGIN-CADENCE-2026-10-30 (orchestrator Silvey; builder: cmd seat).
Authority: Dallas rulings 2026-10-28 items 3-4 (RUN-LOG) and the 2026-10-30
clarify rulings — attach through a Hermes plugin on ``post_llm_call`` (every
turn, NO memory provider), not the design §3 memory-provider seam; 75k state
records = one summary entry per checkpoint appended to the knowledge-vault
``log.md``, full record stays in the seat-state folder. Design of record:
``campaigns/bench/micro-eviction/DESIGN-VAULT-ENGRAPHIS-MEMOS-2026-10-28.md``
§2 (staging workspace ``mev-staging``) and §3 (cadence + slicing).

Duties (one ``post_llm_call`` observer, every turn):

1. Cadence gate — fire only at turn ordinal ``T >= CADENCE_START_TURN`` (6),
   imported from ``micro_eviction.vault_hook`` (single source). Turns 1-5 are
   no-ops with the reason named.
2. Slice — delta since the last processed boundary through the latest message,
   plus the compaction-exit slice when compaction fired since the last
   boundary. The frozen rule (``micro_eviction.capture.extract_slices``) is
   REUSED VERBATIM through ``micro_eviction.vault_hook.VaultHook`` — never
   re-implemented here. The Hermes session store (state.db, opened read-only)
   is the source of truth for ``active``/``compacted``/``csum`` row flags.
3. Stage — the existing sink path (``engraphis_sink.EngraphisSink``) into
   workspace ``mev-staging`` ONLY (pinned constant, never a setting). Never
   any other workspace; never MemOS in this charge.
4. 75k state mirror (Task A, CHARGE-MEV-PLUGIN-CADENCE-FOLLOWUP-2026-10-30) —
   one dated summary entry per checkpoint appended to the knowledge-vault
   ``log.md`` in the followup charge's exact format (``## [YYYY-MM-DD] state |
   <seat-label> checkpoint NN`` + measured input tokens / model id / session
   id / disposition-if-known / relative record link); the full record stays in
   ``log/seat-state/``. Append-only, idempotent per seat-label + NN.
5. Desktop-session 75k state notes (Task B, same charge) — at each 75k of
   MEASURED growth in the pinned desktop Hermes session
   (``session_model_usage`` SUM(input_tokens), DESIGN-75K-ADAPTER §2.1
   cumulative provider basis), write the cmd-seat-schema STATE-NN note +
   INDEX.md row into ``log/seat-state/<desktop-seat>/`` plus the Task A
   ``log.md`` entry. The ``post_llm_call`` payload carries NO token field
   (turn_finalizer.py:469-479 kwargs) — the session store is the only
   measured source; no usage rows => STOP with the missing field named,
   never an estimate labeled as measured.
6. Off-switch — ``MEV_VAULT_HOOK_OFF`` (1/true/yes/on) disables with zero
   writes (same env and semantics as ``vault_hook.off_switch_engaged``).
7. Fail closed on vault errors; NEVER raise into the host turn pipeline.
   ``post_llm_call`` is timeout-bounded (30 s, fail-open on timeout —
   hermes_cli/plugins_dispatch.py), so this callback stays mechanical-fast by
   default and wraps each duty in its own try/except. A Task B write error is
   recorded (fire log + INDEX cursor ``last_error``), never raised.

Build-and-test artifact: NOT installed into ``~/.hermes/plugins/`` (install is
the orchestrator's step after mechanic GREEN). The Engraphis writer is a
constructor seam (``service_factory``) — the program's never-construct-a-writer
discipline (cadence.py); unbound => capture-only with staging "not-bound".
Live activation binds the sanctioned ``MemoryService`` (engraphis venv runtime).
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import sqlite3
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

PLUGIN_NAME = "mev-cadence"
PLUGIN_VERSION = "1.1.0"

# Design §2 / charge: the staging workspace is PINNED. It is deliberately not
# a setting — no config path can redirect staging to another workspace.
STAGING_WORKSPACE = "mev-staging"

OFF_SWITCH_ENV = "MEV_VAULT_HOOK_OFF"   # single source: micro_eviction.vault_hook
OFF_VALUES = ("1", "true", "yes", "on")

# ── Task B desktop duty (CHARGE-MEV-PLUGIN-CADENCE-FOLLOWUP-2026-10-30) ──────
# Interval: the shared seat75k module (N-2 single source), imported from the
# roof tools dir; the plugin constant is only the named fallback when the
# tools dir is unreachable (the basis is recorded in every fire event).
DESK_INTERVAL_FALLBACK = 75_000
DESK_SANDBOX_PATH = "desktop-hermes-session"  # honest value: no sandbox exists
DESK_INDEX_HEADER = (
    "# Seat state records — index\n\n"
    "| checkpoint | last_measured_inputTokens | threshold | cumulative_growth | "
    "model_id | session_uuid | ledger_path | sandbox_path | ledger_sha256_12 | "
    "note | captured |\n"
    "|---|---|---|---|---|---|---|---|---|---|---|\n"
)

# 75k seat-state notes (seat-context-watchdog.serialize_to_obsidian form).
STATE_NOTE_RE = re.compile(r"^STATE-(\d+)-(\d+)t-(\d{8}T\d{9,})-([0-9a-f]{8})\.md$")

_DEFAULT_CAMPAIGN_ROOT = Path(os.environ.get("MEV_CAMPAIGN_ROOT", str(Path.home() / ".mev-cadence")))
_DEFAULT_OBSIDIAN_ROOT = Path(os.environ.get("MEV_SEAT_STATE_ROOT", str(Path.home() / ".mev-cadence" / "seat-state")))
_DEFAULT_LOG_PATH = Path.home() / "icm-knowledge-vault" / "log.md"

_SETTING_KEYS = (
    "campaign_root", "db_path", "ledger_dir", "seat_label", "seat_state_root",
    "log_path", "engraphis_db", "t2_llm", "desktop_seat_label",
    "desktop_session_id", "tools_dir", "charge_path", "floor_theta",
)


def _env_bool(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in OFF_VALUES


def off_switch_engaged() -> bool:
    """Zero-import fast path; ``vault_hook.off_switch_engaged`` (same env, same
    values) is re-checked inside VaultHook as the backstop."""
    return os.environ.get(OFF_SWITCH_ENV, "").strip().lower() in OFF_VALUES


def _fail_closed(reason: str) -> dict:
    return {"ok": False, "event": "fail-closed",
            "reason": f"fail-closed: {reason}", "fired": False, "evicted": False}


# ── seat-state frontmatter (canonical parser shape: tools/
#    check-seat-state-records.py parse_frontmatter) ──────────────────────────

def parse_frontmatter(text: str) -> dict:
    d: dict = {}
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return d
    for ln in lines[1:]:
        if ln.strip() == "---":
            break
        if ": " in ln:
            key, val = ln.split(": ", 1)
            val = val.strip()
            if val.startswith('"') or val.startswith("'"):
                try:
                    val = json.loads(val)
                except Exception:
                    val = val.strip("\"'")
            d[key] = val
    return d


def _as_int(value, fallback) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    try:
        return int(fallback)
    except (TypeError, ValueError):
        return 0


def _entry_date(fm: dict, note: Path) -> str:
    created = str(fm.get("created") or "")
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", created)
    if m:
        return m.group(1)
    fname = STATE_NOTE_RE.match(note.name)
    if fname:  # stamp form 20261030T183957498590
        stamp = fname.group(3)
        return f"{stamp[0:4]}-{stamp[4:6]}-{stamp[6:8]}"
    try:
        return time.strftime("%Y-%m-%d", time.gmtime(note.stat().st_mtime))
    except OSError:
        return "unknown"


def render_state_entry(*, date: str, seat: str, checkpoint: int,
                       tokens, model: str, session: str, note: Path,
                       disposition=None) -> str:
    """One dated summary entry per checkpoint — Task A format, EXACTLY per
    CHARGE-MEV-PLUGIN-CADENCE-FOLLOWUP-2026-10-30: heading
    ``## [YYYY-MM-DD] state | <seat-label> checkpoint NN`` then bullet lines
    (measured input tokens; model id; session id; disposition if known;
    relative link to the full record)."""
    lines = [
        f"## [{date}] state | {seat} checkpoint {checkpoint:02d}",
        f"- measured_input_tokens: {tokens}",
        f"- model_id: {model}",
        f"- session_id: {session}",
    ]
    if disposition:
        lines.append(f"- disposition: {disposition}")
    stem = note.name[:-3] if note.name.endswith(".md") else note.name
    lines.append(f"- record: [[log/seat-state/{seat}/{stem}]]")
    return "\n".join(lines) + "\n\n"


# ── Task B: desktop-session measured usage (FOLLOWUP charge) ─────────────────

def desk_connect_ro(db_path):
    """Read-only store connection — seat75k_adapter.py §3.1 exact discipline
    (URI form + query_only + busy_timeout). Never a writer."""
    uri = f"file:{Path(db_path).resolve()}?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=5.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only = ON")
    con.execute("PRAGMA busy_timeout = 5000")
    return con


def desk_session_usage(con, session_id):
    """(measured cumulative input tokens, rows) — DESIGN-75K-ADAPTER §2.1:
    ONE SUM over ALL of the session's session_model_usage rows (every task);
    the cumulative provider basis, never a context-window reading. Rows are
    ordered by last_seen so the latest measured model is the last row."""
    rows = [dict(r) for r in con.execute(
        "SELECT session_id, model, task, api_call_count, input_tokens,"
        " output_tokens, cache_read_tokens, cache_write_tokens,"
        " reasoning_tokens, estimated_cost_usd, first_seen, last_seen"
        " FROM session_model_usage WHERE session_id=?"
        " ORDER BY last_seen, rowid", (session_id,))]
    total = sum(int(r.get("input_tokens") or 0) for r in rows)
    return total, rows


def desk_burn(rows) -> int:
    """burn_of() semantics (seat-context-watchdog.py): input + output +
    cache_read + cache_write over the session's usage rows."""
    return sum(int(r.get("input_tokens") or 0) + int(r.get("output_tokens") or 0)
               + int(r.get("cache_read_tokens") or 0)
               + int(r.get("cache_write_tokens") or 0) for r in rows)


def _yamlq(v) -> str:
    """Frontmatter scalar render (watchdog yamlq discipline; None renders as
    the literal None, matching serialize_to_obsidian's compaction fields)."""
    if v is None:
        return "None"
    if isinstance(v, bool):
        return "True" if v else "False"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, (list, dict)):
        return json.dumps(v, sort_keys=True)
    s = str(v)
    if s.startswith("[") or s.startswith("{"):
        return s  # already flow-style
    if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
        return s  # bare numeric cursor field (watchdog frontmatter style)
    return json.dumps(s)


def _render_frontmatter(fm: dict) -> str:
    lines = ["---"]
    for k, v in fm.items():
        lines.append(f"{k}: {_yamlq(v)}")
    lines.append("---")
    return "\n".join(lines)


def _split_frontmatter(text: str):
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---\n", 4)
    if end == -1:
        return {}, text
    return parse_frontmatter(text), text[end + 5:]


def desk_read_cursor(index: Path) -> dict:
    """The desktop duty's durable cursor lives in the seat INDEX.md frontmatter
    (the charge's sanctioned write set): anchor/pin/checkpoint basis."""
    if not index.exists():
        return {}
    try:
        fm, _ = _split_frontmatter(index.read_text(encoding="utf-8"))
        return fm or {}
    except OSError:
        return {}


def desk_write_cursor(index: Path, seat: str, updates: dict) -> None:
    """Refresh the cursor frontmatter under flock (watchdog index_append
    discipline); existing checkpoint rows are never rewritten."""
    index.parent.mkdir(parents=True, exist_ok=True)
    with open(index, "a+") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        try:
            f.seek(0)
            txt = f.read()
            fm, body = _split_frontmatter(txt)
            stamp = updates.pop("stamp", "")
            fm.update(updates)
            fm.setdefault("title", f"Seat state records — {seat}")
            fm.setdefault("type", "seat-state-index")
            if not fm.get("created"):
                fm["created"] = stamp
            if stamp:
                fm["updated"] = stamp
            fm.setdefault("tags", f'["seat-state-index", "{seat}"]')
            if not body.strip():
                body = "\n" + DESK_INDEX_HEADER
            f.seek(0)
            f.truncate()
            f.write(_render_frontmatter(fm) + "\n" + body)
        finally:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)


# ── staging sink seam (VaultHook sink -> the existing EngraphisSink path) ───

class StagingSink:
    """Adapter from the VaultHook sink seam to the existing sink path.

    One ``EngraphisSink`` per record (the sink binds a single ``capture_sha``;
    records carry their own). Workspace is PINNED to ``mev-staging``.
    Ownership and staging-load accounting come from the program's own
    referents unless tests/activation inject them:
      * ownership — ``memos_push.resolve_induction_ownership`` (disk scan of
        deployed referents; NO MemOS contact);
      * staging state — ``staging_provider.recount_state`` (the sealed
        record/byte derivation via ``EngraphisSink.recount_staging``; no sha
        basis — a full-store sha cannot fit the bounded every-turn hook).
    """

    def __init__(self, service, *, engraphis_sink_module, ownership_fn=None,
                 staging_state_fn=None, workspace: str = STAGING_WORKSPACE,
                 store_path=None):
        self.service = service
        self._sink_mod = engraphis_sink_module
        self.ownership_fn = ownership_fn
        self.staging_state_fn = staging_state_fn
        self.workspace = workspace
        self.store_path = store_path
        self.basis = "injected" if staging_state_fn is not None else "recount_state"

    def _staging_state(self) -> dict:
        if self.staging_state_fn is not None:
            return self.staging_state_fn()
        import staging_provider  # campaign root on sys.path (bootstrap)
        return staging_provider.recount_state(self.store_path)

    def _ownership(self) -> dict:
        if self.ownership_fn is not None:
            return self.ownership_fn()
        import memos_push  # campaign root on sys.path (bootstrap)
        return memos_push.resolve_induction_ownership()

    def ingest_record(self, record: dict, *, trusted: bool = False) -> dict:
        sink = self._sink_mod.EngraphisSink(
            self.service, workspace=self.workspace,
            capture_sha=record.get("capture_sha"),
            ownership=self._ownership(),
            staging_state=self._staging_state())
        return sink.ingest_record(record, trusted=trusted)


# ── the plugin ──────────────────────────────────────────────────────────────

class MevCadencePlugin:
    """Every-turn micro-eviction cadence + 75k state mirror + desktop notes.

    Constructor is IO-free (the 10 s plugin load timeout must never see a
    campaign import or a store open). Everything heavy is lazy at first fire.
    """

    def __init__(self, *, campaign_root=None, db_path=None, ledger_dir=None,
                 seat_label=None, seat_state_root=None, log_path=None,
                 engraphis_db=None, service_factory=None, ownership_fn=None,
                 staging_state_fn=None, llm_fn=None, floor_theta=None,
                 charge_path=None, t2_llm=False, now_fn=None,
                 desktop_seat_label=None, desktop_session_id=None,
                 tools_dir=None):
        self.campaign_root = Path(
            campaign_root or os.environ.get("MEV_CAMPAIGN_ROOT")
            or _DEFAULT_CAMPAIGN_ROOT)
        self.db_path = db_path or os.environ.get("MEV_HERMES_DB") or self._default_db_path()
        self.ledger_dir = Path(
            ledger_dir or os.environ.get("MEV_LEDGER_DIR")
            or self.campaign_root / "ledgers")
        self.seat_label = (seat_label if seat_label is not None
                           else (os.environ.get("MEV_SEAT_LABEL") or None))
        self.seat_state_root = Path(
            seat_state_root or os.environ.get("MEV_SEAT_STATE_ROOT")
            or _DEFAULT_OBSIDIAN_ROOT)
        self.log_path = Path(
            log_path or os.environ.get("MEV_STATE_LOG_PATH") or _DEFAULT_LOG_PATH)
        self.engraphis_db = engraphis_db or os.environ.get("ENGRAPHIS_LIVE_DB") or None
        # Seams (tests + activation stage inject; config cannot — callables are
        # never settings). service_factory None => capture-only (fail-soft).
        self.service_factory = service_factory
        self.ownership_fn = ownership_fn
        self.staging_state_fn = staging_state_fn
        self.llm_fn = llm_fn
        self.floor_theta = floor_theta
        self.charge_path = charge_path
        self.t2_llm = bool(t2_llm) or _env_bool("MEV_T2_LLM")
        # Task B desktop duty (FOLLOWUP charge): inert until a desktop seat
        # label is configured — the same posture as the mirror's seat_label.
        self.desktop_seat_label = (desktop_seat_label if desktop_seat_label is not None
                                   else (os.environ.get("MEV_DESKTOP_SEAT_LABEL") or None))
        self.desktop_session_id = (desktop_session_id if desktop_session_id is not None
                                   else (os.environ.get("MEV_DESKTOP_SESSION_ID") or None))
        self.tools_dir = Path(
            tools_dir or os.environ.get("MEV_TOOLS_DIR")
            or (Path.home() / "Desktop" / "roof" / "tools"))
        self.now_fn = now_fn or time.time
        self._lock = threading.RLock()  # reentrant: _get_hook -> _mods nesting
        self._mods_ns = None   # cached campaign module namespace (method _mods)
        self._hook = None
        self._staging_status = "not-bound"
        self._staging_basis = "none"
        self._desk_interval_cache = None

    @staticmethod
    def _default_db_path() -> str:
        home = os.environ.get("HERMES_HOME") or str(Path.home() / ".hermes")
        return str(Path(home) / "state.db")

    # ── lazy campaign bootstrap (single sys.path insertion, cached) ────────

    def _bootstrap(self) -> dict:
        root = str(self.campaign_root.resolve())
        if root not in sys.path:
            sys.path.insert(0, root)
        import micro_eviction.vault_hook as vh
        import micro_eviction.source as source
        import micro_eviction.io as mev_io
        import engraphis_sink
        return {"vh": vh, "source": source, "io": mev_io,
                "engraphis_sink": engraphis_sink}

    def _mods(self) -> dict:
        with self._lock:
            if self._mods_ns is None:
                self._mods_ns = self._bootstrap()
            return self._mods_ns

    # ── the cadence hook (VaultHook) + staging seam ────────────────────────

    def _calibrated_floor(self) -> float:
        try:
            calib = self.campaign_root / "reports" / "t2_calibration.json"
            data = json.loads(calib.read_text(encoding="utf-8"))
            return float(data.get("floor_theta", 1.0))
        except Exception:
            return 1.0

    def _activation_factory(self):
        """T1 D-G4 dormant-seam fallback (2026-11-01): build the service
        factory via mev_activation (campaign root on sys.path via bootstrap).
        Returns None on ANY failure -> stay not-bound, fail-soft capture-only.
        NEVER raises into the fire path."""
        try:
            import mev_activation  # type: ignore[import-not-found]
            return mev_activation.make_service_factory()
        except Exception:
            return None

    def _build_sink(self, mods: dict):
        if self.service_factory is None:
            factory = self._activation_factory()
            if factory is None:
                return None, "not-bound", "none"
            self.service_factory = factory
        try:
            service = self.service_factory()
        except Exception as exc:  # fail closed: capture-only, named reason
            return None, f"unavailable:{type(exc).__name__}:{exc}", "none"
        if service is None:
            return None, "not-bound", "none"
        sink = StagingSink(
            service, engraphis_sink_module=mods["engraphis_sink"],
            ownership_fn=self.ownership_fn, staging_state_fn=self.staging_state_fn,
            store_path=self.engraphis_db)
        basis = sink.basis
        return sink, "active", basis

    def _get_hook(self):
        with self._lock:
            if self._hook is not None:
                return self._hook
            mods = self._mods()
            sink, staging_status, staging_basis = self._build_sink(mods)
            self._staging_status = staging_status
            self._staging_basis = staging_basis
            llm_fn = self.llm_fn
            if llm_fn is None and self.t2_llm:
                try:
                    import micro_eviction.t2_classifier as t2
                    llm_fn = t2.default_llm()
                except Exception:
                    llm_fn = None  # mechanical-first posture stands
            floor = self.floor_theta
            if floor is None:
                floor = self._calibrated_floor()
            self._hook = mods["vh"].VaultHook(
                ledger_dir=str(self.ledger_dir), db_path=self.db_path,
                enabled=True, floor_theta=floor, llm_fn=llm_fn, sink=sink,
                charge_path=self.charge_path, now_fn=self.now_fn)
            return self._hook

    # ── 75k seat-state mirror (charge item 6) ──────────────────────────────

    def _mirror_state(self) -> dict:
        if not self.seat_label:
            return {"event": "noop", "reason": "no-seat-configured"}
        seat_dir = self.seat_state_root / "log" / "seat-state" / self.seat_label
        if not seat_dir.is_dir():
            return {"event": "noop", "reason": "no-seat-state-dir",
                    "seat_state_dir": str(seat_dir)}
        notes = []
        for p in seat_dir.glob("STATE-*.md"):
            m = STATE_NOTE_RE.match(p.name)
            if m:
                notes.append((int(m.group(1)), p, m))
        if not notes:
            return {"event": "noop", "reason": "no-state-notes",
                    "seat_state_dir": str(seat_dir)}
        notes.sort(key=lambda t: t[0])
        log_path = self.log_path
        existing = (log_path.read_text(encoding="utf-8")
                    if log_path.exists() else "")
        mods = self._mods()
        entries, appended = [], []
        for _, note, m in notes:
            if note.name in existing:  # idempotent: note already summarized
                continue
            fm = parse_frontmatter(note.read_text(encoding="utf-8"))
            cp = _as_int(fm.get("checkpoint"), m.group(1))
            session = str(fm.get("session_uuid") or "unknown")
            heading_sig = f"state | {self.seat_label} checkpoint {cp:02d}"
            if heading_sig in existing:  # re-serialized note, same checkpoint
                continue
            entries.append(render_state_entry(
                date=_entry_date(fm, note), seat=self.seat_label,
                session=session, checkpoint=cp,
                tokens=fm.get("last_measured_inputTokens", "unknown"),
                model=fm.get("model_id", "unknown"), note=note,
                disposition=fm.get("disposition")))
            appended.append({"checkpoint": cp, "note": note.name,
                             "session_uuid": session})
        if entries:
            block = "".join(entries)
            if existing and not existing.endswith("\n\n"):
                block = "\n" + block  # blank-line separation, vault style
            mods["io"].allow(str(log_path.parent))
            mods["io"].safe_write(str(log_path), block, append=True)
        return {"event": "state-mirror", "appended": len(appended),
                "checkpoints": appended, "log_path": str(log_path)}

    # ── Task B: desktop-session 75k state notes (FOLLOWUP charge) ───────────

    def _desk_interval(self) -> tuple:
        """INTERVAL from the shared seat75k module (N-2 single source); the
        plugin constant is the named fallback when the tools dir is absent."""
        if self._desk_interval_cache is not None:
            return self._desk_interval_cache
        val, basis = DESK_INTERVAL_FALLBACK, "plugin-constant"
        try:
            tools = str(self.tools_dir.resolve())
            if tools not in sys.path:
                sys.path.insert(0, tools)
            import seat75k
            val = int(seat75k.INTERVAL)
            basis = "seat75k"
        except Exception:
            pass
        self._desk_interval_cache = (val, basis)
        return self._desk_interval_cache

    def _desk_crossing(self, *, seat_dir, index, seat, m, total, growth,
                       growth_since, anchor, interval, sid, model, api_calls,
                       burn, rows):
        """One crossing: cmd-schema STATE note (chmod 0444) + INDEX row + the
        Task A log.md entry. Idempotent per checkpoint; every sub-write is
        fail-closed with the error named in the returned record."""
        stamp = datetime.fromtimestamp(self.now_fn(), timezone.utc)
        stamp_iso = stamp.strftime("%Y-%m-%dT%H:%M:%S%z")
        stamp_compact = stamp.strftime("%Y%m%dT%H%M%S%f")
        date = stamp.strftime("%Y-%m-%d")
        heading = f"## [{date}] state | {seat} checkpoint {m:02d}"
        snap = {"store_type": "hermes", "session_id": sid,
                "captured_at": stamp_iso, "anchor_reading": int(anchor),
                "measured_input_tokens": int(total), "growth": int(growth),
                "rows": [{k: r.get(k) for k in (
                    "model", "task", "api_call_count", "input_tokens",
                    "output_tokens", "cache_read_tokens", "cache_write_tokens",
                    "reasoning_tokens", "estimated_cost_usd", "first_seen",
                    "last_seen")} for r in rows]}
        snap_bytes = (json.dumps(snap, sort_keys=True) + "\n").encode("utf-8")
        snap_sha = hashlib.sha256(snap_bytes).hexdigest()[:12]
        result = {"checkpoint": m, "heading": heading, "note": None,
                  "index_row": False, "log_entry": False, "errors": []}
        seat_dir.mkdir(parents=True, exist_ok=True)
        existing = sorted(seat_dir.glob(f"STATE-{m:02d}-*.md"))
        note = existing[0] if existing else None
        if note is None:
            name = (f"STATE-{m:02d}-{total}t-{stamp_compact}-"
                    f"{uuid.uuid4().hex[:8]}.md")
            note = seat_dir / name
            try:
                note.write_text(self._desk_note_text(
                    seat=seat, m=m, measured=total, growth=growth,
                    growth_since=growth_since, anchor=anchor, interval=interval,
                    sid=sid, model=model, api_calls=api_calls, burn=burn,
                    snap_sha=snap_sha, snap_obj=snap, heading=heading,
                    stamp_iso=stamp_iso), encoding="utf-8")
                os.chmod(note, 0o444)  # watchdog notes stay read-only (R-B)
            except Exception as exc:
                result["errors"].append(f"note: {type(exc).__name__}: {exc}")
                return result  # no note => no index row / log entry this fire
        result["note"] = str(note)
        try:
            result["index_row"] = self._desk_index_append(
                index, seat=seat, m=m, measured=total, interval=interval,
                growth=growth, model=model, sid=sid, sha=snap_sha, note=note,
                stamp_iso=stamp_iso)
        except Exception as exc:
            result["errors"].append(f"index: {type(exc).__name__}: {exc}")
        try:
            mods = self._mods()
            log_text = (self.log_path.read_text(encoding="utf-8")
                        if self.log_path.exists() else "")
            if heading not in log_text:
                entry = render_state_entry(
                    date=date, seat=seat, checkpoint=m, tokens=total,
                    model=model, session=sid, note=note)
                block = entry
                if log_text and not log_text.endswith("\n\n"):
                    block = "\n" + entry  # blank-line separation, vault style
                mods["io"].allow(str(self.log_path.parent))
                mods["io"].safe_write(str(self.log_path), block, append=True)
                result["log_entry"] = True
        except Exception as exc:
            result["errors"].append(f"log: {type(exc).__name__}: {exc}")
        return result

    def _desk_note_text(self, *, seat, m, measured, growth, growth_since,
                        anchor, interval, sid, model, api_calls, burn,
                        snap_sha, snap_obj, heading, stamp_iso) -> str:
        """Cmd-seat schema (serialize_to_obsidian field set) with the desktop
        Hermes values; Task B.3 extras: growth since last checkpoint and the
        Task A log.md entry this crossing produced."""
        fm = [
            "---",
            f"title: {json.dumps(f'Seat state record {seat} #{m:02d}')}",
            "type: seat-state-record",
            f"created: {json.dumps(stamp_iso)}",
            f"seat: {json.dumps(seat)}",
            f"checkpoint: {m}",
            f"session_uuid: {json.dumps(sid)}",
            f"ledger_path: {json.dumps(str(self.db_path))}",
            f"sandbox_path: {json.dumps(DESK_SANDBOX_PATH)}",
            f"model_id: {json.dumps(model)}",
            f"threshold: {interval}",
            f"last_measured_inputTokens: {int(measured)}",
            f"cumulative_growth_at_crossing: {int(growth)}",
            f"ledger_sha256_12: {json.dumps(snap_sha)}",
            "compaction_count: None",
            "compaction_tokens_saved: None",
            "compaction_last_compacted_at: None",
            f"cumulative_burn_tokens: {int(burn)}",
            "harness: hermes-agent",
            f"growth_since_last_checkpoint: {int(growth_since)}",
            f"anchor_reading: {int(anchor)}",
            f"api_calls: {int(api_calls)}",
            f"log_md_entry: {json.dumps(heading)}",
            f'tags: ["seat-state", {json.dumps(seat)}]',
            "---",
            "",
        ]
        body = [
            "#[[00_Command]] · seat context state record (75k-interval rule).",
            "",
            "Desktop Hermes session — mev-cadence plugin `post_llm_call` desktop",
            "duty (CHARGE-MEV-PLUGIN-CADENCE-FOLLOWUP-2026-10-30 Task B); same",
            "schema as the cmd seats (seat-context-watchdog.serialize_to_obsidian).",
            "Measured basis DESIGN-75K-ADAPTER.md §2.1: cumulative provider",
            "SUM(session_model_usage.input_tokens) over ALL task rows — never a",
            "context-window reading; monotone, so no trough-compaction detection",
            "(§6 limitation, inherited from the adapter).",
            "",
            f"- checkpoint: {m:02d}",
            f"- threshold: {interval} (growth since the instrument anchor)",
            f"- measured_input_tokens: {int(measured)} (SUM over the session's",
            "  session_model_usage rows, read-only §3.1 connection)",
            f"- growth_since_last_checkpoint: {int(growth_since)}",
            f"- cumulative_growth_at_crossing: {int(growth)}",
            f"- anchor_reading: {int(anchor)} (first measured reading at attach)",
            f"- session_uuid: {sid}",
            "- harness: hermes-agent (desktop session)",
            f"- ledger analog: hermes state.db session_model_usage",
            f"  ({self.db_path})",
            f"- model_id: {model}",
            f"- api_calls: {int(api_calls)}",
            f"- cumulative_burn_tokens: {int(burn)}",
            f"- ledger snapshot sha256_12: {snap_sha} (frozen crossing snapshot",
            "  embedded below — §4.4 analog; the seat-state folder is the only",
            "  sanctioned write root, so the snapshot lives inside the note",
            "  instead of a .state/crossing-*.json sidecar)",
            f"- vault log.md entry produced (Task A): `{heading}`",
            "",
            "## Frozen crossing snapshot (§4.4 analog)",
            "",
            "```json",
            json.dumps(snap_obj, sort_keys=True, indent=1),
            "```",
            "",
        ]
        return "\n".join(fm) + "\n" + "\n".join(body)

    def _desk_index_append(self, index, *, seat, m, measured, interval, growth,
                           model, sid, sha, note, stamp_iso) -> bool:
        """One INDEX row under flock; dedup on the checkpoint number; never
        rewrites existing rows (R-B discipline)."""
        index.parent.mkdir(parents=True, exist_ok=True)
        with open(index, "a+") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            try:
                f.seek(0)
                txt = f.read()
                fm, body = _split_frontmatter(txt)
                for ln in body.splitlines():
                    if ln.startswith("| ") and ln.split("|")[1].strip() == f"{m:02d}":
                        return False  # checkpoint row already present
                row = (f"| {m:02d} | {measured} | {interval} | {growth} | "
                       f"{model} | {sid} | {self.db_path} | {DESK_SANDBOX_PATH} | "
                       f"{sha} | [[{note.stem}]] | {stamp_iso} |\n")
                if not body.strip():
                    body = "\n" + DESK_INDEX_HEADER
                if not body.endswith("\n"):
                    body += "\n"
                body += row
                fm.setdefault("title", f"Seat state records — {seat}")
                fm.setdefault("type", "seat-state-index")
                if not fm.get("created"):
                    fm["created"] = stamp_iso
                fm["updated"] = stamp_iso
                fm.setdefault("tags", f'["seat-state-index", "{seat}"]')
                f.seek(0)
                f.truncate()
                f.write(_render_frontmatter(fm) + "\n" + body)
                return True
            finally:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def _desktop_duty(self, sid: str) -> dict:
        """Task B: one cmd-schema STATE note + INDEX row + one Task A log.md
        entry per 75k of MEASURED growth in the pinned desktop Hermes session.
        Measured basis: session_model_usage SUM(input_tokens) (§2.1). No rows
        => STOP with the missing field named (charge Task B.2) — never an
        estimate labeled as measured. Fail closed: write errors are recorded
        (fire event + INDEX cursor last_error), never raised."""
        seat = self.desktop_seat_label
        if not seat:
            return {"event": "noop", "reason": "no-desktop-seat-configured"}
        seat_dir = self.seat_state_root / "log" / "seat-state" / seat
        index = seat_dir / "INDEX.md"
        try:
            con = desk_connect_ro(self.db_path)
        except Exception as exc:
            return {"event": "fail-closed",
                    "reason": f"state store unreadable: {type(exc).__name__}: {exc}"}
        try:
            total, rows = desk_session_usage(con, sid)
        except Exception as exc:
            return {"event": "fail-closed",
                    "reason": f"usage read failed: {type(exc).__name__}: {exc}"}
        finally:
            try:
                con.close()
            except Exception:
                pass
        if not rows:
            # Task B.2: the store holds no measured figure for this session.
            return {"event": "stop",
                    "reason": "no measured usage rows for session",
                    "missing_field": "session_model_usage.input_tokens",
                    "session_id": sid}
        interval, interval_basis = self._desk_interval()
        cursor = desk_read_cursor(index)
        pinned = self.desktop_session_id or cursor.get("desktop_session_id")
        if pinned and str(pinned) != sid:
            return {"event": "noop", "reason": "not-pinned-session",
                    "pinned_session": str(pinned)}
        model = "unknown"
        for r in reversed(rows):
            if r.get("model"):
                model = r["model"]
                break
        api_calls = sum(int(r.get("api_call_count") or 0) for r in rows)
        burn = desk_burn(rows)
        stamp_iso = datetime.fromtimestamp(self.now_fn(), timezone.utc) \
            .strftime("%Y-%m-%dT%H:%M:%S%z")
        if not cursor:
            # First measured fire: anchor the basis (watchdog launch-anchor
            # analog, R-H reading-based). No note, no log entry.
            desk_write_cursor(index, seat, {
                "desktop_session_id": sid, "anchor_reading": int(total),
                "checkpoint": 0, "last_checkpoint_sum": int(total),
                "stamp": stamp_iso})
            return {"event": "noop", "reason": "first-reading-anchor",
                    "anchor_reading": int(total), "measured": int(total),
                    "interval": interval, "interval_basis": interval_basis,
                    "crossings": []}
        anchor = int(cursor.get("anchor_reading") or 0)
        if total < anchor:
            return {"event": "fail-closed",
                    "reason": f"measured reading decreased {anchor}->{total} "
                              "(cumulative basis violated, §2.1)"}
        growth = total - anchor
        last_cp = int(cursor.get("checkpoint") or 0)
        last_sum = int(cursor.get("last_checkpoint_sum") or anchor)
        m = last_cp + 1
        emitted, clean = [], last_cp
        while growth >= m * interval:
            res = self._desk_crossing(
                seat_dir=seat_dir, index=index, seat=seat, m=m, total=total,
                growth=growth, growth_since=total - last_sum, anchor=anchor,
                interval=interval, sid=sid, model=model, api_calls=api_calls,
                burn=burn, rows=rows)
            emitted.append(res)
            if res["errors"]:
                break  # retry this checkpoint next fire; never skip ahead
            clean = m
            last_sum = total
            m += 1
        if not emitted:
            return {"event": "noop", "reason": "below-threshold",
                    "measured": int(total), "anchor_reading": anchor,
                    "growth": growth, "interval": interval,
                    "interval_basis": interval_basis, "crossings": []}
        errs = [e for r in emitted for e in r["errors"]]
        if clean != last_cp or errs:
            desk_write_cursor(index, seat, {
                "desktop_session_id": sid, "anchor_reading": anchor,
                "checkpoint": clean, "last_checkpoint_sum": last_sum,
                "last_error": "; ".join(errs) if errs else None,
                "stamp": stamp_iso})
        return {"event": "desktop-75k", "measured": int(total),
                "anchor_reading": anchor, "growth": growth,
                "interval": interval, "interval_basis": interval_basis,
                "crossings": [{"checkpoint": r["checkpoint"],
                               "note": r["note"],
                               "index_row": r["index_row"],
                               "log_entry": r["log_entry"],
                               "errors": r["errors"]} for r in emitted],
                "write_errors": errs}

    # ── fire log (evidence at disk) ────────────────────────────────────────

    def _append_fire(self, fire: dict) -> None:
        try:
            mods = self._mods()
            mods["io"].allow(str(self.ledger_dir))
            mods["io"].append_jsonl(
                str(self.ledger_dir / "plugin-fires.jsonl"), fire)
        except Exception:
            pass  # telemetry must never break the host turn

    # ── the post_llm_call observer ─────────────────────────────────────────

    def on_post_llm_call(self, **payload) -> None:
        """``post_llm_call`` callback (hermes_cli/plugins.py VALID_HOOKS).

        Host contract (website/docs/.../hooks.md): fires once per turn on
        successful finalization; return value ignored. This callback ALWAYS
        returns None and never raises — every duty is fail-closed.
        """
        if off_switch_engaged():
            return None  # kill switch: ZERO writes, not even telemetry
        fire = {
            "ts": self.now_fn(), "plugin": PLUGIN_NAME, "hook": "post_llm_call",
            "session_id": payload.get("session_id"),
            "turn_id": payload.get("turn_id"),
            "model": payload.get("model"), "platform": payload.get("platform"),
            "history_messages": len(payload.get("conversation_history") or []),
        }
        try:
            sid = payload.get("session_id") or ""
            if not sid:
                fire["eviction"] = _fail_closed("no session_id in payload")
                fire["mirror"] = {"event": "noop", "reason": "no-session-id"}
                fire["desktop"] = {"event": "noop", "reason": "no-session-id"}
                # F-5: single write via finally below.
                return None
            mods = self._mods()
            try:
                turns = mods["source"].load_turns(sid, self.db_path)
            except Exception as exc:
                fire["eviction"] = _fail_closed(
                    f"session store unreadable: {type(exc).__name__}: {exc}")
                fire["mirror"] = {"event": "noop", "reason": "store-unreadable"}
                fire["desktop"] = {"event": "noop", "reason": "store-unreadable"}
                # F-5: single write via finally below.
                return None
            ordinal = len(turns)
            fire["turn_ordinal"] = ordinal
            if ordinal < mods["vh"].CADENCE_START_TURN:
                fire["eviction"] = {
                    "event": "noop", "reason": "below-cadence-threshold",
                    "turn_ordinal": ordinal, "fired": False}
                fire["mirror"] = {
                    "event": "noop", "reason": "below-cadence-threshold",
                    "turn_ordinal": ordinal}
                fire["desktop"] = {
                    "event": "noop", "reason": "below-cadence-threshold",
                    "turn_ordinal": ordinal}
                # F-5: single write via finally below.
                return None
            try:
                hook = self._get_hook()
                fire["staging"] = self._staging_status
                fire["staging_state_basis"] = self._staging_basis
                fire["eviction"] = hook.on_sync_turn(
                    payload.get("user_message") or "",
                    payload.get("assistant_response") or "",
                    session_id=sid)
            except Exception as exc:
                fire["eviction"] = _fail_closed(
                    f"eviction duty: {type(exc).__name__}: {exc}")
            try:
                fire["mirror"] = self._mirror_state()
            except Exception as exc:
                fire["mirror"] = {
                    "event": "fail-closed",
                    "reason": f"{type(exc).__name__}: {exc}"}
            try:
                fire["desktop"] = self._desktop_duty(sid)
            except Exception as exc:
                fire["desktop"] = {
                    "event": "fail-closed",
                    "reason": f"desktop duty: {type(exc).__name__}: {exc}"}
        except Exception as exc:
            fire["fail_closed"] = True
            fire["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            self._append_fire(fire)
        return None


def _settings_from_ctx(ctx) -> dict:
    """Settings from plugins.entries.<name>.settings via the host's
    plugin-relative get_config API (reserved roots such as 'plugins.entries'
    are rejected by the host — #64227; per-key reads are the supported
    channel). Env still wins later inside the constructor. Callables are
    NEVER settings — only paths and flags."""
    settings: dict = {}
    for k in _SETTING_KEYS:
        try:
            v = ctx.get_config(k)
        except Exception:
            v = None
        if v is not None:
            settings[k] = v
    return settings


def register(ctx):
    """Hermes directory-plugin entry (hermes_cli/plugins.py: plugin.yaml +
    __init__.py exposing register(ctx)). Light by design: the 10 s load
    timeout must never see a campaign import or a store open."""
    plugin = MevCadencePlugin(**_settings_from_ctx(ctx))
    ctx.register_hook("post_llm_call", plugin.on_post_llm_call)
    return plugin


__all__ = [
    "PLUGIN_NAME", "PLUGIN_VERSION", "STAGING_WORKSPACE", "OFF_SWITCH_ENV",
    "MevCadencePlugin", "StagingSink", "register",
    "parse_frontmatter", "render_state_entry",
    "desk_connect_ro", "desk_session_usage", "desk_burn",
    "desk_read_cursor", "desk_write_cursor", "DESK_INDEX_HEADER",
    "DESK_INTERVAL_FALLBACK", "DESK_SANDBOX_PATH",
]
