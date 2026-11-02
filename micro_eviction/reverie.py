"""mev.reverie — spark mint / maintenance for the REVERIE layer.

CHARGE-REVERIE-V1-2026-10-29 §2/§3 (v1.2 — §3 lifecycle is governing).

A SPARK is one connection event across >=2 existing episodes. It is not new
episodic content; its product is a relation. This module is the record layer:
it mints sparks into the **PROVISIONAL** ``reverie-staging`` Engraphis
workspace and maintains corroboration. It is not the firing lane — that is
:mod:`micro_eviction.reverie_logic`, swappable (§4).

The lifecycle, mechanically enforced here
-----------------------------------------
1. MINT (§3.1) — ``Reverie.mint`` / ``on_demand`` / ``scheduled_tick`` build a
   spark (>=2 episodes + provocation + optional synthesis) and hand it to
   ``EngraphisSink.ingest_spark``. That is the ONLY write path: direct store
   writes are refused by construction (:class:`SinkRequired`), and the sink
   refuses a tier/workspace mismatch. MemOS is never written.
2. SURFACE (§3.2) — sparks appear only in retrieval's separate ``hints``
   channel (``micro_eviction.retrieval.Retrieval.hints``); nothing here adds a
   spark to a ranked list.
3. PROMOTE (§3.3) — there is **no** promotion path in this module. Promotion is
   the dream pass's ``--promote-sparks`` stage, cited and revision-open. There
   is deliberately no ``promote`` method (A2: no auto-promotion). Corroboration
   only strengthens weight/visibility (:meth:`Reverie.corroborate`).
4. DECAY (§3.4) — DECAYED is a named terminal state written ONLY through the
   sanctioned staging-drain path. This module has no DECAYED writer:
   :meth:`Reverie.request_decay` *delegates* to an injected drain callable and
   refuses loudly when none is wired (:class:`DecayRefused`).

Also enforced here: the ``reverie-staging`` cap is the single
``STAGING_LIMITS`` referent imported from ``engraphis_sink`` (never forked),
and the workspace is separate from ``mev-staging``.

Read-only discipline: spark reads go through ``retrieval.open_readonly``
(``?mode=ro`` + ``PRAGMA query_only``). Receipts are the one side ledger this
module may append to, and only at a caller-named path.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from . import io as mev_io
from .reverie_logic import DefaultSparkLogic

# ── seat identity (dispatch rule: every new file carries it) ────────────────
SEAT_ID = "ENGINEER-deepseek-v4.1-flash-reverie-engineer"
SEAT_STAGE = "V3-REVERIE"

DEFAULT_CAMPAIGN_ROOT = Path(
    os.environ.get(
        "MEV_CAMPAIGN_ROOT",
        os.environ.get("MEV_CAMPAIGN_ROOT", str(Path.home() / ".mev-cadence")),
    )
)
DEFAULT_ENGRAPHIS_DB = Path(
    os.environ.get("ENGRAPHIS_LIVE_DB", "~/.engraphis/memory.db")
).expanduser()

# ── the single cap referent (engraphis_sink.STAGING_LIMITS — never forked) ───
try:  # campaign-root module; tests put ROOT on sys.path
    import engraphis_sink as _sink_mod  # noqa: E402
    STAGING_LIMITS = _sink_mod.STAGING_LIMITS
except Exception:  # pragma: no cover - lazy path below still refuses to fork
    _sink_mod = None
    STAGING_LIMITS = None


def _sink_module():
    global _sink_mod
    if _sink_mod is None:
        import engraphis_sink
        _sink_mod = engraphis_sink
    return _sink_mod


def staging_limits() -> dict:
    """The adopted cap, read from the single referent (a copy, never a fork)."""
    return dict(_sink_module().STAGING_LIMITS)


# ── vocabulary (the contract) ───────────────────────────────────────────────
SPARK_FORM = "reverie"                  # the registry form name (§2)
REVERIE_STAGING_WORKSPACE = "reverie-staging"    # PROVISIONAL tier (§3.1)
REVERIE_PERMANENT_WORKSPACE = "reverie-permanent"  # PERMANENT tier (§3.3)
MEV_STAGING_WORKSPACE = "mev-staging"   # eviction staging; NOT ours (§5)

PROVISIONAL = "PROVISIONAL"
PERMANENT = "PERMANENT"
DECAYED = "DECAYED"
REVISION_OPEN = "revision-open"

MINT_MIN_EPISODES = 2                    # A1: mint may be >=2 (staging only)
PROMOTION_MIN_DISTINCT_SOURCES = 3       # A1: promotion inherits >=3-distinct
HINT_WEIGHT_BASE = 0.10                  # low by construction (§2 posture)
HINT_WEIGHT_STEP = 0.05                  # per distinct session/context
HINT_WEIGHT_CAP = 0.35                   # strengthens, never outranks recall
DAEMON_SCHEDULE_FORMS = False            # §4: cadence is the sole producer


# ── errors ──────────────────────────────────────────────────────────────────
class ReverieError(RuntimeError):
    """Base class for loud reverie refusals."""


class SinkRequired(ReverieError):
    """A write was attempted without the sanctioned Engraphis sink."""


class SinkWorkspaceRefused(ReverieError):
    """The mint sink is not bound to the PROVISIONAL reverie-staging workspace."""


class InsufficientSparks(ReverieError):
    """A connection needs at least two episodes (A1)."""


class SparkMalformed(ReverieError):
    """A spark record failed its binding/schema checks."""


class DecayRefused(ReverieError):
    """DECAYED is only written by the sanctioned staging-drain path (§3.4)."""


class CadenceUnavailable(ReverieError):
    """The S7 cadence calendar reader is not importable (§4 subscription)."""


# ── small helpers ───────────────────────────────────────────────────────────

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _canon(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, default=str)


def _jload(raw) -> dict:
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}


def _sha16(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _norm_provocation(provocation) -> dict:
    """Provocation is the query (on-demand) or the schedule tick (scheduled).

    A8: provocation may be ambient/associative — a bare string is wrapped, a
    dict is carried verbatim.
    """
    if isinstance(provocation, dict):
        return dict(provocation)
    return {"kind": "on_demand", "query": provocation}


def _norm_ref(ref) -> dict:
    """Normalize one episode reference to record_id + span/bounds (§2)."""
    if isinstance(ref, str):
        return {"record_id": ref}
    if not isinstance(ref, dict):
        raise SparkMalformed(f"spark reference is not an object: {ref!r}")
    out = {"record_id": ref.get("record_id")}
    bounds = ref.get("bounds", ref.get("span"))
    if bounds is not None:
        out["bounds"] = bounds
    for key in ("session_id", "context"):
        if ref.get(key) is not None:
            out[key] = ref[key]
    return out


def _norm_refs(sparks_from: Iterable) -> list[dict]:
    if not isinstance(sparks_from, (list, tuple)):
        raise InsufficientSparks("sparks_from must be a list of episode references")
    refs = [_norm_ref(r) for r in sparks_from]
    if len(refs) < MINT_MIN_EPISODES:
        raise InsufficientSparks(
            f"a spark connects >= {MINT_MIN_EPISODES} episodes; got {len(refs)}")
    for ref in refs:
        if not ref.get("record_id"):
            raise SparkMalformed(f"spark reference lacks record_id: {ref!r}")
        if "bounds" not in ref:
            raise SparkMalformed(
                f"spark reference lacks span/bounds: {ref['record_id']!r}")
    return refs


def spark_id_for(provocation, sparks_from: Iterable, synthesis=None) -> str:
    """Deterministic spark identity: same connection -> same spark (idempotent)."""
    body = {
        "provocation": _norm_provocation(provocation),
        "sparks_from": [_norm_ref(r) for r in sparks_from],
        "synthesis": (synthesis or "").strip() or None,
    }
    return "spark_" + _sha16(_canon(body))


def distinct_sources(sparks_from: Iterable) -> list[str]:
    """Distinct cited record_ids, order-preserving (the >=3 law's unit)."""
    out: list[str] = []
    for ref in sparks_from or []:
        rid = ref.get("record_id") if isinstance(ref, dict) else ref
        if rid and rid not in out:
            out.append(rid)
    return out


# ── the spark record (the record layer; no firing logic) ────────────────────

def build_spark(provocation, sparks_from, synthesis=None, *,
                session_id=None, context=None, weight=None,
                now: str | None = None) -> dict:
    """Build one spark record bound for the PROVISIONAL tier."""
    prov = _norm_provocation(provocation)
    refs = _norm_refs(sparks_from)
    sid = spark_id_for(prov, refs, synthesis)
    synth = (synthesis or "").strip() or None
    content = synth or (
        "reverie: a connection across " + ", ".join(r["record_id"] for r in refs))
    record = {
        "record_id": sid,
        "spark_id": sid,
        "form": SPARK_FORM,
        "provenance": "derived",
        "derived_from": refs,
        "sparks_from": refs,
        "provocation": prov,
        "synthesis": synth,
        "status": PROVISIONAL,
        "tier": PROVISIONAL,
        "weight": float(weight if weight is not None else HINT_WEIGHT_BASE),
        "is_immune": False,
        "marking": "reverie:hint",
        "corroboration": {"sessions": [], "contexts": [], "count": 0},
        "content": content,
        "created_at": now or _utcnow(),
    }
    if session_id is not None:
        record["session_id"] = session_id
    if context is not None:
        record["context"] = context
    violations = validate_spark(record)
    if violations:
        raise SparkMalformed("; ".join(violations))
    return record


def validate_spark(record) -> list[str]:
    """Return binding violations (empty == accepted)."""
    if not isinstance(record, dict):
        return [f"spark is not an object: {type(record).__name__}"]
    v: list[str] = []
    rid = record.get("record_id")
    if not isinstance(rid, str) or not rid.startswith("spark_"):
        v.append(f"record_id must be a spark_ id: {rid!r}")
    if record.get("spark_id") != rid:
        v.append("spark_id must equal record_id")
    if record.get("form") != SPARK_FORM:
        v.append(f"form must be {SPARK_FORM!r}")
    if record.get("provenance") != "derived":
        v.append("provenance must be 'derived'")
    refs = record.get("sparks_from")
    if not isinstance(refs, list) or len(refs) < MINT_MIN_EPISODES:
        v.append(f"sparks_from needs >= {MINT_MIN_EPISODES} episodes")
    if record.get("derived_from") != refs:
        v.append("derived_from must equal sparks_from")
    for ref in (refs if isinstance(refs, list) else []):
        if not isinstance(ref, dict) or not ref.get("record_id"):
            v.append(f"malformed spark reference: {ref!r}")
        elif "bounds" not in ref:
            v.append(f"reference {ref.get('record_id')!r} lacks span/bounds")
    if record.get("status") not in (PROVISIONAL, PERMANENT, DECAYED, REVISION_OPEN):
        v.append(f"unknown status {record.get('status')!r}")
    try:
        w = float(record.get("weight"))
    except (TypeError, ValueError):
        v.append("weight must be numeric")
    else:
        if not (0.0 <= w <= 1.0):
            v.append(f"weight out of range: {w}")
    return v


def promotion_eligibility(spark) -> dict:
    """The dream-pass gate, expressed ONCE and shared.

    A1: promotion inherits the >=3-DISTINCT-source law. A6: synthesis is
    required for promotion. A2: corroboration is NOT a promotion signal — a
    2-dot spark stays ineligible however many times it is corroborated.
    """
    meta = spark.get("metadata") if isinstance(spark.get("metadata"), dict) else spark
    reasons: list[str] = []
    if meta.get("form") != SPARK_FORM:
        reasons.append(f"not a spark form: {meta.get('form')!r}")
    if meta.get("provenance") != "derived":
        reasons.append("provenance is not 'derived'")
    if str(meta.get("status") or "").upper() == DECAYED:
        reasons.append("spark is DECAYED (terminal; drain path only)")
    if meta.get("is_immune") is True:
        reasons.append("spark is immune (never promoted into permanent memory)")
    if not str(meta.get("synthesis") or "").strip():
        reasons.append("synthesis_required_for_promotion (A6)")
    citations = distinct_sources(meta.get("sparks_from") or meta.get("derived_from"))
    if len(citations) < PROMOTION_MIN_DISTINCT_SOURCES:
        reasons.append(
            f"promotion_needs_{PROMOTION_MIN_DISTINCT_SOURCES}_distinct_sources"
            f" (has {len(citations)})")
    return {"ok": not reasons, "reasons": reasons,
            "distinct_sources": len(citations), "citations": citations}


# ── read-only spark reader (ro-URI discipline reused) ───────────────────────

def load_sparks(engraphis_db, *, workspace: str = REVERIE_STAGING_WORKSPACE) -> list[dict]:
    """Read sparks from one workspace with the shared read-only discipline."""
    from .retrieval import open_readonly  # ro-URI + query_only, single source

    path = Path(engraphis_db).expanduser()
    if not path.is_file():
        raise ReverieError(f"Engraphis store not found: {path}")
    conn, _uri = open_readonly(path)
    try:
        rows = conn.execute(
            "SELECT m.id AS store_id, m.content AS content, m.metadata AS metadata "
            "FROM memories m JOIN workspaces w ON w.id = m.workspace_id "
            "WHERE w.name = ?", (workspace,)).fetchall()
    finally:
        conn.close()

    out: list[dict] = []
    for row in rows:
        meta = _jload(row["metadata"])
        if meta.get("form") != SPARK_FORM:
            continue
        rec = dict(meta)
        rec.setdefault("spark_id", meta.get("record_id"))
        rec.setdefault("record_id", meta.get("spark_id"))
        rec["content"] = row["content"] or meta.get("synthesis") or ""
        rec["_store_id"] = row["store_id"]
        rec["metadata"] = meta
        out.append(rec)
    return out


# ── S7 cadence subscription (§4: subscribe, never create schedule forms) ────

def load_calendar_reader(campaign_root=None):
    """Import the S7 cadence calendar reader (declared cross-spec dependency).

    Contract (what S7's ``micro_eviction.cadence`` must expose): one of
    ``calendar_reader(root)`` / ``make_calendar_reader(root)`` returning an
    object with ``due(now) -> list[dict]`` (ticks: at least ``tick_id``,
    ``kind``, ``ts``) — or a ``CalendarReader(root)`` class. This module only
    *reads* ticks; it creates no CALENDAR/SCHEDULE forms (one-producer law).
    """
    root = Path(campaign_root) if campaign_root else DEFAULT_CAMPAIGN_ROOT
    try:
        from . import cadence  # S7 producer; absent until S7 lands
    except Exception:
        return None
    for name in ("calendar_reader", "make_calendar_reader"):
        factory = getattr(cadence, name, None)
        if callable(factory):
            try:
                return factory(root)
            except TypeError:
                return factory()
    cls = getattr(cadence, "CalendarReader", None)
    if cls is not None:
        return cls(root)
    return None


def _due_ticks(reader, now) -> list[dict]:
    for name in ("due", "ticks_due", "due_ticks"):
        fn = getattr(reader, name, None)
        if callable(fn):
            return list(fn(now) or [])
    if callable(reader):
        return list(reader(now) or [])
    raise ReverieError(
        "calendar reader exposes no due(now)/ticks_due(now) method; the "
        "reverie subscription contract is due(now) -> list[dict]")


# ── the record layer ────────────────────────────────────────────────────────

class Reverie:
    """Spark mint/maintenance over the PROVISIONAL reverie-staging workspace."""

    def __init__(self, sink, *, campaign_root=None, engraphis_db=None,
                 logic=None, receipt_path=None, calendar_reader=None,
                 candidate_provider: Optional[Callable] = None,
                 drain: Optional[Callable] = None,
                 source_check: Optional[Callable] = None,
                 spark_reader: Optional[Callable] = None):
        self.sink = self._require_sink(sink)
        self.campaign_root = Path(campaign_root) if campaign_root else DEFAULT_CAMPAIGN_ROOT
        self.engraphis_db = (Path(engraphis_db).expanduser() if engraphis_db
                             else DEFAULT_ENGRAPHIS_DB)
        self.logic = logic or DefaultSparkLogic()
        self.receipt_path = Path(receipt_path) if receipt_path else None
        self.calendar_reader = calendar_reader
        self.candidate_provider = candidate_provider
        self.drain = drain
        self.source_check = source_check
        self.spark_reader = spark_reader or (
            lambda: load_sparks(self.engraphis_db,
                                workspace=REVERIE_STAGING_WORKSPACE))
        self.receipts: list[dict] = []
        self._corroborations: list[dict] = self._load_receipts()

    # ── sink gate: the only write path ──

    @staticmethod
    def _require_sink(sink):
        mod = _sink_module()
        if not isinstance(sink, mod.EngraphisSink):
            raise SinkRequired(
                "spark writes go ONLY through "
                f"{mod.__name__}.EngraphisSink.ingest_spark; got "
                f"{type(sink).__name__} (direct writes are refused)")
        if sink.workspace != REVERIE_STAGING_WORKSPACE:
            raise SinkWorkspaceRefused(
                f"mint workspace must be {REVERIE_STAGING_WORKSPACE!r}; "
                f"got {sink.workspace!r} (promotion is the dream pass, not mint)")
        return sink

    # ── receipts (corroboration is receipt-recorded) ──

    def _load_receipts(self) -> list[dict]:
        if self.receipt_path is None or not self.receipt_path.is_file():
            return []
        rows = mev_io.load_jsonl(self.receipt_path, tolerant=True)
        return [r for r in rows if r.get("kind") == "spark_corroboration"]

    def _record_receipt(self, event: dict) -> dict:
        event = dict(event)
        event.setdefault("ts", time.time())
        self.receipts.append(event)
        journal = getattr(self.sink, "journal_event", None)
        if callable(journal):
            journal(event)
        if self.receipt_path is not None:
            mev_io.allow(self.receipt_path.parent)
            mev_io.append_jsonl(self.receipt_path, event)
        return event

    # ── MINT (§3.1) ──

    def mint(self, provocation, sparks_from, synthesis=None, *,
             session_id=None, context=None, now: str | None = None) -> dict:
        if self.source_check is not None:
            missing = [rid for rid in distinct_sources(
                [_norm_ref(r) for r in (sparks_from or [])])
                if not self.source_check(rid)]
            if missing:
                raise SparkMalformed(
                    f"sparks_from references unknown records: {missing}")
        record = build_spark(provocation, sparks_from, synthesis,
                             session_id=session_id, context=context, now=now)
        out = self.sink.ingest_spark(record, tier=PROVISIONAL)
        if out.get("op") == "hold":
            self._record_receipt({
                "kind": "spark_mint_held", "spark_id": record["spark_id"],
                "workspace": REVERIE_STAGING_WORKSPACE,
                "reason": out.get("reason"), "limits": out.get("limits"),
            })
            return {"op": "hold", "tier": PROVISIONAL, "spark_id": record["spark_id"],
                    "status": PROVISIONAL, "record": record, "sink": out,
                    "promotion": False, "auto_promoted": False}
        self._record_receipt({
            "kind": "spark_mint", "spark_id": record["spark_id"],
            "tier": PROVISIONAL, "workspace": REVERIE_STAGING_WORKSPACE,
            "derived_from": [r["record_id"] for r in record["derived_from"]],
            "synthesis": record["synthesis"],
        })
        return {"op": out.get("op"), "tier": PROVISIONAL,
                "spark_id": record["spark_id"], "status": PROVISIONAL,
                "record": record, "sink": out,
                "promotion": False, "auto_promoted": False}

    def on_demand(self, provocation, candidates, *, synthesis=None,
                  session_id=None) -> dict:
        """On-demand mode: provocation = the query; logic proposes the dots."""
        prov = _norm_provocation(provocation)
        proposals = self.logic.propose(prov, candidates)
        minted = []
        for proposal in proposals:
            minted.append(self.mint(
                prov, proposal["sparks_from"], synthesis,
                session_id=session_id or proposal.get("session_id"),
                context=proposal.get("context")))
        return {"mode": "on-demand", "provocation": prov,
                "proposals": len(proposals), "minted": minted,
                "schedule_forms_created": DAEMON_SCHEDULE_FORMS}

    def scheduled_tick(self, now=None) -> dict:
        """Scheduled light mode: SUBSCRIBE to the S7 cadence calendar.

        This method reads due ticks from the S7 reader and runs the swappable
        logic over the caller's candidate pool. It creates no CALENDAR or
        SCHEDULE form — cadence is their sole producer (§4).
        """
        now = time.time() if now is None else now
        reader = self.calendar_reader or load_calendar_reader(self.campaign_root)
        base = {"mode": "scheduled", "schedule_forms_created": DAEMON_SCHEDULE_FORMS}
        if reader is None:
            return {**base, "status": "cadence_unavailable", "ticks": 0,
                    "minted": [],
                    "reason": ("S7 cadence calendar reader unavailable "
                               "(micro_eviction.cadence not importable); reverie "
                               "subscribes to cadence and never creates schedule forms")}
        try:
            ticks = _due_ticks(reader, now)
        except ReverieError as exc:
            return {**base, "status": "cadence_contract_error", "ticks": 0,
                    "minted": [], "reason": str(exc)}

        minted: list[dict] = []
        for tick in ticks:
            tick = tick if isinstance(tick, dict) else {"tick_id": str(tick)}
            prov = {
                "kind": "schedule_tick",
                "tick_id": tick.get("tick_id") or tick.get("id"),
                "calendar_kind": tick.get("kind"),
                "scheduled_for": tick.get("ts") or tick.get("scheduled_for"),
                "source": "S7-cadence",
            }
            candidates = (list(self.candidate_provider(prov) or [])
                          if self.candidate_provider else [])
            for proposal in self.logic.propose(prov, candidates):
                try:
                    minted.append(self.mint(
                        prov, proposal["sparks_from"],
                        session_id=proposal.get("session_id"),
                        context=proposal.get("context")))
                except ReverieError as exc:
                    minted.append({"op": "refused", "reason": str(exc)})
        return {**base, "status": "ok", "ticks": len(ticks), "minted": minted}

    # ── CORROBORATION (strengthens; NEVER promotes — A2) ──

    def corroborate(self, spark_id: str, *, session_id: str,
                    context: str | None = None, now=None) -> dict:
        if not session_id:
            raise ReverieError(
                "corroboration requires a session_id (distinct sessions/contexts "
                "are the signal)")
        if self.get(spark_id) is None:
            raise ReverieError(f"unknown spark {spark_id!r} in "
                               f"{REVERIE_STAGING_WORKSPACE}")
        key = (spark_id, session_id, context)
        if any((e["spark_id"], e.get("session_id"), e.get("context")) == key
               for e in self._corroborations):
            return {"op": "skip", "reason": "duplicate_corroboration",
                    "spark_id": spark_id, "tier": PROVISIONAL,
                    "promotion": False, "auto_promoted": False,
                    **self.corroboration_summary(spark_id)}

        event = {"kind": "spark_corroboration", "spark_id": spark_id,
                 "session_id": session_id, "context": context,
                 "tier": PROVISIONAL, "promotion": False,
                 "ts": time.time() if now is None else now}
        self._corroborations.append(event)
        self._record_receipt(event)
        summary = self.corroboration_summary(spark_id)
        return {"op": "corroborate", "spark_id": spark_id,
                "tier": PROVISIONAL, "promotion": False, "auto_promoted": False,
                "receipt": event, **summary}

    def corroboration_summary(self, spark_id: str) -> dict:
        events = [e for e in self._corroborations if e.get("spark_id") == spark_id]
        sessions = sorted({e["session_id"] for e in events if e.get("session_id")})
        contexts = sorted({e["context"] for e in events if e.get("context")})
        weight = min(HINT_WEIGHT_BASE + HINT_WEIGHT_STEP * (len(sessions) + len(contexts)),
                     HINT_WEIGHT_CAP)
        return {"distinct_sessions": len(sessions), "distinct_contexts": len(contexts),
                "sessions": sessions, "contexts": contexts,
                "corroboration_count": len(events), "derived_weight": weight,
                "weight_cap": HINT_WEIGHT_CAP}

    def promotion_eligibility(self, spark) -> dict:
        return promotion_eligibility(spark)

    # ── DECAY (§3.4): delegate to the sanctioned drain, never write it ──

    def request_decay(self, spark_id: str) -> dict:
        """DECAYED is written ONLY by the sanctioned staging-drain path."""
        if self.drain is None:
            raise DecayRefused(
                "DECAYED is written only through the sanctioned staging-drain "
                "path (gate-review-gated, S7/A13); no drain is wired — reverie "
                "has no DECAYED writer (non-destructive, no new writer)")
        result = self.drain(spark_id)
        self._record_receipt({
            "kind": "spark_decay_delegated", "spark_id": spark_id,
            "via": "sanctioned_staging_drain", "reverie_wrote": False,
            "result": result if isinstance(result, dict) else str(result),
        })
        return {"op": "delegated", "spark_id": spark_id,
                "written_by": "sanctioned_staging_drain", "reverie_wrote": False,
                "result": result}

    def drain_candidates(self) -> list[dict]:
        """Read-only drain candidacy (the WRITE belongs to the drain path)."""
        out = []
        for spark in self.spark_reader():
            if str(spark.get("status") or "").upper() != PROVISIONAL:
                continue
            out.append({"spark_id": spark.get("spark_id"),
                        "status": spark.get("status"),
                        "via": "staging-drain (gate review; not written here)"})
        return out

    # ── readers ──

    def get(self, spark_id: str) -> dict | None:
        for spark in self.spark_reader():
            if spark.get("spark_id") == spark_id or spark.get("record_id") == spark_id:
                return spark
        return None

    def status(self, spark_id: str) -> str | None:
        spark = self.get(spark_id)
        return None if spark is None else spark.get("status")

    def list(self) -> list[dict]:
        return list(self.spark_reader())


__all__ = [
    "SEAT_ID", "SEAT_STAGE", "DEFAULT_CAMPAIGN_ROOT", "DEFAULT_ENGRAPHIS_DB",
    "STAGING_LIMITS", "staging_limits",
    "SPARK_FORM", "REVERIE_STAGING_WORKSPACE", "REVERIE_PERMANENT_WORKSPACE",
    "MEV_STAGING_WORKSPACE", "PROVISIONAL", "PERMANENT", "DECAYED",
    "REVISION_OPEN", "MINT_MIN_EPISODES", "PROMOTION_MIN_DISTINCT_SOURCES",
    "HINT_WEIGHT_BASE", "HINT_WEIGHT_STEP", "HINT_WEIGHT_CAP",
    "ReverieError", "SinkRequired", "SinkWorkspaceRefused", "InsufficientSparks",
    "SparkMalformed", "DecayRefused", "CadenceUnavailable",
    "spark_id_for", "distinct_sources", "build_spark", "validate_spark",
    "promotion_eligibility", "load_sparks", "load_calendar_reader", "Reverie",
]
