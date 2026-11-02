"""mev.vault_hook — S3 turn-boundary cadence hook (R2 + G2).

The mini-eviction cadence: at every turn boundary for turn ordinal T >= 6 the
hook runs the Stage-2 slice pipeline (extract_slices -> capture_slice = T1/T2
authoring -> durable staging) over the messages that left the active window
(frozen slice-boundary rule, plan §1.2 / capture.py slice amendment).

Ownership split (R2): this MODULE + fixtures + the rollback/activation scripts
live in the campaign tree. ACTIVATION (installing into the Hermes estate at the
MemoryManager.sync_all._sync wrap point, agent/memory_manager.py:625) is a
separate gated stage executed by the orchestrator only — see
ops/estate-activation/. Nothing here installs, patches, or contacts the estate.

INERT UNTIL ACTIVATED: importing this module has no side effects (no disk, no
env, no estate). A VaultHook instance is inert unless constructed with
enabled=True (the activation shim does that) and the runtime off-switch env
MEV_VAULT_HOOK_OFF is not engaged. Fails closed: any ambiguity or error yields
a fail-closed report and no further writes — never a crash into the host turn
pipeline, never a silent partial decision.

Caps honored at every boundary (R8, adopted numbers):
  * per-boundary authoring cap 8,000 measured tokens across T2 passes;
  * circuit breaker: T2 fallback rate > 25% over any 5 consecutive
    boundaries -> cadence degrades to T1-only and the layer STOPS for
    adjudication;
  * injection budget + adequacy gate + grant check unchanged — the hook reads
    authorization.live_grant VERBATIM and records the 2,000-char injection
    budget (injection.BUDGET_CHARS) without altering it.

Write seam signature mirrors MemoryProvider.sync_turn so the activation shim
can delegate straight through (G2: fold into the existing provider's
sync_turn, never a second provider).
"""
from __future__ import annotations

import os
import time

from . import authorization
from . import capture
from . import injection
from . import io
from . import source
from .t2_classifier import (
    BREAKER_RATE_THRESHOLD,
    BREAKER_WINDOW,
    PER_BOUNDARY_TOKEN_CAP,
    FallbackBreaker,
    T2BoundaryState,
)

# Turn ordinal at which the cadence starts; turns 1..5 are no-ops (design §3:
# "every turn after turn 5"). Frozen here per plan §1.2 ("T >= 6").
CADENCE_START_TURN = 6

# Runtime off-switch: set to 1/true/yes/on to disengage the cadence without
# uninstalling the shim (checked at every fire; ops kill-switch).
OFF_SWITCH_ENV = "MEV_VAULT_HOOK_OFF"

FALLBACK_RATE_DENOMINATOR = "items per boundary (R5)"

_OFF_VALUES = ("1", "true", "yes", "on")


def off_switch_engaged() -> bool:
    """True when the runtime kill-switch env disengages the cadence."""
    return os.environ.get(OFF_SWITCH_ENV, "").strip().lower() in _OFF_VALUES


class VaultHook:
    """Turn-boundary mini-eviction cadence. Inert until enabled.

    on_sync_turn mirrors MemoryProvider.sync_turn(self, user_content,
    assistant_content, *, session_id="", messages=None, turn_author=None).
    It returns a per-boundary cadence report and never raises: fail-closed is
    reported as {"ok": False, "event": "fail-closed", ...} with no writes past
    the last durable slice (capture's at-least-once cursor + idempotent
    record_ids make replay safe).
    """

    def __init__(
        self,
        *,
        ledger_dir: str,
        db_path: str | None = None,
        session_id: str | None = None,
        enabled: bool = False,
        floor_theta: float = 1.0,
        llm_fn=None,
        budget_cap: int = PER_BOUNDARY_TOKEN_CAP,
        breaker: FallbackBreaker | None = None,
        sink=None,
        sink_trusted: bool = False,
        charge_path: str | None = None,
        now_fn=None,
    ) -> None:
        self.ledger_dir = str(ledger_dir)
        self.db_path = db_path
        self.session_id = session_id
        self.enabled = bool(enabled)
        self.floor_theta = float(floor_theta)
        self.llm_fn = llm_fn
        self.budget_cap = int(budget_cap)
        self.breaker = breaker if breaker is not None else FallbackBreaker(
            window=BREAKER_WINDOW, threshold=BREAKER_RATE_THRESHOLD)
        self.sink = sink
        self.sink_trusted = bool(sink_trusted)
        self.charge_path = charge_path
        self._now_fn = now_fn if now_fn is not None else time.time

    # ── cadence report helpers ────────────────────────────────────────────

    def _report(self, **fields) -> dict:
        rep = {
            "ok": True,
            "event": "noop",
            "reason": "",
            "session_id": self.session_id,
            "turn_ordinal": 0,
            "fired": False,
            "evicted": False,
            "slices": [],
            "records_total": 0,
            "staged_writes": 0,
            "held": False,
            "hold_reason": None,
            "budget": {
                "cap_tokens": self.budget_cap,
                "tokens_used": 0,
                "truncated": False,
                "items_seen": 0,
                "items_below_floor": 0,
                "items_t2_llm": 0,
            },
            "fallback_rate": 0.0,
            "fallback_rate_denominator": FALLBACK_RATE_DENOMINATOR,
            "breaker": None,
            "injection_budget_chars": injection.BUDGET_CHARS,
            "grant": None,
        }
        rep.update(fields)
        return rep

    @staticmethod
    def _noop(reason: str, **fields) -> dict:
        fields.setdefault("event", "noop")
        fields.setdefault("fired", False)
        fields.setdefault("evicted", False)
        fields["reason"] = reason
        fields.setdefault("ok", True)
        return fields

    # ── cursor (plan §1.2 step 7; written by capture.capture_slice) ───────

    def _cursor_path(self, session_id: str):
        return io.Path(self.ledger_dir) / f".slice_cursor_{session_id}.json"

    def _load_cursor(self, session_id: str) -> dict:
        import json
        path = self._cursor_path(session_id)
        if not path.exists():
            return {"last_boundary_id": 0, "last_processed_max_msg_id": 0,
                    "last_turn_ordinal": 0}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"unreadable slice cursor {path}: {exc}")
        return {
            "last_boundary_id": int(data.get("last_boundary_id") or 0),
            "last_processed_max_msg_id": int(data.get("last_processed_max_msg_id") or 0),
            "last_turn_ordinal": int(data.get("last_turn_ordinal") or 0),
        }

    # ── grant (verbatim; plan R2: reads authorization.live_grant VERBATIM) ─

    def _read_grant(self) -> dict:
        if not self.charge_path:
            return {"grant": None, "sandbox_only": True,
                    "status_verbatim": None, "charge_path": None,
                    "source": "no-charge-path"}
        out = dict(authorization.live_grant(self.charge_path))
        out["source"] = "authorization.live_grant"
        return out

    # ── the turn-boundary seam ────────────────────────────────────────────

    def on_sync_turn(self, user_content, assistant_content, *,
                     session_id: str = "", messages=None, turn_author=None) -> dict:
        """Fire the mini-eviction cadence at this turn boundary.

        Write seam from turn 6 onward (frozen slice-boundary rule); turns 1-5
        and the off-switch are no-ops with the reason named. Never raises.
        """
        sid = session_id or self.session_id or ""
        try:
            rep = self._report(session_id=sid)
            rep["grant"] = self._read_grant()
            if not self.enabled:
                rep.update(self._noop("not-activated"))
                return rep
            if off_switch_engaged():
                rep.update(self._noop("off-switch"))
                return rep
            if not sid:
                return self._fail_closed("no session_id bound", sid)
            if self.db_path is None:
                return self._fail_closed("no session store bound", sid)

            turns = source.load_turns(sid, self.db_path)
            turn_ordinal = len(turns)
            rep["turn_ordinal"] = turn_ordinal
            if turn_ordinal < CADENCE_START_TURN:
                rep.update(self._noop("below-cadence-threshold"))
                rep["turn_ordinal"] = turn_ordinal
                return rep

            cursor = self._load_cursor(sid)
            slices = capture.extract_slices(
                sid,
                db_path=self.db_path,
                last_boundary_id=cursor["last_boundary_id"],
                last_processed_max_msg_id=cursor["last_processed_max_msg_id"],
                last_turn_ordinal=cursor["last_turn_ordinal"],
                turns=turns,
            )
            if not slices:
                rep.update(self._noop("empty-slice"))
                rep["turn_ordinal"] = turn_ordinal
                return rep

            return self._run_boundary(rep, sid, turns, slices)
        except Exception as exc:  # fail closed, never crash the host seam
            return self._fail_closed(f"{type(exc).__name__}: {exc}", sid)

    def _fail_closed(self, reason: str, session_id: str) -> dict:
        rep = self._report(session_id=session_id)
        rep["ok"] = False
        rep["event"] = "fail-closed"
        rep["reason"] = f"fail-closed: {reason}"
        rep["fired"] = False
        rep["evicted"] = False
        try:
            rep["grant"] = self._read_grant()
        except Exception:
            rep["grant"] = {"grant": None, "sandbox_only": True,
                            "source": "fail-closed"}
        return rep

    def _run_boundary(self, rep: dict, sid: str, turns: list, slices: list) -> dict:
        """One cadence boundary: capture every slice under one T2 budget,
        stage through the sink when bound, observe the fallback breaker."""
        t2_state = T2BoundaryState(
            budget_cap=self.budget_cap, llm_fn=self.llm_fn,
            floor_theta=self.floor_theta)
        if self.breaker.tripped:
            t2_state.degrade()

        staged: list[dict] = []
        sink_writes = 0
        hold_reason = None
        for sl in slices:
            recs = capture.capture_slice(
                sid, sl,
                turns=turns,
                now=float(self._now_fn()),
                floor_theta=self.floor_theta,
                t2_state=t2_state,
                ledger_dir=self.ledger_dir,
            )
            staged.extend(recs)
            if self.sink is not None and hold_reason is None:
                for rec in recs:
                    try:
                        self.sink.ingest_record(rec, trusted=self.sink_trusted)
                        sink_writes += 1
                    except Exception as exc:
                        hold_reason = f"{type(exc).__name__}: {exc}"
                        break
            rep["slices"].append({
                "turn_id": sl["turn_id"],
                "bounds": [list(r) for r in sl["bounds"]],
                "slice_kind": sl["slice_kind"],
                "boundary_id": sl.get("boundary_id"),
                "record_ids": [r["record_id"] for r in recs],
            })

        obs = self.breaker.observe(
            boundary_id=turns[-1]["msg_id"] if turns else rep["turn_ordinal"],
            items=t2_state.items_seen,
            below_floor=t2_state.items_below_floor,
        )
        if obs.get("tripped"):
            t2_state.degrade()

        rep["event"] = "cadence-boundary"
        rep["fired"] = True
        rep["evicted"] = bool(staged)
        rep["reason"] = "ok" if not hold_reason else "staging-hold"
        rep["records_total"] = len(staged)
        rep["staged_writes"] = sink_writes
        rep["held"] = hold_reason is not None
        rep["hold_reason"] = hold_reason
        if hold_reason:
            rep["drain_review_demanded"] = True
        rep["budget"] = {
            "cap_tokens": self.budget_cap,
            "tokens_used": t2_state.tokens_used,
            "truncated": t2_state.truncated,
            "items_seen": t2_state.items_seen,
            "items_below_floor": t2_state.items_below_floor,
            "items_t2_llm": t2_state.items_t2_llm,
        }
        rep["fallback_rate"] = t2_state.fallback_rate()
        rep["breaker"] = {
            "window": self.breaker.window,
            "threshold": self.breaker.threshold,
            "tripped": obs.get("tripped", self.breaker.tripped),
            "window_rate": obs.get("window_rate"),
            "degraded_t1_only": obs.get("degraded_t1_only", self.breaker.tripped),
            "stop_for_adjudication": obs.get("stop_for_adjudication",
                                             self.breaker.tripped),
            "trip_reason": obs.get("trip_reason"),
        }
        return rep
