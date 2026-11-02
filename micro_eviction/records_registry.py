"""mev.records_registry — the FORMS ESTATE (V3 S6, plan §8 REGISTRY / §10 row S6).

One producer per form. Only this module mints FORM IDENTITIES; every other
module *references* them. A form identity is the named epistemic role a record
type plays in the tracking estate (design §8), not the record itself.

Registered forms (charge CHARGE-S6-FORMS-ESTATE-V1 §Task 1):

    capture      — slice/proposal records gated at the eviction boundary
    promotion    — lifecycle promotion transitions (pattern/rule/calcified)
    staging      — records resident in the Engraphis `mev-staging` workspace
    state_record — durable cursor/state snapshots written through the io guard
    receipt      — per-stage gate receipts (schema owned by receipt_tools)
    gate_close   — seat close records written by the seat runner
    reverie      — RESERVED: ontology in design; schema placeholder only

Single-producer rule (mechanical, two-sided):
  * ``mint_form`` refuses when the *calling frame* is not this module
    (:class:`MintRefused`). A foreign module attempting to mint is refused, the
    form table is unchanged, and the refusal names the offending file.
  * Registration is idempotent: re-registering a byte-identical spec returns
    ``unchanged`` and performs no write; a divergent spec under an existing
    name is REFUSED (:class:`FormConflict`) — never silently overwritten.
  * The persisted table is byte-stable across a no-op re-registration
    (``minted_at`` is carried on the first mint and excluded from comparison).

Validation: ``validate_record(form, record)`` runs the form's schema
(required fields, identity pattern, enums, plus receipt_tools' own schema when
importable). ``assert_record`` raises :class:`RecordRejected` with the list.
The reserved ``reverie`` form has no ontology yet, so validation returns an
EXPLICIT reserved marker — it does not pretend to accept or reject records.

Persistence: ``<campaign>/reports/records_registry.json`` (charge §Task 1).
Writes go only to that file, through ``micro_eviction.io`` containment.

Read-only mirrors (plan §7.3/§8; derived at read time, NEVER persisted):
  * ``induction_ownership`` — ``memos_push.resolve_induction_ownership()``
    verbatim (no second source);
  * ``authorization`` — ``authorization.live_grant()`` verbatim.
Mirrors are live derivations on purpose: persisting a ``derived_at`` clock in
the forms table would break the byte-idempotency of registration.

Inert on import. No writes outside the registry file.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from . import io as mev_io

# ── seat identity (dispatch rule: every new S6 file carries it) ──────────────
SEAT_ID = "ENGINEER-deepseek-v4.1-flash-s6-forms-engineer"
SEAT_STAGE = "V3-S6"

DEFAULT_CAMPAIGN_ROOT = Path(
    os.environ.get(
        "MEV_CAMPAIGN_ROOT",
        os.environ.get("MEV_CAMPAIGN_ROOT", str(Path.home() / ".mev-cadence")),
    )
)

REGISTRY_RELPATH = Path("reports") / "records_registry.json"
REGISTRY_SCHEMA_VERSION = 1
GENERATOR = "micro_eviction/records_registry.py"

ACTIVE = "active"
RESERVED = "reserved"

_SELF_FILE = os.path.abspath(__file__)

# reservation marker for the in-flight `reverie` ontology (charge §Task 1)
REVERIE_RESERVED_REASON = (
    "reserved form: reverie ontology is in design; schema placeholder only "
    "(no record may be validated against a schema that does not exist yet)"
)

_ID_REC = re.compile(r"^rec_[0-9a-f]{16}$")

# ── immune predicate (S7 named referent: immune predicate = registry `is_immune`) ─
#
# The predicate lives HERE, in the forms estate, so no consumer invents its own
# immunity heuristic. A record is immune when an explicit carrier says so:
#   * `immune: true` / `is_immune: true` at the top level, or inside
#     `provenance` / `metadata` (the two containers the estate writes);
#   * `retention` in {permanent, immune} in any of those containers.
# Absence is NOT immunity: an untagged record is ordinary long-tail material.
IMMUNE_KEYS = ("immune", "is_immune")
IMMUNE_RETENTION_VALUES = ("permanent", "immune")


# ── errors ───────────────────────────────────────────────────────────────────

class RegistryError(RuntimeError):
    """Base class for loud registry refusals."""


class MintRefused(RegistryError):
    """One-producer violation: a form identity was minted from outside this module."""

    def __init__(self, caller: str):
        self.caller = caller
        super().__init__(
            "one-producer violation: mint_form called from "
            f"{caller!r}; only {GENERATOR} mints form identities"
        )


class FormConflict(RegistryError):
    """An existing form name was offered a divergent spec — refused, never overwritten."""

    def __init__(self, form: str):
        self.form = form
        super().__init__(
            f"form {form!r} already registered with a different spec; "
            "form identities are immutable (refusing to overwrite)"
        )


class RecordRejected(RegistryError):
    """A presented record failed its form schema."""

    def __init__(self, form: str, violations: list[str]):
        self.form = form
        self.violations = list(violations)
        super().__init__(
            f"record rejected for form {form!r}: " + "; ".join(self.violations)
        )


class RegistryCorrupt(RegistryError):
    """The persisted registry is unreadable or fails its own integrity check."""


# ── canonical serialisation ──────────────────────────────────────────────────

def _canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def structural_hash(forms: dict, schema_version: int = REGISTRY_SCHEMA_VERSION) -> str:
    """Deterministic schema digest over the form table.

    ``minted_at`` clocks are excluded: the digest identifies the *form→schema
    mapping*, so it is stable across a re-provision on a different day.
    """
    body = {"schema_version": schema_version,
            "forms": {name: _spec_body(rec) for name, rec in forms.items()}}
    return hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()


def _spec_body(form_record: dict) -> dict:
    """Spec identity for comparison — excludes the mint clock."""
    return {k: v for k, v in form_record.items() if k != "minted_at"}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ── the forms estate (minted by ensure_default_forms) ────────────────────────
#
# `source` tells the retrieval shim WHERE the form's record surface lives;
# `producer` names the single owning producer (the one-producer rule).
# `producer_evidence` is the on-disk citation the charge requires.

DEFAULT_FORMS: tuple[dict, ...] = (
    {
        "form": "capture",
        "status": ACTIVE,
        "version": 1,
        "producer": "micro_eviction/capture.py",
        "producer_symbol": "capture_slice",
        "producer_evidence": "micro_eviction/capture.py:329-446 (capture_slice)",
        "source": {
            "kind": "jsonl_glob",
            "glob": "ledgers/*.jsonl",
            "exclude_suffixes": [".spine.jsonl"],
            "exclude_names": [
                "lifecycle-transitions.jsonl",
                "conflict-queue.jsonl",
                "retraction-directives.jsonl",
            ],
            "require": ["record_id", "content"],
        },
        "identity": {"field": "record_id", "prefix": "rec_",
                     "pattern": "^rec_[0-9a-f]{16}$"},
        "required": ["record_id", "session_id", "type", "content",
                     "provenance", "scope"],
        # topic_hash is present on S2 slice records; sealed/batch ledger rows
        # predate it, so retrieval derives it via capture.topic_hash when absent.
        "optional": ["topic_hash", "turn_id", "bounds", "created_at",
                     "authoring_tier", "capture_sha", "supersedes",
                     "confidence", "mutable", "superseded_by", "memos"],
        "topic_hash": "topic_hash",
        "memos_pointer": "memos",
        "notes": "batch capture() is legacy bulk-import only (capture.py:60-63)",
    },
    {
        "form": "promotion",
        "status": ACTIVE,
        "version": 1,
        "producer": "micro_eviction/lifecycle.py",
        "producer_symbol": "append_promotions",
        "producer_evidence": (
            "micro_eviction/lifecycle.py:95-123 (append_promotions)"),
        "source": {
            "kind": "jsonl_glob",
            "glob": "ledgers/lifecycle-transitions.jsonl",
            "select": {"kind": "promotion"},
            "require": ["topic_hash"],
        },
        "identity": {"rule": "derived", "prefix": "prom_",
                     "basis": ["kind", "to", "topic_hash", "members"]},
        "required": ["kind", "to", "topic_hash", "type", "members", "sessions"],
        "optional": ["counts", "evidence", "via", "memos"],
        "enums": {"kind": ["promotion"],
                  "to": ["pattern", "rule", "calcified"]},
        "topic_hash": "topic_hash",
        "memos_pointer": "memos",
        "notes": "promotion rows carry no record_id; identity is derived",
    },
    {
        "form": "staging",
        "status": ACTIVE,
        "version": 1,
        "producer": "engraphis_sink.py",
        "producer_symbol": "EngraphisSink.ingest_record",
        "producer_evidence": (
            "engraphis_sink.py:252-296 (ingest_record; the only staging writer)"),
        "source": {
            "kind": "engraphis_workspace",
            "workspace": "mev-staging",
            "metadata_field": "record_id",
        },
        "identity": {"field": "metadata.record_id", "prefix": "rec_",
                     "pattern": "^rec_[0-9a-f]{16}$"},
        "required": ["metadata.record_id", "content", "metadata.topic_hash",
                     "metadata.capture_sha", "metadata.type"],
        "optional": ["metadata.session_id", "metadata.bounds",
                     "metadata.authoring_tier", "metadata.supersedes",
                     "metadata.memos"],
        "topic_hash": "metadata.topic_hash",
        "memos_pointer": "metadata.memos",
        "notes": "live surface read via sqlite mode=ro (staging_provider.py:91-100)",
    },
    {
        "form": "state_record",
        "status": ACTIVE,
        "version": 1,
        "producer": "micro_eviction/io.py",
        "producer_symbol": "safe_write",
        "producer_evidence": (
            "micro_eviction/io.py:27-42 (safe_write: the single guarded write path "
            "every durable state snapshot must pass)"),
        "source": {
            "kind": "json_glob",
            "glob": ["ledgers/.slice_cursor_*.json",
                     "ledgers/.memos_dispatcher_cursor.json"],
        },
        "identity": {"rule": "file", "prefix": "state_"},
        # A durable state snapshot is opaque by design: its schema is the
        # identity + write-path contract, not a key set. Requiring keys here
        # would reject the cursor shapes the estate actually writes.
        "required": [],
        "optional": ["byte_offset", "row_count", "last_boundary_id",
                     "last_processed_max_msg_id", "last_turn_ordinal",
                     "seen_directive_ids", "ts", "last_run_ts", "last_run",
                     "reason", "relock_stage", "shas"],
        "topic_hash": None,
        "memos_pointer": None,
        "notes": "state snapshots have one producer: the write-path guard",
    },
    {
        "form": "receipt",
        "status": ACTIVE,
        "version": 1,
        "producer": "receipt_tools.py",
        "producer_symbol": "write_receipt",
        "producer_evidence": (
            "receipt_tools.py:242-253 (write_receipt: schema-validated, atomic); "
            "schema keys at receipt_tools.py:39-44"),
        "source": {
            "kind": "json_glob",
            # R10 per-stage gate receipts ONLY. The A1 activation/rollback
            # receipts and the seal report (verify_report.json) are activation
            # artifacts with different schemas — outside the seven-form estate,
            # not silently folded into `receipt`.
            "glob": ["reports/stage_S*_receipt.json"],
        },
        "identity": {"rule": "file", "prefix": "rcpt_"},
        "required": ["stage", "gate_cmd", "verdict", "ts", "upstream_shas",
                     "gate_output"],
        "optional": ["tokens_in", "tokens_out", "derived_usd", "dollars_label",
                     "t2_fallback_rate", "fixtures", "gate_output_tail",
                     "gate_evidence", "applied_corrections", "machine_state",
                     "plan_findings"],
        "enums": {"verdict": ["PASS", "FAIL"]},
        "topic_hash": None,
        "memos_pointer": None,
        "notes": ("required keys mirror receipt_tools.RECEIPT_REQUIRED_KEYS; "
                  "non-conforming on-disk receipts are FLAGGED, not hidden"),
    },
    {
        "form": "gate_close",
        "status": ACTIVE,
        "version": 1,
        "producer": "tools/cmd-seat-run.sh",
        "producer_symbol": "close-record writer",
        "producer_evidence": (
            "tools/cmd-seat-run.sh:229-251 (writes .state/close-record.json + "
            "CLOSE.md at seat close)"),
        "source": {
            "kind": "markdown_glob",
            "glob": ["work-*/CLOSE*.md", "CLOSE*.md"],
        },
        "identity": {"rule": "file", "prefix": "close_"},
        # close records are free-form markdown; the schema is the form's
        # identity + producer contract, not a key set
        "required": [],
        "optional": [],
        "topic_hash": None,
        "memos_pointer": None,
        "notes": "gate close records are markdown; identity is path-derived",
    },
    {
        "form": "reverie",
        "status": RESERVED,
        "version": 0,
        "producer": None,
        "producer_symbol": None,
        "producer_evidence": "none — ontology in design",
        "source": {"kind": "reserved"},
        "identity": {"field": "record_id", "pattern": None},
        # placeholder envelope only; NOT a schema (ontology in design)
        "required": ["record_id", "content"],
        "optional": [],
        "enums": {},
        "topic_hash": "topic_hash",
        "memos_pointer": None,
        "notes": REVERIE_RESERVED_REASON,
    },
)


# ── S7 cadence forms (registered by ensure_cadence_forms, never by default) ──
#
# CHARGE-S7-ENTROPY-CADENCE-V1 deliverables 2 and 4: the long-tail index and the
# pass receipt are REGISTRY FORMS, and cadence is their single producer. They
# are a separate table from DEFAULT_FORMS on purpose: the S6 estate is frozen at
# seven forms (selftest_v3_s6 asserts it), so S7 activation is an explicit,
# idempotent ADDITION rather than a silent widening of the S6 surface.

CADENCE_FORMS: tuple[dict, ...] = (
    {
        "form": "pass_receipt",
        "status": ACTIVE,
        "version": 1,
        "producer": "micro_eviction/cadence.py",
        "producer_symbol": "CadenceScheduler.run_pass",
        "producer_evidence": (
            "micro_eviction/cadence.py write_pass_receipt (the only pass writer); "
            "one receipt per receipt-bearing pass (A2/A8)"),
        "source": {
            "kind": "json_glob",
            "glob": "reports/pass_receipts/pass_*.json",
        },
        "identity": {"field": "pass_id", "prefix": "pass_",
                     "pattern": "^pass_[0-9a-f]{16}$"},
        # the six R10 receipt keys are carried VERBATIM (A12 discipline) and
        # extended with the cadence fields a pass must state.
        "required": ["pass_id", "slot_key", "stage", "gate_cmd", "verdict", "ts",
                     "upstream_shas", "gate_output", "outcome", "fire_at",
                     "interval_s", "next_fire_at", "window", "encounter_signal",
                     "longtail"],
        "optional": ["missed_passes", "cap_state", "at_cap", "cap_basis",
                     "drain_review_demanded", "promotion_review_demanded",
                     "dedup_of", "notes", "failures"],
        "enums": {
            "verdict": ["PASS", "FAIL", "HOLD"],
            "outcome": ["STAGED", "NOOP", "DEDUPED", "HOLD_AT_CAP",
                        "FAILED", "PARTIAL"],
        },
        "topic_hash": None,
        "memos_pointer": None,
        "notes": ("core keys mirror receipt_tools.RECEIPT_REQUIRED_KEYS; "
                  "verdict HOLD is the A11 at-cap state (not a FAIL, not a skip)"),
    },
    {
        "form": "longtail_index",
        "status": ACTIVE,
        "version": 1,
        "producer": "micro_eviction/cadence.py",
        "producer_symbol": "rebuild_index",
        "producer_evidence": (
            "micro_eviction/cadence.py rebuild_index (derived data: recomputed "
            "from pass receipts + capture rows, atomic write)"),
        "source": {
            "kind": "jsonl_glob",
            "glob": "longtail/index.jsonl",
            "require": ["record_id", "state"],
        },
        # pointer identity: the index row IS a pointer to a capture record
        "identity": {"field": "record_id", "prefix": "rec_",
                     "pattern": "^rec_[0-9a-f]{16}$"},
        "required": ["record_id", "state", "created_at", "staged_at",
                     "first_seen_pass", "encounters", "spine_ref"],
        "optional": ["topic_hash", "type", "age_s", "age_days",
                     "last_encounter_fire_at", "drain_review_demanded",
                     "promotion_review_demanded", "retired_at", "directives",
                     "meta_missing"],
        "enums": {"state": ["resident", "aging", "rising", "drain_candidate",
                            "decayed", "retired"]},
        "topic_hash": None,
        "memos_pointer": None,
        "notes": "payload-free ordered pointers (R18); rebuildable derived data",
    },
)


def is_immune(record) -> bool:
    """The registry's immune predicate (S7 §6 named referent).

    Immune records are never indexed by the long tail and never touched by a
    cadence pass (charge §3 safety / A6). The rule is explicit and carrier-based;
    nothing is inferred from content or type.
    """
    if not isinstance(record, dict):
        return False
    containers = [record]
    for key in ("provenance", "metadata"):
        node = record.get(key)
        if isinstance(node, dict):
            containers.append(node)
    for node in containers:
        for key in IMMUNE_KEYS:
            if node.get(key) is True:
                return True
        retention = node.get("retention")
        if isinstance(retention, str) and retention.strip().lower() in IMMUNE_RETENTION_VALUES:
            return True
    return False



# ── the registry ─────────────────────────────────────────────────────────────

class RecordsRegistry:
    """Single-producer registry of record forms (persisted JSON)."""

    def __init__(self, campaign_root=None, *, registry_path=None):
        self.campaign_root = Path(campaign_root) if campaign_root else DEFAULT_CAMPAIGN_ROOT
        self.path = (Path(registry_path) if registry_path
                     else self.campaign_root / REGISTRY_RELPATH)
        self._doc = self._read()

    # ── persistence ──

    def _empty_doc(self) -> dict:
        return {"schema_version": REGISTRY_SCHEMA_VERSION,
                "generator": GENERATOR,
                "forms": {},
                "summary": self._summary({}),
                "structural_hash": structural_hash({})}

    def _summary(self, forms: dict) -> dict:
        active = sorted(n for n, f in forms.items() if f.get("status") == ACTIVE)
        reserved = sorted(n for n, f in forms.items() if f.get("status") == RESERVED)
        producers = sorted({f["producer"] for f in forms.values()
                            if f.get("producer")})
        # NOTE (S7 defect fix): the flag used to demand one DISTINCT producer per
        # active form, which is a different rule than the one-producer law. The
        # law is "each active form names exactly one producer" — a single
        # producer legitimately owns several forms (cadence owns the index AND
        # the pass receipt, plan §8). `producer_forms` keeps the ownership map
        # visible so the invariant is auditable either way.
        producer_forms: dict[str, list[str]] = {}
        for name in active:
            producer_forms.setdefault(forms[name]["producer"], []).append(name)
        return {"forms": len(forms), "active": len(active),
                "reserved": len(reserved), "active_names": active,
                "reserved_names": reserved, "producers": producers,
                "producer_forms": {p: sorted(v)
                                   for p, v in sorted(producer_forms.items())},
                "one_producer_per_active_form": all(
                    bool(f.get("producer")) for f in forms.values()
                    if f.get("status") == ACTIVE)}

    def _read(self) -> dict:
        p = self.path
        if not p.is_file():
            return self._empty_doc()
        try:
            doc = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise RegistryCorrupt(f"unreadable registry {p}: {exc}") from exc
        if not isinstance(doc, dict) or "forms" not in doc:
            raise RegistryCorrupt(f"registry {p} is not a form table")
        if doc.get("schema_version") != REGISTRY_SCHEMA_VERSION:
            raise RegistryCorrupt(
                f"registry {p} schema_version={doc.get('schema_version')!r} "
                f"!= {REGISTRY_SCHEMA_VERSION}")
        forms = doc["forms"]
        if not isinstance(forms, dict):
            raise RegistryCorrupt(f"registry {p} forms is not an object")
        for name, rec in forms.items():
            self._validate_form_spec(rec, expected_name=name)
        digest = structural_hash(forms)
        if doc.get("structural_hash") not in (None, digest):
            raise RegistryCorrupt(
                f"registry {p} structural_hash mismatch "
                f"({doc.get('structural_hash')} != {digest})")
        doc["structural_hash"] = digest
        doc["summary"] = self._summary(forms)
        return doc

    def _snapshot(self) -> dict:
        return {"schema_version": REGISTRY_SCHEMA_VERSION,
                "generator": GENERATOR,
                "forms": self._doc["forms"],
                "summary": self._summary(self._doc["forms"]),
                "structural_hash": structural_hash(self._doc["forms"])}

    def save(self, *, force: bool = False) -> bool:
        """Persist; byte-identical no-op writes nothing (idempotency at disk)."""
        text = json.dumps(self._snapshot(), sort_keys=True, indent=1,
                          ensure_ascii=False) + "\n"
        if not force and self.path.is_file():
            try:
                if self.path.read_text(encoding="utf-8") == text:
                    return False
            except OSError:
                pass
        mev_io.allow(self.path.parent)
        mev_io.safe_write(self.path, text)
        return True

    # ── form-spec validation ──

    def _validate_form_spec(self, spec: dict, *, expected_name=None) -> None:
        if not isinstance(spec, dict):
            raise RegistryCorrupt(f"form spec is not an object: {spec!r}")
        name = spec.get("form")
        if not isinstance(name, str) or not name:
            raise RegistryCorrupt("form spec missing a name")
        if expected_name is not None and name != expected_name:
            raise RegistryCorrupt(
                f"form spec name {name!r} != table key {expected_name!r}")
        status = spec.get("status")
        if status not in (ACTIVE, RESERVED):
            raise RegistryCorrupt(f"form {name!r} has invalid status {status!r}")
        producer = spec.get("producer")
        if status == ACTIVE:
            if not isinstance(producer, str) or not producer.strip():
                raise RegistryCorrupt(
                    f"form {name!r} is active but names no single producer")
        if not isinstance(spec.get("source"), dict):
            raise RegistryCorrupt(f"form {name!r} missing source descriptor")
        if not isinstance(spec.get("identity"), dict):
            raise RegistryCorrupt(f"form {name!r} missing identity rule")
        if not isinstance(spec.get("required"), list):
            raise RegistryCorrupt(f"form {name!r} required must be a list")

    # ── minting (the one-producer gate) ──

    def mint_form(self, spec: dict) -> dict:
        """Mint a form identity. REFUSED unless called from this module.

        This is the mechanical one-producer rule: no foreign module can add a
        form. Internal callers (``ensure_default_forms``) pass the guard.
        """
        caller = os.path.abspath(sys._getframe(1).f_code.co_filename)
        if caller != _SELF_FILE:
            raise MintRefused(caller)
        return self._mint(spec)

    def _mint(self, spec: dict, *, now: str | None = None) -> dict:
        """Internal mint path (unwrapped by design — the public gate is above)."""
        self._validate_form_spec(spec)
        name = spec["form"]
        record = dict(spec)
        record.setdefault("minted_at", now or _utcnow())
        existing = self._doc["forms"].get(name)
        if existing is not None:
            if _canonical(_spec_body(existing)) == _canonical(_spec_body(record)):
                return {"form": name, "status": "unchanged", "written": False}
            raise FormConflict(name)
        self._doc["forms"][name] = record
        return {"form": name, "status": "minted", "written": False}

    def ensure_default_forms(self) -> dict:
        """Register the seven forms idempotently (the only public registration)."""
        minted, unchanged = [], []
        for spec in DEFAULT_FORMS:
            result = self.mint_form(spec)  # internal caller -> guard passes
            (minted if result["status"] == "minted" else unchanged).append(
                result["form"])
        written = self.save()
        return {"path": str(self.path), "minted": minted, "unchanged": unchanged,
                "written": written, "forms": len(self._doc["forms"]),
                "structural_hash": structural_hash(self._doc["forms"]),
                "summary": self._summary(self._doc["forms"])}

    def ensure_cadence_forms(self) -> dict:
        """Register the S7 cadence forms idempotently (S7 activation step).

        Additive and idempotent: the S6 seven-form estate is untouched, and a
        re-run writes nothing (byte-identical table). The mint guard still
        applies — a foreign module cannot add a cadence form either.
        """
        minted, unchanged = [], []
        for spec in CADENCE_FORMS:
            result = self.mint_form(spec)  # internal caller -> guard passes
            (minted if result["status"] == "minted" else unchanged).append(
                result["form"])
        written = self.save()
        return {"path": str(self.path), "minted": minted, "unchanged": unchanged,
                "written": written, "forms": len(self._doc["forms"]),
                "structural_hash": structural_hash(self._doc["forms"]),
                "summary": self._summary(self._doc["forms"])}

    # ── readers ──

    def get(self, form: str) -> dict:
        rec = self._doc["forms"].get(form)
        if rec is None:
            raise RegistryError(f"unknown form {form!r}; registered: "
                                f"{sorted(self._doc['forms'])}")
        return rec

    def forms(self) -> dict:
        return self._doc["forms"]

    def form_names(self) -> list[str]:
        return sorted(self._doc["forms"])

    def describe(self) -> dict:
        return {"path": str(self.path),
                "registry": self._snapshot(),
                "mirrors": self.mirrors()}

    def mirrors(self) -> dict:
        """Read-time mirrors (plan §7.3/§8). Never persisted, never second-sourced."""
        out: dict = {}
        try:
            import memos_push  # campaign-root module; lazy, read-only
            out["induction_ownership"] = memos_push.resolve_induction_ownership()
        except Exception as exc:  # loud in the payload, never fatal
            out["induction_ownership"] = {
                "effective": "unresolved-gap",
                "flag_source": "unavailable",
                "error": f"{type(exc).__name__}: {exc}",
            }
        charge = (self.campaign_root.parent.parent.parent / "charges"
                  / "CHARGE-S6-FORMS-ESTATE-V1-2026-10-29.md")
        if charge.is_file():
            try:
                from . import authorization as auth
                out["authorization"] = auth.live_grant(charge)
            except Exception as exc:
                out["authorization"] = {"error": f"{type(exc).__name__}: {exc}"}
        else:
            out["authorization"] = {
                "grant": None, "sandbox_only": True, "status": "absent",
                "reason": "no charge file at the derived path",
                "charge_path": str(charge),
            }
        return out

    # ── record validation ──

    def validate_record(self, form: str, record) -> list[str]:
        """Validate a presented record against its form schema.

        Returns a list of violations; empty == accepted. An unknown form raises;
        the reserved form returns an explicit reserved marker (never a silent
        accept, never a fake reject).
        """
        spec = self.get(form)
        if spec.get("status") == RESERVED:
            return [REVERIE_RESERVED_REASON]
        if not isinstance(record, dict):
            return [f"record is not an object: {type(record).__name__}"]

        violations: list[str] = []
        for path in spec.get("required") or []:
            if _get_path(record, path) is _MISSING:
                violations.append(f"missing field: {path}")

        ident = spec.get("identity") or {}
        field = ident.get("field")
        pattern = ident.get("pattern")
        if field and pattern:
            value = _get_path(record, field)
            if value is _MISSING:
                violations.append(f"missing identity field: {field}")
            elif not isinstance(value, str) or not re.match(pattern, value):
                violations.append(
                    f"identity {field} does not match {pattern}: {value!r}")

        for path, allowed in (spec.get("enums") or {}).items():
            value = _get_path(record, path)
            if value is _MISSING:
                continue
            if value not in allowed:
                violations.append(
                    f"field {path} not in {allowed}: {value!r}")

        if form == "receipt":
            violations.extend(_receipt_tools_violations(record))
        # collapse byte-identical messages; the two schema sources word the
        # same defect differently on purpose (independent agreement is evidence)
        return list(dict.fromkeys(violations))

    def assert_record(self, form: str, record) -> dict:
        violations = self.validate_record(form, record)
        if violations:
            raise RecordRejected(form, violations)
        return record


# ── field-path + receipt-tools helpers ───────────────────────────────────────

_MISSING = object()


def _get_path(record: dict, path: str):
    """Dotted-path lookup (``metadata.record_id`` / ``_record_id``)."""
    cur = record
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return _MISSING
        cur = cur[part]
    return cur


def _receipt_tools_violations(record: dict) -> list[str]:
    """Union receipt_tools' own schema when importable (single source, no fork)."""
    try:
        import receipt_tools
    except Exception:
        return []
    try:
        return list(receipt_tools.validate_receipt(record))
    except Exception as exc:
        return [f"receipt_tools.validate_receipt failed: {exc}"]


__all__ = [
    "SEAT_ID", "SEAT_STAGE", "DEFAULT_CAMPAIGN_ROOT", "REGISTRY_RELPATH",
    "REGISTRY_SCHEMA_VERSION", "GENERATOR", "ACTIVE", "RESERVED",
    "REVERIE_RESERVED_REASON", "DEFAULT_FORMS", "CADENCE_FORMS",
    "IMMUNE_KEYS", "IMMUNE_RETENTION_VALUES", "is_immune",
    "RegistryError", "MintRefused", "FormConflict", "RecordRejected",
    "RegistryCorrupt",
    "structural_hash", "RecordsRegistry",
]
