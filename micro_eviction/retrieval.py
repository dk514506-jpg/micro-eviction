"""mev.retrieval — Engraphis-mediated READ-ONLY retrieval over record surfaces (V3 S6).

Plan §2 R13 / §10 row S6; charge CHARGE-S6-FORMS-ESTATE-V1 §Task 2.

What this module is
-------------------
The single query surface over the forms estate. It reads:
  * the forms registry (``records_registry.json``) for what exists and how each
    form's records are addressed;
  * the on-disk record surfaces named by each form's ``source`` descriptor
    (capture/promotion ledgers, receipt/close/state files, the Engraphis
    ``mev-staging`` workspace);
  * MemOS consolidated views — ONLY where the record itself carries an
    EXPLICIT mapping pointer (``memos`` / ``memos_node_id`` inside the record
    or its staging metadata). No pointer means ``unmapped`` — an explicit
    answer, not silence. There is no fuzzy/implied join.

What this module is NOT
-----------------------
No write path. There is no ingest symbol, no push, no HTTP, no ``safe_write``,
no ``append_jsonl``; the import graph is clean of ``engraphis_sink`` and
``memos_push`` (asserted two-sided in ``selftest_v3_s6.py``). Both sqlite
stores are opened with a read-only URI (``?mode=ro``) plus
``PRAGMA query_only=ON``; :meth:`Retrieval.readonly_report` proves the
discipline by probing that writes are refused.

Retraction / staleness (the S6 stub — no MemOS writes, by design)
------------------------------------------------------------------
A mapping is reported STALE — in a visible result with a marker, never as
empty output — when any of these hold:
  * the record is directive-hit upstream (``directives.is_retracted``, the S6
    retraction-propagation stub: mark the mapping stale, do not touch MemOS);
  * the mapping pointer itself says ``retracted`` / ``stale``;
  * the pointer is incomplete (a pointer without ``node_id``);
  * the MemOS node is absent, the store is unavailable/unreadable, or the
    node is time-invalid (``valid_to`` set / ``expires_at`` passed).
A stale/retracted resolution NEVER returns node content — nothing from MemOS is
surfaced — but the result is explicit (``status: "stale"`` + ``marker``).

Every result carries the read-only evidence (``ro``) it was produced under.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path

from . import directives as dirmod
from .records_registry import (
    REVERIE_RESERVED_REASON,
    RESERVED,
    RecordsRegistry,
    _get_path,
    _MISSING,
)

# ── seat identity (dispatch rule: every new S6 file carries it) ──────────────
SEAT_ID = "ENGINEER-deepseek-v4.1-flash-s6-forms-engineer"
SEAT_STAGE = "V3-S6"

DEFAULT_CAMPAIGN_ROOT = Path(
    os.environ.get(
        "MEV_CAMPAIGN_ROOT",
        os.environ.get("MEV_CAMPAIGN_ROOT", str(Path.home() / ".mev-cadence")),
    )
)
DEFAULT_ENGRAPHIS_DB = Path(
    os.environ.get("ENGRAPHIS_LIVE_DB", "~/.engraphis/memory.db")
).expanduser()
DEFAULT_MEMOS_DB = Path(
    os.environ.get(
        "MEMOS_DB_PATH", "~/scratch/mem-migration/lab/memos-db/memos.db")
).expanduser()

STAGING_WORKSPACE = "mev-staging"

# ── hints channel (CHARGE-REVERIE-V1 §3.2/§5; ADDITIVE) ─────────────────────
#
# Sparks NEVER enter a ranked list. They surface ONLY through the separate
# `hints` return field, weight-capped and is_immune-filtered, enforced here at
# the retrieval seam (not merely inside the provider). The reverie-staging
# workspace is read directly (the registry `reverie` form is still the S6
# RESERVED placeholder; this charge may not edit S6 files).
REVERIE_STAGING_WORKSPACE = "reverie-staging"
SPARK_FORM = "reverie"
HINT_WEIGHT_CAP = 0.35
HINT_STATUS_DECAYED = "DECAYED"

# resolution statuses (the vocabulary is the contract)
OK = "ok"
NOT_FOUND = "not_found"
RESERVED_RESULT = "reserved"
UNMAPPED = "unmapped"
STALE = "stale"

MARKER_SOURCE_MISSING = "MAPPING_SOURCE_MISSING"
MARKER_STALE = "MAPPING_STALE"
MARKER_RETRACTED = "MAPPING_RETRACTED"

_NO_MAPPING = {"status": UNMAPPED, "reason": "no_explicit_mapping",
               "marker": None}


# ── read-only sqlite discipline ─────────────────────────────────────────────

def open_readonly(path) -> tuple[sqlite3.Connection, str]:
    """Open a sqlite file with a read-only URI; refuse anything less."""
    p = Path(path).expanduser()
    if not p.is_file():
        raise FileNotFoundError(f"store not found: {p}")
    uri = p.resolve().as_uri() + "?mode=ro"
    if "mode=ro" not in uri:  # belt and braces
        raise AssertionError("read-only URI discipline violated")
    conn = sqlite3.connect(uri, uri=True, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn, uri


def write_refused(conn: sqlite3.Connection) -> bool:
    """Probe that this connection cannot write. Read-only, leaves no state.

    ``BEGIN IMMEDIATE`` demands a write lock; on a ``mode=ro`` /
    ``query_only`` connection sqlite refuses it. Any other outcome is a
    discipline failure reported as False (fail closed, loudly upstream).
    """
    try:
        conn.rollback()
    except sqlite3.Error:
        pass
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.Error as exc:
        msg = str(exc).lower()
        return ("readonly" in msg or "read-only" in msg
                or "query_only" in msg)
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass
    return False


def _sha12(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]


def _jload(raw):
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw or "{}")
    except (TypeError, ValueError):
        return {}


# ── retrieval ───────────────────────────────────────────────────────────────

class Retrieval:
    """Read-only retrieval over the forms estate (one query surface)."""

    def __init__(self, campaign_root=None, *, registry=None, registry_path=None,
                 engraphis_db=None, memos_db=None):
        self.campaign_root = (Path(campaign_root) if campaign_root
                              else DEFAULT_CAMPAIGN_ROOT)
        self.registry = registry or RecordsRegistry(
            self.campaign_root, registry_path=registry_path)
        self.provisioned_registry = False
        if not self.registry.forms():
            self.registry.ensure_default_forms()
            self.provisioned_registry = True
        self.engraphis_db = (Path(engraphis_db).expanduser() if engraphis_db
                             else DEFAULT_ENGRAPHIS_DB)
        self.memos_db = (Path(memos_db).expanduser() if memos_db
                         else DEFAULT_MEMOS_DB)
        self._conns: dict[str, sqlite3.Connection] = {}
        self._uris: dict[str, str] = {}
        self._directives_cache = None
        self._ro_cache = None
        self.source_failures: list[dict] = []

    # ── store handles ──

    def _conn(self, which: str) -> sqlite3.Connection:
        if which not in self._conns:
            path = self.engraphis_db if which == "engraphis" else self.memos_db
            conn, uri = open_readonly(path)
            self._conns[which] = conn
            self._uris[which] = uri
        return self._conns[which]

    def close(self) -> None:
        for conn in self._conns.values():
            try:
                conn.close()
            except sqlite3.Error:
                pass
        self._conns.clear()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def readonly_report(self) -> dict:
        """Evidence: both stores opened mode=ro + query_only, writes refused."""
        if self._ro_cache is not None:
            return self._ro_cache
        out: dict = {"discipline": "sqlite-ro-uri+query_only", "memos_write": False}
        for name, path in (("engraphis", self.engraphis_db),
                           ("memos", self.memos_db)):
            entry = {"path": str(path), "exists": path.is_file()}
            if path.is_file():
                try:
                    conn = self._conn(name)
                    entry["uri"] = self._uris[name]
                    entry["mode_ro"] = "mode=ro" in self._uris[name]
                    row = conn.execute("PRAGMA query_only").fetchone()
                    entry["query_only"] = int(row[0]) if row else None
                    entry["write_refused"] = write_refused(conn)
                except (sqlite3.Error, FileNotFoundError) as exc:
                    entry["error"] = f"{type(exc).__name__}: {exc}"
                    entry["mode_ro"] = False
            out[name] = entry
        self._ro_cache = out
        return out

    # ── directives (retraction propagation source) ──

    def _directives(self) -> list[dict]:
        if self._directives_cache is None:
            path = dirmod.derive_directives_path(self.campaign_root / "calcified.jsonl")
            try:
                self._directives_cache = dirmod.load_directives(path)
            except Exception:
                self._directives_cache = []
        return self._directives_cache

    # ── source readers (driven by the registry's source descriptor) ──

    def _patterns(self, spec) -> list[str]:
        glob = (spec.get("source") or {}).get("glob")
        if glob is None:
            return []
        return [glob] if isinstance(glob, str) else list(glob)

    def _iter_rows(self, form: str) -> list[dict]:
        spec = self.registry.get(form)
        src = spec.get("source") or {}
        kind = src.get("kind")
        if kind == "reserved":
            return []
        if kind == "jsonl_glob":
            return self._read_jsonl(spec)
        if kind == "engraphis_workspace":
            return self._read_engraphis(spec)
        if kind == "json_glob":
            return self._read_json_files(spec)
        if kind == "markdown_glob":
            return self._read_markdown(spec)
        raise ValueError(f"form {form!r}: unknown source kind {kind!r}")

    def _match_file(self, path: Path, src: dict) -> bool:
        name = path.name
        for suffix in src.get("exclude_suffixes") or []:
            if name.endswith(suffix):
                return False
        return name not in set(src.get("exclude_names") or [])

    def _read_jsonl(self, spec: dict) -> list[dict]:
        src = spec["source"]
        select = src.get("select") or {}
        required = src.get("require") or []
        after = self._derive_after(spec)
        out: list[dict] = []
        for pattern in self._patterns(spec):
            for path in sorted(self.campaign_root.glob(pattern)):
                if not path.is_file() or not self._match_file(path, src):
                    continue
                for line_no, line in enumerate(
                        path.read_text(encoding="utf-8").splitlines(), 1):
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    if not isinstance(row, dict):
                        continue
                    if any(row.get(k) != v for k, v in select.items()):
                        continue
                    if any(row.get(k) in (None, "") for k in required):
                        continue
                    out.append(self._envelope(
                        spec, row, source={"path": str(path), "line": line_no},
                        after=after))
        return out

    def _read_engraphis(self, spec: dict) -> list[dict]:
        src = spec["source"]
        workspace = src.get("workspace") or STAGING_WORKSPACE
        out: list[dict] = []
        try:
            conn = self._conn("engraphis")
            rows = conn.execute(
                "SELECT m.id AS store_id, m.content AS content, m.mtype AS mtype, "
                "m.metadata AS metadata, m.provenance AS provenance "
                "FROM memories m JOIN workspaces w ON w.id = m.workspace_id "
                "WHERE w.name = ?", (workspace,)).fetchall()
        except (sqlite3.Error, FileNotFoundError) as exc:
            self.source_failures.append(
                {"form": spec["form"], "reason": "engraphis_store_unavailable",
                 "path": str(self.engraphis_db),
                 "error": f"{type(exc).__name__}: {exc}"})
            return out
        for row in rows:
            meta = _jload(row["metadata"])
            payload = {"metadata": meta,
                       "_store": {"store_id": row["store_id"],
                                  "mtype": row["mtype"],
                                  "workspace": workspace,
                                  "provenance": _jload(row["provenance"])}}
            out.append(self._envelope(
                spec, payload,
                source={"path": f"{self.engraphis_db}"
                                f"#workspace={workspace}"
                                f"#memory={row['store_id']}",
                        "line": None},
                content=row["content"] or "", after=None))
        return out

    def _read_json_files(self, spec: dict) -> list[dict]:
        out: list[dict] = []
        for pattern in self._patterns(spec):
            for path in sorted(self.campaign_root.glob(pattern)):
                if not path.is_file():
                    continue
                try:
                    doc = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    doc = {"_unreadable": True}
                if not isinstance(doc, dict):
                    doc = {"_value": doc}
                out.append(self._envelope(
                    spec, doc, source={"path": str(path), "line": 1},
                    content="", after=None))
        return out

    def _read_markdown(self, spec: dict) -> list[dict]:
        out: list[dict] = []
        for pattern in self._patterns(spec):
            for path in sorted(self.campaign_root.glob(pattern)):
                if not path.is_file():
                    continue
                try:
                    text = path.read_text(encoding="utf-8")
                except OSError:
                    text = ""
                out.append(self._envelope(
                    spec, {"text": text},
                    source={"path": str(path), "line": 1}, content=text,
                    after=None))
        return out

    # ── envelope construction ──

    def _derive_after(self, spec: dict):
        """Identity rule for row forms that must derive their own record_id."""
        ident = spec.get("identity") or {}
        if ident.get("rule") == "derived":
            basis = ident.get("basis") or []

            def derive(row: dict) -> str:
                body = {k: row.get(k) for k in basis}
                digest = hashlib.sha256(
                    json.dumps(body, sort_keys=True, separators=(",", ":"),
                               ensure_ascii=False).encode("utf-8")).hexdigest()
                return ident.get("prefix", "der_") + digest[:16]
            return derive
        return None

    def _record_id_for_file(self, spec: dict, path: Path) -> str:
        ident = spec.get("identity") or {}
        prefix = ident.get("prefix") or "rec_"
        try:
            rel = str(path.relative_to(self.campaign_root))
        except ValueError:
            rel = str(path)
        return prefix + _sha12(rel)

    def _envelope(self, spec: dict, payload: dict, *, source: dict,
                  content: str | None = None, after=None) -> dict:
        form = spec["form"]
        ident = spec.get("identity") or {}
        record_id = None
        if after is not None:
            record_id = after(payload)
        if record_id is None and ident.get("field"):
            value = _get_path(payload, ident["field"])
            if value is not _MISSING and value not in (None, ""):
                record_id = value
        if record_id is None and ident.get("rule") == "file":
            record_id = self._record_id_for_file(spec, Path(source["path"]))
        if record_id is None:
            record_id = spec.get("form", "rec") + "_" + _sha12(
                json.dumps(payload, sort_keys=True, default=str))

        if content is None:
            value = payload.get("content")
            content = value if isinstance(value, str) else ""

        th_path = spec.get("topic_hash")
        topic_hash = None
        if th_path:
            value = _get_path(payload, th_path)
            if value is not _MISSING and isinstance(value, str) and value:
                topic_hash = value
        if topic_hash is None and form == "capture":
            topic_hash = self._derive_topic_hash(payload, content)

        return {
            "form": form,
            "record_id": record_id,
            "topic_hash": topic_hash,
            "content": content,
            "source": source,
            "payload": payload,
            "_memos_pointer": self._pointer(spec, payload),
        }

    def _derive_topic_hash(self, payload: dict, content: str):
        """Single source: capture.topic_hash (never a forked rule)."""
        if not content:
            return None
        try:
            from .capture import topic_hash as _topic_hash
            return _topic_hash(payload.get("type") or "", content)
        except Exception:
            return None

    def _pointer(self, spec: dict, payload: dict):
        path = spec.get("memos_pointer")
        if not path:
            return None
        value = _get_path(payload, path)
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, str) and value.strip():
            return {"node_id": value.strip(), "status": "active"}
        nid = payload.get("memos_node_id")
        if isinstance(nid, str) and nid.strip():
            return {"node_id": nid.strip(),
                    "status": payload.get("memos_status", "active")}
        return None

    # ── MemOS mapping resolution (explicit pointers only) ──

    def resolve(self, record: dict) -> dict:
        """Resolve the record's MemOS consolidated view, or report why not.

        Returns ``status`` in {ok, unmapped, stale}. STALE is a visible answer
        with a marker; it returns NO node content and writes nothing to MemOS.
        """
        rid = record.get("record_id")
        base = {"form": record.get("form"), "record_id": rid,
                "topic_hash": record.get("topic_hash"),
                "memos_write": False, "ro": self.readonly_report()}
        pointer = record.get("_memos_pointer")

        # (1) upstream directive retraction — the S6 propagation stub. Checked
        # BEFORE the pointer test: a retracted record is never served silently,
        # even when it carries no mapping (then `mapping` is None but the
        # result is an explicit STALE, not an empty answer).
        directive = None
        try:
            directive = dirmod.hit(
                self._directives(), topic_hash=record.get("topic_hash"),
                record_ids=[rid] if rid else None,
                content=record.get("content") or None)
        except Exception:
            directive = None
        if directive is not None:
            return self._stale(
                base, pointer, "retracted_upstream", MARKER_RETRACTED,
                directive_id=directive.get("directive_id"))

        if pointer is None:
            return {**base, **_NO_MAPPING, "mapping": None, "node": None}

        # (2) the pointer itself carries the status
        state = str(pointer.get("status") or "active").lower()
        if state == "retracted":
            return self._stale(base, pointer, "mapping_retracted",
                               MARKER_RETRACTED)
        if state == "stale":
            return self._stale(base, pointer, "mapping_stale", MARKER_STALE)

        # (3) pointer completeness — a pointer with no referent is a poison case
        node_id = pointer.get("node_id")
        if not isinstance(node_id, str) or not node_id.strip():
            return self._stale(base, pointer, "mapping_pointer_incomplete",
                               MARKER_SOURCE_MISSING)

        # (4) the MemOS source must exist and be readable
        if not self.memos_db.is_file():
            return self._stale(base, pointer, "memos_store_unavailable",
                               MARKER_SOURCE_MISSING)
        try:
            conn = self._conn("memos")
            row = conn.execute(
                "SELECT id, content, summary, type, metadata, created_at, "
                "updated_at, valid_from, valid_to, expires_at, trust_score "
                "FROM nodes WHERE id = ?", (node_id,)).fetchone()
        except sqlite3.Error as exc:
            return self._stale(base, pointer,
                               f"memos_store_unreadable: {exc}",
                               MARKER_SOURCE_MISSING)
        if row is None:
            return self._stale(base, pointer, "memos_node_missing",
                               MARKER_SOURCE_MISSING)

        # (5) time validity (the consolidation engine may invalidate a node)
        node = dict(row)
        now = time.time()
        for field in ("valid_to", "expires_at"):
            value = node.get(field)
            if value in (None, "", 0):
                continue
            if _is_past(value, now):
                return self._stale(base, pointer,
                                   f"memos_node_{field}", MARKER_STALE)
        if str(node.get("type") or "").lower() == "retracted":
            return self._stale(base, pointer, "memos_node_retracted",
                               MARKER_RETRACTED)

        return {**base, "status": OK, "reason": "explicit_mapping",
                "marker": None, "mapping": pointer,
                "node": {"id": node["id"], "type": node.get("type"),
                         "summary": node.get("summary"),
                         "content": node.get("content"),
                         "created_at": node.get("created_at"),
                         "updated_at": node.get("updated_at"),
                         "trust_score": node.get("trust_score"),
                         "metadata": _jload(node.get("metadata"))}}

    def _stale(self, base: dict, pointer: dict, reason: str, marker: str,
               **extra) -> dict:
        return {**base, "status": STALE, "reason": reason, "marker": marker,
                "mapping": pointer, "node": None, **extra}

    # ── queries ──

    def by_form(self, form: str, *, full: bool = False) -> dict:
        spec = self.registry.get(form)  # unknown form -> loud RegistryError
        if spec.get("status") == RESERVED:
            return self._result(RESERVED_RESULT, {"form": form}, [],
                                reason=REVERIE_RESERVED_REASON, form=form)
        rows = self._iter_rows(form)
        hits = [self._hit(r, full=full, resolve=True) for r in rows]
        reason = None
        if not hits:
            failures = [f for f in self.source_failures if f["form"] == form]
            reason = ("source_unavailable" if failures
                      else "no_records_for_form")
        return self._result(OK if hits else NOT_FOUND, {"form": form}, hits,
                            reason=reason, form=form)

    def by_record_id(self, record_id: str, *, form: str | None = None) -> dict:
        forms = [form] if form else self.registry.form_names()
        found: list[dict] = []
        for name in forms:
            spec = self.registry.get(name)
            if spec.get("status") == RESERVED:
                continue
            for row in self._iter_rows(name):
                if row["record_id"] == record_id:
                    found.append(row)
        if not found:
            return self._result(NOT_FOUND, {"record_id": record_id, "form": form},
                                [], reason="no_record_with_id")
        hits = []
        for row in found:
            hit = self._hit(row, full=True, resolve=False)
            resolution = self.resolve(row)
            hit["memos"] = _summarise(resolution)
            hit["memos_resolution"] = resolution
            hits.append(hit)
        return self._result(OK, {"record_id": record_id, "form": form}, hits)

    def by_topic_hash(self, topic_hash: str) -> dict:
        hits: list[dict] = []
        for name in self.registry.form_names():
            spec = self.registry.get(name)
            if spec.get("status") == RESERVED or not spec.get("topic_hash"):
                continue
            for row in self._iter_rows(name):
                if row["topic_hash"] == topic_hash:
                    hit = self._hit(row, full=False, resolve=False)
                    hit["memos"] = _summarise(self.resolve(row))
                    hits.append(hit)
        return self._result(
            OK if hits else NOT_FOUND, {"topic_hash": topic_hash}, hits,
            reason=None if hits else "no_record_with_topic_hash")

    def stats(self) -> dict:
        out = {}
        for name in self.registry.form_names():
            spec = self.registry.get(name)
            if spec.get("status") == RESERVED:
                out[name] = {"count": 0, "status": RESERVED_RESULT}
                continue
            rows = self._iter_rows(name)
            out[name] = {"count": len(rows), "status": OK if rows else NOT_FOUND}
        out["registry"] = self._registry_meta()
        out["ro"] = self.readonly_report()
        return out

    # ── hints channel (sparks ONLY; never a ranked hit) ──

    def _spark_rows(self) -> list[dict]:
        workspace = REVERIE_STAGING_WORKSPACE
        try:
            conn = self._conn("engraphis")
            rows = conn.execute(
                "SELECT m.id AS store_id, m.content AS content, "
                "m.metadata AS metadata "
                "FROM memories m JOIN workspaces w ON w.id = m.workspace_id "
                "WHERE w.name = ?", (workspace,)).fetchall()
        except (sqlite3.Error, FileNotFoundError) as exc:
            self.source_failures.append(
                {"form": SPARK_FORM, "reason": "reverie_workspace_unavailable",
                 "path": str(self.engraphis_db),
                 "error": f"{type(exc).__name__}: {exc}"})
            return []
        out: list[dict] = []
        for row in rows:
            meta = _jload(row["metadata"])
            if meta.get("form") != SPARK_FORM:
                continue
            out.append({"store_id": row["store_id"],
                        "content": row["content"] or "", "metadata": meta})
        return out

    def hints(self, provocation=None, *, limit: int = 5) -> dict:
        """The SEPARATE hints channel: sparks only, capped, immune-filtered.

        Structural posture (§3.2/§5): this is a distinct return channel. The
        ranked `hits` of every query stay spark-free; callers that want nudges
        must read `hints`. A hint is never a memory and never an auto-write
        basis (``never_memory`` is carried on every entry).
        """
        query = {"form": SPARK_FORM, "provocation": provocation}
        # immune predicate (v1.2 referent): a form marked is_immune yields no
        # hints at all; a record-level is_immune flag is filtered per spark.
        try:
            form_is_immune = self.registry.get(SPARK_FORM).get("is_immune") is True
        except Exception:
            form_is_immune = False
        sparks = self._spark_rows()
        visible: list[dict] = []
        filtered = {"is_immune": 0, "decayed": 0, "malformed": 0}
        for spark in sparks:
            meta = spark["metadata"]
            if form_is_immune or meta.get("is_immune") is True:
                filtered["is_immune"] += 1
                continue
            if str(meta.get("status") or "").upper() == HINT_STATUS_DECAYED:
                filtered["decayed"] += 1
                continue
            refs = meta.get("sparks_from") or meta.get("derived_from") or []
            if not isinstance(refs, list) or len(refs) < 2:
                filtered["malformed"] += 1
                continue
            raw = meta.get("weight")
            try:
                raw_weight = float(raw) if raw is not None else 0.0
            except (TypeError, ValueError):
                raw_weight = 0.0
            weight = min(raw_weight, HINT_WEIGHT_CAP)
            visible.append({
                "spark_id": meta.get("spark_id") or meta.get("record_id"),
                "record_id": meta.get("record_id") or meta.get("spark_id"),
                "form": SPARK_FORM,
                "channel": "hints",
                "weight": weight,
                "raw_weight": raw_weight,
                "capped": raw_weight > weight,
                "hint_text": meta.get("synthesis"),
                "synthesis": meta.get("synthesis"),
                "sparks_from": refs,
                "provocation": meta.get("provocation"),
                "corroboration": meta.get("corroboration") or {},
                "marking": meta.get("marking") or "reverie:hint",
                "never_memory": True,
            })
        visible.sort(key=lambda h: (-h["weight"], str(h.get("spark_id"))))
        visible = visible[:max(0, int(limit))]
        status = OK if visible else NOT_FOUND
        reason = None if visible else (
            "no_visible_sparks" if sparks else "no_sparks_in_workspace")
        out = self._result(status, query, [], reason=reason, form=SPARK_FORM)
        out.update({
            "channel": "hints",
            "hints": visible,
            "count": len(visible),
            "weight_cap": HINT_WEIGHT_CAP,
            "is_immune_filtered": True,
            "form_is_immune": form_is_immune,
            "decayed_excluded": True,
            "filtered": filtered,
            "ranked_contains_sparks": False,
        })
        return out

    def query(self, form=None, *, provocation=None, full: bool = False,
              hint_limit: int = 5) -> dict:
        """The caller contract: ranked hits + a SEPARATE hints field.

        The ranked list can never contain a spark (``ranked_contains_sparks``
        is measured, not asserted by convention).
        """
        ranked = self.by_form(form or "capture", full=full)
        h = self.hints(provocation, limit=hint_limit)
        ranked["channel"] = "ranked"
        ranked["hints"] = h["hints"]
        ranked["hints_meta"] = {k: h[k] for k in (
            "status", "channel", "count", "weight_cap", "is_immune_filtered",
            "form_is_immune", "decayed_excluded", "filtered")}
        ranked["ranked_contains_sparks"] = any(
            hit.get("form") == SPARK_FORM for hit in ranked["hits"])
        return ranked

    # ── result plumbing ──

    def _registry_meta(self) -> dict:
        return {"path": str(self.registry.path),
                "forms": len(self.registry.forms()),
                "structural_hash": self.registry._snapshot()["structural_hash"],
                "provisioned_here": self.provisioned_registry}

    def _hit(self, row: dict, *, full: bool, resolve: bool) -> dict:
        out = {"form": row["form"], "record_id": row["record_id"],
               "topic_hash": row["topic_hash"], "source": row["source"],
               "memos": dict(_NO_MAPPING)}
        # the registry validates records the read path presents
        violations = self.registry.validate_record(
            row["form"], self._validation_body(row))
        out["schema_valid"] = not violations
        out["violations"] = violations[:5]
        if full:
            out["content"] = row["content"]
            out["payload"] = row["payload"]
        else:
            out["content_head"] = (row["content"] or "")[:160]
        if resolve:
            out["memos"] = _summarise(self.resolve(row))
        return out

    def _validation_body(self, row: dict) -> dict:
        """The record body a form's schema is written against."""
        if row["form"] == "staging":
            return {"metadata": row["payload"].get("metadata") or {},
                    "content": row["content"]}
        return row["payload"]

    def _result(self, status: str, query: dict, hits: list[dict], *,
                reason=None, form=None) -> dict:
        return {"status": status, "query": query,
                "form": form or query.get("form"),
                "count": len(hits), "hits": hits, "reason": reason,
                # the caller contract carries a SEPARATE hints channel; a
                # ranked result never holds a spark in `hits`
                "hints": [],
                "source_failures": list(self.source_failures),
                "registry": self._registry_meta(),
                "ro": self.readonly_report()}


def _summarise(resolution: dict) -> dict:
    return {"status": resolution["status"], "reason": resolution["reason"],
            "marker": resolution.get("marker")}


def _is_past(value, now: float) -> bool:
    """MemOS timestamps are epoch milliseconds; accept seconds too."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return False
    if v <= 0:
        return False
    if v > 1e11:  # milliseconds
        return v / 1000.0 <= now
    return v <= now


__all__ = [
    "SEAT_ID", "SEAT_STAGE", "DEFAULT_CAMPAIGN_ROOT", "DEFAULT_ENGRAPHIS_DB",
    "DEFAULT_MEMOS_DB", "STAGING_WORKSPACE",
    "REVERIE_STAGING_WORKSPACE", "SPARK_FORM", "HINT_WEIGHT_CAP",
    "HINT_STATUS_DECAYED",
    "OK", "NOT_FOUND", "RESERVED_RESULT", "UNMAPPED", "STALE",
    "MARKER_SOURCE_MISSING", "MARKER_STALE", "MARKER_RETRACTED",
    "open_readonly", "write_refused", "Retrieval",
]
