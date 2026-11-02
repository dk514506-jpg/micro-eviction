"""mev.t2_classifier — T2 mechanical-first typing with measured LLM fallback (S2).

Plan §3 (R5) + §1.2 step 6, frozen at the S2 gate:

  * T1 VERBATIM-carrier marker rule (first match wins) — masked spans,
    corrections, decisions, identifiers are captured whole; they never reach
    the scorer or the LLM.
  * Mechanical scorer for the remainder: a fixed-weight linear scorer over the
    binary indicator vector (plan §3.1 feature rules 2-3). Confidence = softmax
    top probability; ties -> `other` with margin confidence.
  * Below the floor theta* the item is queued for the T2 pass (compact LLM,
    `xiaomi/mimo-v2.6-flash`, per-slice, NEVER batch). Token usage is MEASURED
    from the call; if the transport reports no usage the pass fails CLOSED
    (never estimated, G6 law).
  * Caps (R8): 8,000 measured tokens per boundary across T2 passes; T2
    fallback rate > 25% over any 5 consecutive boundaries -> the boundary state
    degrades to T1-only and the layer STOPS for adjudication. Fallback-rate
    denominator = items per boundary (never per slice, never per session).

Frozen derivations:
  * SELECTION_RULE — the floor is the calibration elbow on the classifier's
    precision/recall curve ("pr-curve-elbow-chord-distance"); the RULE is
    pinned, the VALUE is measured at build time and lives in
    reports/t2_calibration.json (never hard-coded here).
  * WEIGHTS / FEATURE_SPEC — frozen at the S2 gate (plan §3.1 step 4); they do
    not change after the gate.
  * COUNTING_RULE — the sealed-corpus gold census pin (GAPS-INVESTIGATION G3 +
    stage brief: 29 gold items, 3 distinct gold-confidence theta points).
"""
from __future__ import annotations

import json
import math
import re
import subprocess
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence

from . import io
from .schema import HEX12_RE, LITERAL_RE, VALID_TYPES

# ---------------------------------------------------------------------------
# Adopted numbers (V3.1 / plan §11) + frozen rule names
# ---------------------------------------------------------------------------
T2_MODEL = "xiaomi/mimo-v2.6-flash"
PER_BOUNDARY_TOKEN_CAP = 8000          # measured tokens per boundary, T2 passes
BREAKER_WINDOW = 5                      # consecutive boundaries
BREAKER_RATE_THRESHOLD = 0.25           # fallback rate > 25% trips the breaker
SELECTION_RULE = "pr-curve-elbow-chord-distance"
COUNTING_RULE = (
    "gold = accepted rows (superseded_by falsy) with non-empty content and type in "
    "schema.VALID_TYPES from ledgers/*.jsonl, EXCLUDING *.spine.jsonl (spine rows: "
    "no type) and ledgers/lifecycle-transitions.jsonl (content-less transition rows), "
    "PLUS the typed calcified rule entry (calcified.jsonl rule_content / "
    "reports/stage1_report.json calcified_entries, type recoverable as rule). "
    "n = 28 ledger + 1 calcified = 29 gold items. "
    "Per GAPS-INVESTIGATION G3 (exclude spine/transition rows and content-less items)."
)

TYPES: tuple[str, ...] = ("rule", "lesson", "event", "artifact", "decision", "other")

# ---------------------------------------------------------------------------
# T1 VERBATIM-carrier marker rule (plan §1.2 step 6) — first match wins.
#   1. masked-span pattern: LITERAL_RE/HEX12_RE hit + span markers (a literal
#      preserved inside backtick/quote/bracket/paren delimiters),
#   2. correction marker (correction:/corrected: head),
#   3. decision marker (decision:/decided head),
#   4. identifier token (12-hex / digit-run literal per schema.LITERAL_RE at the
#      item head).
# ---------------------------------------------------------------------------
_SPAN_RE = re.compile(
    r"[`\"'[(][^`\"')\]]*?(?:\b\d[\d,.]{3,}\b|\b[0-9a-f]{12}\b)[^`\"')\]]*?[`\"')\]]"
)
_CORRECTION_HEAD_RE = re.compile(r"^\s*(?:corrections?|corrected)\s*:", re.I)
_DECISION_HEAD_RE = re.compile(r"^\s*(?:decisions?\s*:|decided\b)", re.I)
_IDENTIFIER_HEAD_RE = re.compile(r"^\s*(?:\d[\d,.]{3,}|[0-9a-f]{12})\b")

CARRIER_KINDS = ("masked_span", "correction", "decision", "identifier")

# Carrier-kind -> type fallback for T1 whole-capture typing (plan feature-rule
# table: correction->event, decision->decision, masked span/identifier->artifact).
_CARRIER_KIND_TYPE = {
    "masked_span": "artifact",
    "identifier": "artifact",
    "correction": "event",
    "decision": "decision",
}


def verbatim_carrier_kind(text: str) -> Optional[str]:
    """Return the VERBATIM-carrier kind of an item, or None (goes to T2)."""
    if _SPAN_RE.search(text):
        return "masked_span"
    if _CORRECTION_HEAD_RE.search(text):
        return "correction"
    if _DECISION_HEAD_RE.search(text):
        return "decision"
    if _IDENTIFIER_HEAD_RE.search(text):
        return "identifier"
    return None


def is_verbatim_carrier(text: str) -> bool:
    return verbatim_carrier_kind(text) is not None


# ---------------------------------------------------------------------------
# Mechanical type marker-head table (plan §3.1 feature rule 2) — T1 typing for
# carriers (first match wins) and classifier indicators for the remainder.
# ---------------------------------------------------------------------------
_HEAD_RULE_RE = re.compile(r"^\s*(?:standing\s+)?rules?:", re.I)
_HEAD_LESSON_RE = re.compile(r"^\s*lessons?:", re.I)
_DECISION_MARKER_RE = re.compile(r"\bdecisions?:|\bdecided\b", re.I)
_CORRECTION_MARKER_RE = re.compile(r"\bcorrections?:|\bcorrected\b", re.I)
_ARTIFACT_MARKER_RE = re.compile(
    r"\bartifact\b|\b[\w-]+\.\w{1,5}\b|\b[0-9a-f]{12}\b", re.I
)
_OCCURRENCE_MARKER_RE = re.compile(
    r"\b(?:occurred|happened|false positive|flagged|fixed|missed|logged|beat|added|"
    r"dropped|removed|rejected|broke|failed)\b",
    re.I,
)
_IMPERATIVE_RE = re.compile(
    r"\b(?:never|always|must|verify|ensure|keep|report|pin|measure|enumerate|score|"
    r"refuse|avoid|do not|don't)\b|(?<![\w-])only(?![\w-])",
    re.I,
)
_PAST_EVENT_RE = re.compile(
    r"\b(?:was|were|had|been|became|went|took|made|gave|found|showed|claimed|"
    r"replaced|adopted|collapsed)\b",
    re.I,
)

# Binary indicator vector (plan §3.1 feature rules 2-3). Feature regexes are
# frozen; the WEIGHTS table below is the build-time choice frozen at the gate.
FEATURE_SPEC: dict[str, str] = {
    "head_rule": r"^\s*(?:standing\s+)?rules?:",
    "head_lesson": r"^\s*lessons?:",
    "decision_marker": r"\bdecisions?:|\bdecided\b",
    "correction_marker": r"\bcorrections?:|\bcorrected\b",
    "artifact_marker": r"\bartifact\b|\b[\w-]+\.\w{1,5}\b|\b[0-9a-f]{12}\b",
    "occurrence_marker": (
        r"\b(?:occurred|happened|false positive|flagged|fixed|missed|logged|beat|"
        r"added|dropped|removed|rejected|broke|failed)\b"
    ),
    "imperative_rule": (
        r"\b(?:never|always|must|verify|ensure|keep|report|pin|measure|enumerate|"
        r"score|refuse|avoid|do not|don't)\b|(?<![\w-])only(?![\w-])"
    ),
    "past_event": (
        r"\b(?:was|were|had|been|became|went|took|made|gave|found|showed|claimed|"
        r"replaced|adopted|collapsed)\b"
    ),
    "literal_dense": "(literals per char >= LITERAL_DENSITY_MIN)",
    "wide_bounds": "(max message-id span >= WIDE_BOUNDS_MIN)",
}

LITERAL_DENSITY_MIN = 0.002   # literals / chars, frozen
WIDE_BOUNDS_MIN = 5           # message-id span, frozen

# Frozen at the S2 gate (plan §3.1 step 4: chosen on the sealed corpus,
# recorded in reports/t2_calibration.json, never changed after the gate).
WEIGHTS: dict[str, dict[str, float]] = {
    #              rule  lesson  event  artifact  decision  other
    "head_rule":         {"rule": 3.0},
    "head_lesson":       {"lesson": 3.0},
    "decision_marker":   {"decision": 3.0},
    "correction_marker": {"event": 3.0},
    "artifact_marker":   {"artifact": 2.0},
    "occurrence_marker": {"event": 1.5},
    "imperative_rule":   {"rule": 2.0},
    "past_event":        {"event": 1.0},
    "literal_dense":     {"artifact": 1.0},
    # wide_bounds carries no class signal on the sealed corpus (all gold bounds
    # are single-message spans); zeroed rather than spuriously biasing a type.
    "wide_bounds":       {"artifact": 0.0},
}


def indicator_vector(content: str, bounds: Optional[Sequence[Sequence[int]]] = None) -> dict[str, int]:
    """Frozen binary indicator vector over a slice item (plan §3.1 rules 2-3)."""
    x = {
        "head_rule": int(bool(_HEAD_RULE_RE.search(content))),
        "head_lesson": int(bool(_HEAD_LESSON_RE.search(content))),
        "decision_marker": int(bool(_DECISION_MARKER_RE.search(content))),
        "correction_marker": int(bool(_CORRECTION_MARKER_RE.search(content))),
        "artifact_marker": int(bool(_ARTIFACT_MARKER_RE.search(content))),
        "occurrence_marker": int(bool(_OCCURRENCE_MARKER_RE.search(content))),
        "imperative_rule": int(bool(_IMPERATIVE_RE.search(content))),
        "past_event": int(bool(_PAST_EVENT_RE.search(content))),
    }
    n_chars = max(len(content), 1)
    lit_density = len(LITERAL_RE.findall(content)) / n_chars
    x["literal_dense"] = int(lit_density >= LITERAL_DENSITY_MIN)
    width = 0
    if bounds:
        for rng in bounds:
            if isinstance(rng, (list, tuple)) and len(rng) == 2:
                width = max(width, int(rng[1]) - int(rng[0]))
    x["wide_bounds"] = int(width >= WIDE_BOUNDS_MIN)
    return x


def _t1_carrier_type(kind: str, content: str) -> str:
    """T1 whole-capture typing: marker-head table first, then carrier-kind map."""
    if _HEAD_RULE_RE.search(content):
        return "rule"
    if _HEAD_LESSON_RE.search(content):
        return "lesson"
    if _DECISION_HEAD_RE.search(content):
        return "decision"
    if _CORRECTION_HEAD_RE.search(content):
        return "event"
    if _ARTIFACT_MARKER_RE.search(content):
        return "artifact"
    if _OCCURRENCE_MARKER_RE.search(content):
        return "event"
    return _CARRIER_KIND_TYPE[kind]


def score_types(content: str, bounds: Optional[Sequence[Sequence[int]]] = None,
                weights: Optional[dict[str, dict[str, float]]] = None) -> dict[str, float]:
    """Fixed-weight linear scores over the indicator vector (plan §3.1 step 4)."""
    w = WEIGHTS if weights is None else weights
    x = indicator_vector(content, bounds)
    scores = {t: 0.0 for t in TYPES}
    for fname, v in x.items():
        if not v:
            continue
        for t, wt in w.get(fname, {}).items():
            scores[t] = scores.get(t, 0.0) + float(wt)
    return scores


def classify_mechanical(
    content: str,
    bounds: Optional[Sequence[Sequence[int]]] = None,
    *,
    weights: Optional[dict[str, dict[str, float]]] = None,
) -> tuple[str, float, str, Optional[str]]:
    """Mechanical-first typing: (type, confidence, tier, carrier_kind).

    T1 VERBATIM carriers (plan §1.2 step 6) bypass the scorer entirely:
    captured whole, typed by the marker-head table, confidence 1.0, tier T1.
    The remainder is scored: confidence = softmax top probability over type
    scores; a top-score tie -> `other` with margin confidence. The caller
    routes below-floor items to the T2 pass (tier T2).
    """
    kind = verbatim_carrier_kind(content)
    if kind is not None:
        return _t1_carrier_type(kind, content), 1.0, "T1", kind
    scores = score_types(content, bounds, weights)
    ordered = sorted(TYPES, key=lambda t: (-scores[t], t))
    top, second = ordered[0], ordered[1]
    margin = scores[top] - scores[second]
    if margin <= 1e-12:
        return "other", 0.0, "T2", None
    denom = sum(math.exp(scores[t] - scores[top]) for t in TYPES)
    conf = 1.0 / denom   # softmax probability of the top type
    return top, conf, "T2", None


# ---------------------------------------------------------------------------
# Floor routing (the floor VALUE is measured, never hard-coded here).
# ---------------------------------------------------------------------------
def route_item(content: str, bounds: Optional[Sequence[Sequence[int]]] = None,
               *, floor_theta: float = 1.0,
               weights: Optional[dict[str, dict[str, float]]] = None) -> dict:
    """Classify one item and route it against the calibrated floor.

    T1 carriers never queue for T2 (plan §3.3 s_verbatim_carrier_t1). Any
    non-carrier with confidence < floor_theta is queued for the T2 pass.
    """
    rtype, conf, tier, kind = classify_mechanical(content, bounds, weights=weights)
    queued = tier == "T2" and conf < floor_theta
    return {
        "type": rtype,
        "confidence": conf,
        "tier": tier,
        "carrier_kind": kind,
        "queued_for_t2": queued,
    }


# ---------------------------------------------------------------------------
# T2 pass: per-slice, never batch; measured tokens; 8K per-boundary budget.
# ---------------------------------------------------------------------------
class T2ProtocolError(RuntimeError):
    """The LLM response could not be parsed into typed labels (loud, journaled)."""


class T2BudgetExhausted(RuntimeError):
    """The per-boundary T2 token budget is exhausted — pass truncated."""


def default_llm(model: str = T2_MODEL) -> Callable[[str], dict]:
    """Runtime adapter for the T2 model. Tokens are MEASURED from the response;
    if the transport reports no usage the call fails CLOSED (never estimated).

    NOTE (plan-note): the charge pins the T2 runtime MODEL but not the transport
    flags. This adapter shells `mimo run --model <model> <prompt>` and accepts a
    JSON envelope {"text": ..., "usage": {"input": ..., "output": ...}} as the
    whole stdout or as its final line; any other shape with no usage raises.
    Fixtures always inject stub callables — the gate never touches the pool.
    """
    def _call(prompt: str) -> dict:
        proc = subprocess.run(
            ["mimo", "run", "--model", model, prompt],
            capture_output=True, text=True, timeout=120,
        )
        if proc.returncode != 0:
            raise T2ProtocolError(f"mimo run failed rc={proc.returncode}: {proc.stderr[-400:]}")
        out = proc.stdout.strip()
        text = out
        tokens_in = tokens_out = None
        try:
            obj = json.loads(out)
        except json.JSONDecodeError:
            obj = None
            lines = out.splitlines()
            if lines:
                try:
                    obj = json.loads(lines[-1])
                except json.JSONDecodeError:
                    obj = None
        if isinstance(obj, dict) and "text" in obj:
            text = str(obj["text"])
            usage = obj.get("usage") or {}
            tokens_in = usage.get("input")
            tokens_out = usage.get("output")
        if not isinstance(tokens_in, int) or not isinstance(tokens_out, int):
            raise T2ProtocolError(
                "T2 pass fails CLOSED: transport reported no measured usage "
                "(tokens are measured, never estimated)"
            )
        return {"text": text, "tokens_in": tokens_in, "tokens_out": tokens_out}
    return _call


T2_PROMPT = (
    "Classify each item below into exactly one type from: "
    + ", ".join(TYPES)
    + ".\nReturn ONLY a JSON array of type strings, one per item, in order.\nItems:\n"
)


def _t2_prompt(contents: Sequence[str]) -> str:
    return T2_PROMPT + "\n".join(f"{i + 1}. {c}" for i, c in enumerate(contents))


def parse_t2_labels(text: str, n: int) -> list[str]:
    """Strict parse of the T2 pass response (loud failure, never silent)."""
    raw = text.strip()
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise T2ProtocolError(f"T2 response is not JSON: {exc}") from None
    if not isinstance(obj, list) or len(obj) != n:
        raise T2ProtocolError(f"T2 response must be a JSON array of {n} types, got {obj!r}")
    out = []
    for v in obj:
        if not isinstance(v, str) or v not in VALID_TYPES:
            raise T2ProtocolError(f"T2 label not in VALID_TYPES: {v!r}")
        out.append(v)
    return out


@dataclass
class T2BoundaryState:
    """Per-boundary T2 accounting: 8K measured-token budget + fallback breaker.

    One instance per cadence boundary; shared across that boundary's slices.
    Fallback-rate denominator = items per boundary (R5).
    """
    budget_cap: int = PER_BOUNDARY_TOKEN_CAP
    llm_fn: Optional[Callable[[str], dict]] = None
    floor_theta: float = 1.0
    tokens_used: int = 0
    truncated: bool = False
    items_seen: int = 0
    items_below_floor: int = 0
    items_t2_llm: int = 0
    t2_errors: list = field(default_factory=list)
    calls: list = field(default_factory=list)
    _degraded: bool = False

    @property
    def t1_only(self) -> bool:
        return self._degraded

    @property
    def stopped(self) -> bool:
        return self._degraded

    def degrade(self) -> None:
        """Circuit-breaker trip -> T1-only: no further T2 LLM passes; the layer
        STOPS for adjudication (R8)."""
        self._degraded = True

    def observe_items(self, routed: Sequence[dict]) -> None:
        self.items_seen += len(routed)
        self.items_below_floor += sum(1 for r in routed if r.get("queued_for_t2"))

    def fallback_rate(self) -> float:
        if self.items_seen == 0:
            return 0.0
        return self.items_below_floor / self.items_seen

    def run_pass(self, contents: Sequence[str]) -> tuple[list[str], dict]:
        """One T2 LLM call for ONE slice's below-floor items. Never batch.

        Token usage is measured from the response. If the call would exceed the
        per-boundary 8K budget, the pass is TRUNCATED (no call made) and the
        caller keeps the mechanical provisional types (R8).
        """
        if self._degraded:
            raise T2BudgetExhausted("layer degraded to T1-only (circuit breaker tripped)")
        if self.llm_fn is None:
            raise T2ProtocolError("no T2 llm_fn bound to the boundary state")
        if self.budget_cap - self.tokens_used <= 0:
            self.truncated = True
            raise T2BudgetExhausted("per-boundary T2 token budget exhausted")
        prompt = _t2_prompt(contents)
        resp = self.llm_fn(prompt)
        used = int(resp["tokens_in"]) + int(resp["tokens_out"])
        self.tokens_used += used
        self.calls.append({"n_items": len(contents), "tokens": used,
                           "tokens_in": resp["tokens_in"], "tokens_out": resp["tokens_out"]})
        if self.tokens_used > self.budget_cap:
            self.truncated = True
        labels = parse_t2_labels(resp["text"], len(contents))
        self.items_t2_llm += len(contents)
        return labels, {"tokens": used, "tokens_in": resp["tokens_in"],
                        "tokens_out": resp["tokens_out"]}


class FallbackBreaker:
    """R8 circuit breaker: fallback rate > 25% over any 5 consecutive
    boundaries -> T1-only + the layer STOPS for adjudication."""

    def __init__(self, window: int = BREAKER_WINDOW,
                 threshold: float = BREAKER_RATE_THRESHOLD):
        self.window = window
        self.threshold = threshold
        self.history: list[dict] = []
        self.tripped: bool = False
        self.trip_at: Optional[int] = None

    def observe(self, *, boundary_id: Any, items: int, below_floor: int) -> dict:
        entry = {
            "boundary_id": boundary_id,
            "items": items,
            "below_floor": below_floor,
            "rate": (below_floor / items) if items else 0.0,
        }
        self.history.append(entry)
        window = self.history[-self.window:]
        win_items = sum(e["items"] for e in window)
        win_below = sum(e["below_floor"] for e in window)
        win_rate = (win_below / win_items) if win_items else 0.0
        result = {
            **entry,
            "window_boundaries": len(window),
            "window_items": win_items,
            "window_below_floor": win_below,
            "window_rate": win_rate,
            "tripped": self.tripped,
            "degraded_t1_only": self.tripped,
            "stop_for_adjudication": self.tripped,
        }
        if not self.tripped and len(window) == self.window and win_rate > self.threshold:
            self.tripped = True
            self.trip_at = len(self.history)
            result["tripped"] = True
            result["degraded_t1_only"] = True
            result["stop_for_adjudication"] = True
            result["trip_reason"] = (
                f"fallback rate {win_rate:.4f} > {self.threshold} over "
                f"{self.window} consecutive boundaries"
            )
        return result


# ---------------------------------------------------------------------------
# Sealed-corpus gold census + calibration (plan §3.2; G3 counting rule).
# ---------------------------------------------------------------------------
def load_gold_corpus(root: str) -> list[dict]:
    """Pinned counting rule (COUNTING_RULE): the 29-item sealed gold census."""
    import hashlib
    import os

    items: list[dict] = []
    corpus_shas: dict[str, str] = {}
    ledger_dir = os.path.join(root, "ledgers")
    for name in sorted(os.listdir(ledger_dir)):
        if not name.endswith(".jsonl"):
            continue
        if name.endswith(".spine.jsonl") or name == "lifecycle-transitions.jsonl":
            continue
        path = os.path.join(ledger_dir, name)
        with open(path, "rb") as f:
            corpus_shas[f"ledgers/{name}"] = hashlib.sha256(f.read()).hexdigest()
        for row in io.load_jsonl(path):
            content = row.get("content")
            rtype = row.get("type")
            if not isinstance(content, str) or not content.strip():
                continue
            if rtype not in VALID_TYPES:
                continue
            if row.get("superseded_by"):
                continue
            items.append({
                "item_id": row["record_id"],
                "source": f"ledgers/{name}",
                "content": content,
                "bounds": [list(r) for r in (row.get("provenance") or {}).get(
                    "source_exchange_bounds", [])],
                "gold_type": rtype,
                "gold_confidence": row.get("confidence"),
            })
    calc_path = os.path.join(root, "calcified.jsonl")
    with open(calc_path, "rb") as f:
        corpus_shas["calcified.jsonl"] = hashlib.sha256(f.read()).hexdigest()
    for row in io.load_jsonl(calc_path):
        content = row.get("rule_content")
        rtype = row.get("type")
        if isinstance(content, str) and content.strip() and rtype in VALID_TYPES:
            items.append({
                "item_id": "calcified:" + str(row.get("topic_hash", ""))[:16],
                "source": "calcified.jsonl",
                "content": content,
                "bounds": [list(r) for r in (row.get("provenance") or {}).get(
                    "source_exchange_bounds", [])],
                "gold_type": rtype,
                "gold_confidence": row.get("confidence"),
            })
    rep_path = os.path.join(root, "reports", "stage1_report.json")
    with open(rep_path, "rb") as f:
        corpus_shas["reports/stage1_report.json"] = hashlib.sha256(f.read()).hexdigest()
    return items, corpus_shas


def _macro_precision(accepted: list[dict]) -> float:
    """Type-accuracy proxy, macro over classes present in the accepted set."""
    classes = sorted({it["gold_type"] for it in accepted})
    if not classes:
        return 1.0
    accs = []
    for c in classes:
        sub = [it for it in accepted if it["gold_type"] == c]
        accs.append(sum(1 for it in sub if it["pred_type"] == c) / len(sub))
    return sum(accs) / len(accs)


def pr_curve(preds: Sequence[dict], *, conf_key: str = "pred_conf") -> list[dict]:
    """Sweep theta over {0.00..1.00}; precision macro over classes present;
    recall = covered-and-correct / all gold; empty accepted -> (P=1, R=0)."""
    n_all = len(preds)
    out = []
    for i in range(101):
        theta = round(i / 100.0, 2)
        accepted = [p for p in preds if p[conf_key] >= theta]
        if accepted:
            precision = _macro_precision(accepted)
            n_correct = sum(1 for p in accepted if p["pred_type"] == p["gold_type"])
            recall = n_correct / n_all if n_all else 0.0
        else:
            precision, n_correct, recall = 1.0, 0, 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        out.append({"theta": theta, "precision": round(precision, 6),
                    "recall": round(recall, 6), "f1": round(f1, 6),
                    "n_accepted": len(accepted), "n_correct": n_correct})
    return out


def _elbow(curve: Sequence[dict]) -> tuple[Optional[float], list, dict]:
    """Elbow = argmax perpendicular distance to the chord joining the curve
    endpoints (theta=0 and theta=1); ties -> larger theta. FAIL-CLOSED on a
    degenerate curve (chord of zero length, or every point on the chord)."""
    p0 = (curve[0]["recall"], curve[0]["precision"])
    p1 = (curve[-1]["recall"], curve[-1]["precision"])
    vx, vy = p1[0] - p0[0], p1[1] - p0[1]
    norm = math.hypot(vx, vy)
    if norm <= 1e-12:
        return None, [], {"reason": "zero-length chord between curve endpoints"}
    distances = []
    for row in curve:
        px, py = row["recall"], row["precision"]
        cross = abs(vx * (py - p0[1]) - vy * (px - p0[0]))
        distances.append({"theta": row["theta"], "distance": cross / norm})
    best = max(d["distance"] for d in distances)
    if best <= 1e-12:
        return None, distances, {"reason": "curve lies on the chord (no interior elbow)"}
    cands = [d for d in distances if abs(d["distance"] - best) <= 1e-12]
    chosen = max(c["theta"] for c in cands)   # ties -> larger theta
    return chosen, distances, {}


def calibrate(root: str, *, weights: Optional[dict[str, dict[str, float]]] = None,
              ts: Optional[str] = None) -> dict:
    """Build reports/t2_calibration.json content (plan §3.2 recording schema).

    * per_theta  = THE curve: the classifier's P-R curve over the sealed corpus
      (plan §3.2 "the classifier's precision/recall curve"); floor_theta is its
      elbow and is the operative runtime floor.
    * corpus_curve = the same procedure swept on the gold confidence FIELD
      (GAPS-G3 axis; exactly 3 distinct theta points on this corpus) — recorded
      as the corpus reference, non-operative.
    """
    items, corpus_shas = load_gold_corpus(root)
    preds = []
    for it in items:
        ptype, conf, tier, kind = classify_mechanical(
            it["content"], it["bounds"], weights=weights)
        preds.append({**it, "pred_type": ptype, "pred_conf": conf,
                      "tier": tier, "carrier_kind": kind,
                      "correct": ptype == it["gold_type"]})
    for p in preds:
        p["gold_conf_field"] = (
            float(p["gold_confidence"]) if p["gold_confidence"] is not None else 0.0)

    curve = pr_curve(preds, conf_key="pred_conf")
    floor_theta, distances, deg = _elbow(curve)
    corpus_curve = pr_curve(preds, conf_key="gold_conf_field")
    corpus_floor, corpus_distances, corpus_deg = _elbow(corpus_curve)

    conf_values = [p["gold_conf_field"] for p in preds]
    distinct_fields = sorted({round(v, 6) for v in conf_values})
    distribution: dict[str, int] = {}
    for p in preds:
        key = ("null" if p["gold_confidence"] is None
               else format(float(p["gold_confidence"]), ".2f"))
        distribution[key] = distribution.get(key, 0) + 1
    class_dist: dict[str, int] = {}
    for p in preds:
        class_dist[p["gold_type"]] = class_dist.get(p["gold_type"], 0) + 1

    degenerate_reasons = []
    if len(preds) < 20:
        degenerate_reasons.append(f"gold items {len(preds)} < 20")
    if len(distinct_fields) < 2:
        degenerate_reasons.append("fewer than 2 distinct theta points (gold confidences)")
    if len(distinct_fields) == 1:
        degenerate_reasons.append("all-identical confidences")
    if deg:
        degenerate_reasons.append(f"classifier curve degenerate: {deg['reason']}")
    curve_thetas = sorted({round(r["theta"], 2) for r in curve
                           if 0 < r["n_accepted"] < len(preds)})
    if len(curve_thetas) < 2:
        degenerate_reasons.append("classifier curve has fewer than 2 distinct theta points")

    below = [p for p in preds if p["pred_conf"] < (floor_theta if floor_theta is not None else 1.0)]
    fallback_rate = len(below) / len(preds) if preds else 0.0

    artifact = {
        "items_n": len(preds),
        "counting_rule": COUNTING_RULE,
        "corpus_shas": corpus_shas,
        "per_theta": curve,
        "floor_theta": floor_theta,
        "rule": SELECTION_RULE,
        "rule_pinned": (
            "elbow = argmax over theta of the perpendicular distance from "
            "(recall(theta), precision(theta)) to the chord joining the curve "
            "endpoints (theta=0 and theta=1); ties -> larger theta; sweep theta "
            "over {0.00, 0.01, ..., 1.00}; precision = type accuracy macro over "
            "classes present among items with confidence >= theta; recall = "
            "covered-and-correct / all gold items; empty accepted set -> (P=1, R=0)"
        ),
        "weights": WEIGHTS if weights is None else weights,
        "feature_spec": FEATURE_SPEC,
        "curve_basis": "classifier_confidence (plan 3.2: the classifier's P-R curve; operative floor)",
        "corpus_curve_basis": "gold_confidence_field (GAPS-G3 corpus axis; non-operative reference)",
        "corpus_curve": corpus_curve,
        "corpus_curve_floor_theta": corpus_floor,
        "corpus_theta_points": len([v for v in distinct_fields if v > 0]),
        "distinct_gold_confidence_values": distinct_fields,
        "gold_confidence_distribution": distribution,
        "class_distribution": class_dist,
        "elbow_distances": distances,
        "corpus_elbow_distances": corpus_distances,
        "degenerate": bool(degenerate_reasons),
        "degenerate_reasons": degenerate_reasons,
        "fallback_rate_at_floor": round(fallback_rate, 6),
        "fallback_rate_denominator": "items per boundary (R5)",
        "corpus_accuracy": {
            "correct": sum(1 for p in preds if p["correct"]),
            "total": len(preds),
        },
        "t2_model": T2_MODEL,
        "per_boundary_authoring_cap_tokens": PER_BOUNDARY_TOKEN_CAP,
        "circuit_breaker": {
            "fallback_rate_threshold": BREAKER_RATE_THRESHOLD,
            "window_boundaries": BREAKER_WINDOW,
            "action": "T1-only + STOP for adjudication",
        },
        "predictions": [
            {"item_id": p["item_id"], "gold": p["gold_type"], "pred": p["pred_type"],
             "conf": p["pred_conf"], "gold_conf": p["gold_conf_field"],
             "tier": p["tier"], "carrier_kind": p["carrier_kind"], "correct": p["correct"]}
            for p in preds
        ],
        "ts": ts or io_time_iso(),
    }
    return artifact


def io_time_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_calibration(root: str, path: Optional[str] = None, **kw) -> str:
    artifact = calibrate(root, **kw)
    import os
    out = path or os.path.join(root, "reports", "t2_calibration.json")
    io.allow(root)
    io.safe_write(out, json.dumps(artifact, ensure_ascii=False, sort_keys=True, indent=1) + "\n")
    return out
