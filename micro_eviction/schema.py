"""mev.schema — record schema gate + provenance binding (Stage 0 mechanical gate)."""
from __future__ import annotations
import re

VALID_TYPES = {"lesson", "rule", "event", "artifact", "decision", "other"}
VALID_SCOPES = {"global", "task", "archive"}
REQUIRED_PROPOSAL_FIELDS = ("type", "content", "bounds", "scope", "confidence", "mutable")

# literal-binding regex (frozen): digit runs >=4 chars, or standalone 12-hex tokens
LITERAL_RE = re.compile(r"\b\d[\d,.]{3,}\b|\b[0-9a-f]{12}\b")
HEX12_RE = re.compile(r"^[0-9a-f]{12}$")

def validate_proposal(prop: dict, session_texts: dict[int, str]) -> list[str]:
    """Return a list of gate violations; empty == accepted."""
    errs: list[str] = []
    for f in REQUIRED_PROPOSAL_FIELDS:
        if f not in prop:
            label = "provenance source exchange bounds" if f == "bounds" else f
            errs.append(f"missing field: {f} ({label})")
    if errs:
        return errs
    t = prop["type"]
    if not isinstance(t, str) or t not in VALID_TYPES:
        errs.append(f"invalid type: {t!r}")
    if not isinstance(prop["content"], str) or not prop["content"].strip():
        errs.append("empty content")
    if not isinstance(prop["scope"], str) or prop["scope"] not in VALID_SCOPES:
        errs.append(f"invalid scope: {prop.get('scope')!r}")
    conf = prop["confidence"]
    if not isinstance(conf, (int, float)) or not (0.0 <= float(conf) <= 1.0):
        errs.append(f"invalid confidence: {conf!r}")
    if not isinstance(prop["mutable"], bool):
        errs.append("mutable must be bool")
    # provenance: bounds is a non-empty list of [start,end] message-id ranges
    bounds = prop["bounds"]
    if not isinstance(bounds, list) or not bounds:
        errs.append("missing provenance: bounds")
        return errs
    in_texts: list[str] = []
    for rng in bounds:
        if (not isinstance(rng, (list, tuple))) or len(rng) != 2:
            errs.append(f"malformed bound range: {rng!r}")
            continue
        a, b = rng
        if not (isinstance(a, int) and isinstance(b, int) and a <= b):
            errs.append(f"invalid bound range: {rng!r}")
            continue
        for mid in range(a, b + 1):
            if mid not in session_texts:
                errs.append(f"bound message {mid} not in session")
            else:
                in_texts.append(session_texts[mid])
    # artifact sha (optional): 12-hex and must appear in bounds text
    sha = prop.get("artifact_sha256_12")
    if sha is not None:
        if not isinstance(sha, str) or not HEX12_RE.match(sha):
            errs.append(f"invalid artifact_sha256_12: {sha!r}")
        elif not any(sha in t for t in in_texts):
            errs.append(f"artifact sha {sha} not present in bounds text")
    # literal binding: every digit-run/hex token in content must appear in bounds text
    if not errs:
        blob = "\n".join(in_texts)
        blob_l = blob.lower()
        for lit in LITERAL_RE.findall(prop["content"]):
            if lit.lower() not in blob_l:
                errs.append(f"literal {lit!r} in content not found in bounds text")
    return errs
