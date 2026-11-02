"""mev.reverie_logic — the SWAPPABLE firing lane for reverie (CHARGE-REVERIE-V1 §4).

§4 requires the firing logic to live in a swappable ``logic`` module, "never
baked into the record layer". This module is that lane: it turns a provocation
plus a candidate pool into zero or more spark *proposals*. It mints nothing,
writes nothing, and knows nothing about the sink — :mod:`micro_eviction.reverie`
owns the record layer and the only write path.

The default logic is deliberately deterministic and dependency-free (a build
default, behind the flag the spec asks for). It proposes a connection across
episodes drawn from DISTINCT sessions/contexts first — a reverie is the act of
connecting disparate episodes, so same-context material is the fallback, not
the preference.
"""
from __future__ import annotations

from typing import Any, Iterable

DEFAULT_MIN_EPISODES = 2
MAX_PROPOSED_EPISODES = 3


def _norm(ref: dict) -> dict:
    """Keep only what a citation needs (record_id + span/bounds)."""
    out = {"record_id": ref.get("record_id")}
    bounds = ref.get("bounds", ref.get("span"))
    if bounds is not None:
        out["bounds"] = bounds
    for key in ("session_id", "context"):
        if ref.get(key) is not None:
            out[key] = ref[key]
    return out


class DefaultSparkLogic:
    """Deterministic, swappable default: connect distinct-context episodes."""

    name = "default-distinct-context"

    def propose(self, provocation: Any, candidates: Iterable[dict], *,
                min_episodes: int = DEFAULT_MIN_EPISODES) -> list[dict]:
        refs = [r for r in (candidates or []) if isinstance(r, dict)
                and r.get("record_id")]
        if len(refs) < min_episodes:
            return []

        distinct: list[dict] = []
        seen: set = set()
        for ref in refs:
            key = ref.get("context") or ref.get("session_id") or ref["record_id"]
            if key in seen:
                continue
            seen.add(key)
            distinct.append(_norm(ref))

        chosen = distinct[:MAX_PROPOSED_EPISODES]
        if len(chosen) < min_episodes:
            # not enough distinct contexts: fall back to the pool in order
            chosen = [_norm(r) for r in refs[:min_episodes]]
        if len(chosen) < min_episodes:
            return []

        return [{
            "sparks_from": chosen,
            "synthesis": None,
            "session_id": chosen[0].get("session_id"),
            "context": chosen[0].get("context"),
            "logic": self.name,
            "provocation_kind": (provocation or {}).get("kind")
            if isinstance(provocation, dict) else None,
        }]


__all__ = ["DEFAULT_MIN_EPISODES", "MAX_PROPOSED_EPISODES",
           "DefaultSparkLogic"]
