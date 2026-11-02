"""mev.eviction_hook — local instrumentation of the eviction hook (W2b / D2).

The eviction hook fires when the replay encounters a compaction boundary —
the moment at which context compaction would evict messages. Each fire event
records (boundary_id, ts) and must precede any measurement line in the run log.
A run without hook-fire evidence is not a result (charge).
"""
from __future__ import annotations
import time


class EvictionHook:
    """Call at each compaction boundary during replay. Logs hook-fire events
    and enforces that fire-log precedes any measurement line."""

    def __init__(self) -> None:
        self.fires: list[dict] = []
        self.measurement_logged = False

    def fire(self, boundary_id: int, ts: float | None = None) -> dict:
        if self.measurement_logged:
            raise RuntimeError(
                "hook-fire after measurement line — invalid order (charge: "
                "hook-fire must precede any measurement)"
            )
        ev = {
            "event": "hook-fire",
            "boundary_id": boundary_id,
            "ts": ts if ts is not None else time.time(),
        }
        self.fires.append(ev)
        return ev

    def log_measurement(self) -> None:
        """Called when the first measurement is recorded. After this, no more
        hook-fire events are allowed (they must all precede measurement)."""
        self.measurement_logged = True

    @property
    def fired(self) -> bool:
        return len(self.fires) > 0

    @property
    def fire_evidence(self) -> list[dict]:
        return list(self.fires)
