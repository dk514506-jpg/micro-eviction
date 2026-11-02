"""mev.store_dispatch — additive store seam (CHARGE-CC-MEV-BRIDGE-V1 §3).

Loaders register for a db_path shape (suffix). source.load_turns and
partition.load_messages consult the registry BEFORE the sqlite path; when
nothing is registered for a suffix, behavior is byte-unchanged (Hermes
state.db). The Command Code bridge registers the .jsonl transcript loader
at worker startup; tests register it directly. Additive only — no existing
symbol changes when no loader is registered.
"""
from __future__ import annotations
from pathlib import Path

_LOADERS: dict[str, tuple] = {}  # suffix -> (load_turns_fn, load_messages_fn)


def register(suffix: str, *, load_turns, load_messages) -> None:
    """Register a store loader pair for a db_path suffix (e.g. '.jsonl')."""
    _LOADERS[str(suffix).lower()] = (load_turns, load_messages)


def loader_for(db_path) -> tuple | None:
    """Return the registered (load_turns, load_messages) pair, or None."""
    if db_path is None:
        return None
    return _LOADERS.get(Path(str(db_path)).suffix.lower())
