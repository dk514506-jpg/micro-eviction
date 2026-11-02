"""Enforce that tests exercise the EXPORT copy, never the estate roof copy."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
HERMES_PLUGIN = ROOT / "hermes_plugin"
MICRO_EVICTION = ROOT / "micro_eviction"

# Strip any estate-path entries from sys.path (roof campaign copies).
for entry in list(sys.path):
    if "roof/campaigns/bench/micro-eviction" in entry:
        sys.path.remove(entry)

for p in (str(HERMES_PLUGIN), str(MICRO_EVICTION), str(ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

# Neutral env in all cases.
os.environ.setdefault("MEV_CAMPAIGN_ROOT", str(Path.home() / ".mev-cadence-release-test"))
os.environ.setdefault("MEV_SEAT_STATE_ROOT", str(Path.home() / ".mev-cadence-release-test" / "seat-state"))
os.environ.pop("MEV_VAULT_HOOK_OFF", None)
