"""Public-release smoke: import plugin from the EXPORT copy, validate plugin.yaml
schema, verify off-switch constant. Works from a fresh checkout with no estate
paths. The full estate-harness integration suites remain sandbox-anchored; this
file is the repo-native CI gate.
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "hermes_plugin"))
sys.path.insert(0, str(ROOT))
os.environ.pop("MEV_VAULT_HOOK_OFF", None)
os.environ["MEV_CAMPAIGN_ROOT"] = str(Path.home() / ".mev-cadence-release-test")
os.environ["MEV_SEAT_STATE_ROOT"] = str(Path.home() / ".mev-cadence-release-test" / "seat-state")


def test_import_plugin_module_from_export():
    import mev_cadence as m
    assert Path(m.__file__).resolve().is_relative_to(ROOT.resolve())


def test_plugin_yaml_valid():
    import yaml
    p = ROOT / "hermes_plugin" / "mev_cadence" / "plugin.yaml"
    data = yaml.safe_load(p.read_text())
    assert data["name"] == "mev-cadence"
    assert "post_llm_call" in data["provides_hooks"]
    assert data["kind"] == "standalone"


def test_off_switch_constant_defined():
    import mev_cadence as m
    assert m.OFF_SWITCH_ENV == "MEV_VAULT_HOOK_OFF"


def test_no_roof_literals_in_export():
    import mev_cadence as m
    src = Path(m.__file__).read_text()
    assert "/home/greenknight" not in src
