# mev-cadence — Hermes Agent plugin

Micro-eviction session memory for [Hermes Agent](https://hermes-agent.nousresearch.com):
a `post_llm_call` observer that, from turn ordinal 6 onward, slices the delta
since the last processed boundary (plus compaction-exit slices), runs it through
a mechanical micro-eviction pipeline, and — when staging is configured — stores
the evicted records in an [Engraphis](https://engraphis.ai) workspace. It also
mirrors 75k-token seat-state checkpoints as one dated summary entry per
checkpoint into a knowledge-vault `log.md` (append-only, idempotent per
seat-label+NN).

Built as an open-source, standalone Hermes plugin. Neutral by default; the
Engraphis/75k mirror integrations are optional and opt-in.

## Features

- Every-turn cadence from turn 6 (turns 1–5 are no-ops).
- Delta slicing since the last processed boundary, including compaction-exit slices.
- Slices recorded durably as session ledgers + slice cursors before any staging.
- Optional Engraphis staging (`mev-staging` workspace) via the sink path.
- Optional 75k seat-state mirror: `log.md` summary entries + seat-state notes
  (`log/seat-state/<label>/` with `INDEX.md`).
- Off-switch: `MEV_VAULT_HOOK_OFF=1` disables all firing (fail-closed, never
  raises into the host turn pipeline).
- Two-sided test suite (31+ regressions, canonical-sync coverage).

## Requirements

- Hermes Agent >= 0.21.5 (plugin hook: `post_llm_call`).
- Python >= 3.9.
- Optional: Engraphis store (default path `~/.engraphis/memory.db`) for staging;
  if absent, staging is skipped and the plugin stays capture-only.

## Install

Copy `hermes_plugin/mev_cadence/` into your Hermes plugins directory:

```bash
mkdir -p ~/.hermes/plugins/
cp -r hermes_plugin/mev_cadence ~/.hermes/plugins/
```

Then enable it in `~/.hermes/config.yaml` under `plugins`:

```yaml
plugins:
  entries:
    mev-cadence:
      settings:
        # all settings optional — defaults below
        seat_label: desktop-main
        desktop_seat_label: desktop-main
        ledger_dir: ~/.hermes/mev-cadence/ledgers
        campaign_root: ~/.mev-cadence
        seat_state_root: ~/.mev-cadence/seat-state
        log_path: ~/icm-knowledge-vault/log.md
        engraphis_db: ~/.engraphis/memory.db
        tools_dir: ~/.mev-cadence/tools
```

## Configuration

| Setting | Default | Purpose |
|---|---|---|
| `campaign_root` | `MEV_CAMPAIGN_ROOT` env, else `~/.mev-cadence` | micro-eviction campaign tree root |
| `db_path` | `<HERMES_HOME>/state.db` | Hermes session store |
| `ledger_dir` | `<campaign_root>/ledgers` | durable slice ledger + cursor dir |
| `seat_label` / `desktop_seat_label` | unset (mirror duty inert) | 75k seat-state labels |
| `seat_state_root` | `MEV_SEAT_STATE_ROOT` env, else `~/.mev-cadence/seat-state` | vault root holding `log/seat-state` |
| `log_path` | `~/icm-knowledge-vault/log.md` | knowledge-vault summary log |
| `engraphis_db` | `ENGRAPHIS_LIVE_DB`, else `~/.engraphis/memory.db` | Engraphis store for staging recount |
| `t2_llm` | `false` | bind T2 fallback transport (mechanical T1 only) |
| `tools_dir` | `~/.mev-cadence/tools` | optional `seat75k.py` interval source |

Environment overrides: `MEV_VAULT_HOOK_OFF` (1/true/yes/on disables),
`MEV_CAMPAIGN_ROOT`, `MEV_SEAT_STATE_ROOT`, `HERMES_HOME`.

## Behavior

- Fire cadence every turn with ordinal >= 6; below that, no-op.
- Fail closed on vault errors; never raise into the host pipeline.
- Staging writes only to the configured workspace via the sink path; when no
  Engraphis store is configured, capture still proceeds (ledger only).

## Development

Run the suite from the repo root:

```bash
python -m pytest tests -q
```

The suite is fixture-isolated: it never writes to the real `~/.hermes/plugins/`,
`state.db`, or `~/.engraphis/memory.db`.

## License

Apache-2.0 (see `LICENSE`). Attribution and lineage in `NOTICE`; NVIDIA skills
referenced for structure are Apache-2.0 (c) NVIDIA Corporation.
