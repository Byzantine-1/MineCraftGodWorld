# minecraft-god-mvp

Durable AI world engine for Minecraft-style multi-agent simulation.

This repository is the engine layer only. It focuses on deterministic behavior, transactional durability, and replay-safe command handling.

## What Is Implemented

- Core agent turn loop with durable memory and idempotent operation processing.
- God command plane for:
  - markers, jobs, roster
  - economy ledger (mint/transfer/balance)
  - markets/offers/trades
  - town board, chronicle, news
  - clock/seasons/threat/factions/rep
  - moods and deterministic event deck
  - mayor-gated major missions with per-town crier queue
  - deterministic nether global shocks with bounded ledger/modifiers
  - townsfolk-driven side quests with deterministic dedupe/linkage
  - rumors, mayor decisions, side quests
  - traits and titles
- Runtime Town Crier (optional, default off) that reads durable news and broadcasts runtime-only.

## Engine Guarantees

- Durable mutations are transactional and eventId-guarded.
- Read-only commands do not mutate `memory.json`.
- Validation failures are clean no-ops.
- Runtime side effects execute only after durable commit.
- Additive schema evolution only (backward compatible load/sanitize).

## Quick Start

```powershell
npm ci
npm run cli
```

At the CLI prompt, use `god ...` commands.

## Minimal Command Walkthrough

```text
god mark add alpha_hall 0 64 0 town:alpha
god mark add beta_gate 200 64 0 town:beta

god faction set alpha iron_pact
god faction set beta veil_church

god mint Mara 50
god mint Eli 50
god market add bazaar alpha_hall
god offer add bazaar Eli sell 3 10
god offer list bazaar
god trade bazaar <offer_id> Mara 2

god clock
god clock advance 1
god decision list alpha
god decision choose <decision_id> <option_key>

god rumor list alpha
god mayor talk alpha
god mayor accept alpha
god mission status alpha
god mission advance alpha
god mission complete alpha
god nether status
god nether tick 3
god townsfolk talk alpha gate_warden

god town board alpha 10
god news tail 10
god chronicle tail 10
```

Major mission failure path:

```text
god mission fail alpha route collapsed
```

## Durable State Location

- Durable state file used by CLI: `src/memory.json`
- Major mission state keys:
  - `world.majorMissions[]`
  - `world.towns.<town>.activeMajorMissionId`
  - `world.towns.<town>.majorMissionCooldownUntilDay`
  - `world.towns.<town>.crierQueue[]`
- Nether + townsfolk additive keys:
  - `world.nether.eventLedger[]`
  - `world.nether.modifiers`
  - `world.nether.deckState`
  - `world.nether.lastTickDay`
  - `world.quests[].origin`
  - `world.quests[].townId`
  - `world.quests[].npcKey`
  - `world.quests[].supportsMajorMissionId`

## Test And Validation

Public unit/integration suite:

```powershell
npm test
```

Required stress validation:

```powershell
node scripts/stressTest.js --agents=3 --tier=2 --timers
```

Scale profiling:

```powershell
node scripts/scaleValidation.js --fresh-csv --repeats=5 --timers --agent-series=1,3,5 --tier=2
```

External blackbox suite:

```powershell
npm run test:blackbox
```

See `docs/blackbox-testing.md` for environment setup and runner behavior.

## Documentation Index

- `docs/runbook.md` - operations and verification runbook
- `docs/HARDENING.md` - durability and invariant notes
- `docs/blackbox-testing.md` - external blackbox usage
- `docs/scaling-ceiling.md` - scale findings and guardrails

## License

This repository is licensed under ISC. See `LICENSE`.

For GitHub license detection, ensure `LICENSE` is committed at repository root and pushed to the default branch.
