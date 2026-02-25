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
  - deterministic town pressure meters (`hope`/`dread`) updated on committed outcomes
  - war-bulletin town board pulse card (frontline, pressure, rations/strain, impacts, orders)
  - deterministic project + salvage systems (`god project ...`, `god salvage ...`) for build/explore loops
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
god project start alpha trench_reinforcement
god project list alpha
god project advance alpha <project_id>
god project complete alpha <project_id>
god salvage plan alpha no_mans_land_scrap
god salvage list alpha
god salvage resolve alpha <run_id> secure

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
- Pressure + impact additive keys:
  - `world.towns.<town>.hope`
  - `world.towns.<town>.dread`
  - `world.towns.<town>.recentImpacts[]` (bounded)
- Project + salvage additive keys:
  - `world.projects[]` (bounded)
  - `world.salvageRuns[]` (bounded)
  - `world.projects[].supportsMajorMissionId` (optional link seam)
  - `world.salvageRuns[].supportsMajorMissionId` (optional link seam)
  - `world.salvageRuns[].supportsProjectId` (optional link seam)
  - `world.towns.<town>.recentImpacts[].projectId` / `.salvageRunId` (optional refs)

## War Bulletin Pulse Card

`god town board <town> [limit]` now opens with a grimdark pulse card while keeping existing board sections.

Example header lines:

```text
GOD TOWN BOARD WAR FRONTLINE STATUS: label=SIEGE PRESSURE score=92 threat=65 mission=active nether=SCARCITY
GOD TOWN BOARD WAR HOPE DREAD: hope=57(rising) dread=61(high)
GOD TOWN BOARD WAR RATIONS STRAIN: rations=41 strain=69 outlook=strained
GOD TOWN BOARD WAR WHAT CHANGED TODAY: count=3
GOD TOWN BOARD WAR ORDERS OF THE DAY: mission=Hold mm_alpha_1 phase 2. Reinforce supply routes.
GOD TOWN BOARD PROJECTS: count=1
GOD TOWN BOARD PROJECT: id=pr_alpha_trench_reinforcement_12 type=trench_reinforcement status=active stage=2 updated_day=12 supports_major_mission_id=mm_alpha_1
GOD TOWN BOARD SALVAGE: id=sr_alpha_no_mans_land_scrap_12 target=no_mans_land_scrap status=resolved planned_day=12 resolved_day=12 outcome=secure supplies=5 supports_major_mission_id=mm_alpha_1 supports_project_id=pr_alpha_trench_reinforcement_12
```

## Hope / Dread Mapping

Pressure meters are deterministic and only move inside committed mutations:

- nether event application: mapping by event type (for example `LONG_NIGHT`, `SCARCITY`, `THREAT_SURGE`)
- major mission completion/failure
- townsfolk quest completion/failure

Replay of the same operation/event id remains a durable no-op, so meter deltas do not double-apply.

## Projects + Salvage (Phase 3B)

These commands are deterministic engine abstractions for build/explore loops. They do not require live block counting or embodiment integration.

Built-in project types:

- `trench_reinforcement`
- `watchtower_line`
- `ration_depot`
- `field_chapel`
- `lantern_line`

Built-in salvage target keys:

- `no_mans_land_scrap`
- `ruined_hamlet_supplies`
- `abandoned_shrine_relics`
- `collapsed_tunnel_tools`

Built-in salvage resolve outcomes:

- `secure`
- `contested`
- `botched`

Example command outputs:

```text
GOD PROJECT START: town=alpha type=trench_reinforcement status=created project_id=<id> stage=1
GOD PROJECT ADVANCE: town=alpha project_id=<id> status=active stage=2
GOD PROJECT COMPLETE: town=alpha project_id=<id> status=completed stage=3
GOD SALVAGE PLAN: town=alpha target=no_mans_land_scrap status=created run_id=<id>
GOD SALVAGE RESOLVE: town=alpha run_id=<id> target=no_mans_land_scrap outcome=secure supplies=5
```

Replay safety:

- repeated command retries under the same operation/event identity are durable no-ops
- bounded history is enforced in sanitize and on enqueue for `world.projects[]` and `world.salvageRuns[]`

ID semantics (Option A, intentional):

- `god project start <town> <projectType>` is deterministic per `(day, town, projectType)` and returns the same `project_id` on repeats that day
- `god salvage plan <town> <targetKey>` is deterministic per `(day, town, targetKey)` and returns the same `run_id` on repeats that day
- repeated same-day requests return `status=existing` and do not append extra news/chronicle/crier/recent-impact entries
- repeated same-day requests do not apply extra pressure/threat/nether deltas

Deterministic timestamping:

- durable feed timestamps (`news`, `chronicle`, crier-linked announcements) are derived from durable state + command identity, not OS wall clock
- replay of the same committed operation does not create duplicate feed rows

## Package Boundaries

Current package scripts include both engine-only and transitional bridge/testing tooling.

Engine-only aliases:

- `npm run engine:cli`
- `npm run engine:test`
- `npm run engine:smoke`
- `npm run engine:stress`
- `npm run engine:scale`

Transitional aliases (kept for current workflows, candidate for repo split):

- `npm run bridge:bots`
- `npm run bridge:playtest`
- `npm run qa:blackbox`

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
