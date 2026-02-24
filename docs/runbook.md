# Operations Runbook

## Scope

This runbook covers local bring-up, verification, and operational checks for the engine repo.

## 1) Preflight

```powershell
cd C:\path\to\minecraft-god-mvp
npm ci
```

## 2) Baseline Gate (Required Before Changes)

```powershell
npm test
node scripts/stressTest.js --agents=3 --tier=2 --timers
```

Expected:

- `LOCK_TIMEOUTS: 0`
- `INTEGRITY_OK: true`
- `UNHANDLED_REJECTIONS: 0`

## 3) Start CLI

```powershell
npm run cli
```

## 4) Quick World Bring-Up (Copy/Paste In CLI)

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
```

Copy `offer_id` from `god offer list bazaar`, then:

```text
god trade bazaar <offer_id> Mara 2
god balance Mara
god balance Eli
```

## 5) Story Loop Verification (Clock, Events, Decisions, Rumors)

```text
god clock
god clock season long_night
god clock advance 1
god event list
god decision list alpha
god decision show <decision_id>
god decision choose <decision_id> <option_key>
god rumor list alpha 10
god news tail 10
god chronicle tail 10
```

## 6) Side Quest And Titles Verification

```text
god rumor quest <rumor_id>
god quest list alpha
god quest accept Mara <quest_id>
god quest visit <quest_id>

god rep add Mara iron_pact 5
god title Mara
god trait Mara
god town board alpha 10
```

## 7) Major Mission Verification (Mayor Gate + Crier)

```text
god mayor talk alpha
god mayor accept alpha
god mission status alpha
god mission advance alpha
god mission complete alpha
god mission fail alpha route collapsed
god town board alpha 10
god news tail 10
god chronicle tail 10
```

Expected outputs include lines like:

- `GOD MAYOR TALK: town=alpha ...`
- `GOD MAYOR ACCEPT: town=alpha ...`
- `GOD MISSION STATUS: town=alpha ...`
- `GOD TOWN BOARD MAJOR MISSION ACTIVE: ...` or `... TEASER: ...`

Durable state changes are in `src/memory.json` under:

- `world.majorMissions[]`
- `world.towns.<town>.activeMajorMissionId`
- `world.towns.<town>.majorMissionCooldownUntilDay`
- `world.towns.<town>.crierQueue[]`

## 8) Nether + Townsfolk Verification (Phase 2)

```text
god nether status
god nether tick
god nether status
god town board alpha 10
god news tail 10
god chronicle tail 10

god townsfolk talk alpha gate_warden
god townsfolk talk alpha gate_warden
god quest list alpha
god quest show <quest_id>
```

Expected outputs include lines like:

- `GOD NETHER STATUS: seed=... cursor=... last_tick_day=...`
- `GOD NETHER TICK: n_days=... applied_events=...`
- `GOD TOWN BOARD NETHER PULSE: longNight=... omen=... scarcity=... threat=...`
- `GOD TOWN BOARD SIDE QUESTS TOP: count=...`
- `GOD TOWNSFOLK TALK: town=alpha ... status=created|existing ...`
- `GOD QUEST: ... origin=townsfolk ...`

Durable state changes are in `src/memory.json` under:

- `world.nether.eventLedger[]` (bounded)
- `world.nether.modifiers`
- `world.nether.deckState`
- `world.nether.lastTickDay`
- `world.towns.<town>.crierQueue[]` (`nether_event` entries, bounded)
- `world.quests[]` townsfolk additive fields:
  - `origin: "townsfolk"`
  - `townId`
  - `npcKey`
  - `supportsMajorMissionId` (when linked)

Replay/idempotency check:

```text
god nether tick 5
god nether tick 5   # same operation replay path should not duplicate durable effects
```

In harness/integration paths where the same operation/event id is retried, duplicate re-application should be a durable no-op.

## 9) Town Crier Verification (Optional, Runtime-Only)

Start a new shell before `npm run cli`:

```powershell
$env:TOWN_CRIER_ENABLED="1"
$env:TOWN_CRIER_INTERVAL_MS="1500"
$env:TOWN_CRIER_MAX_PER_TICK="1"
$env:TOWN_CRIER_RECENT_WINDOW="25"
$env:TOWN_CRIER_DEDUPE_WINDOW="100"
npm run cli
```

Then in CLI:

```text
god inspect world
god news tail 10
```

Expected:

- periodic runtime lines prefixed with `[NEWS]` or `[NEWS:<town>]`
- no durable mutation caused by crier broadcasting

Disable by clearing env vars and restarting CLI.

## 10) Release Gate Checklist

Before merge/release:

1. `npm test` passes.
2. `node scripts/stressTest.js --agents=3 --tier=2 --timers` passes invariants.
3. No read-only command writes durable state.
4. Idempotency tests for new mutating commands are present.
5. Docs updated for any new command surface.
