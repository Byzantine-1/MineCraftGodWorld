# In-Game Trader Mode Playtest (Tonight)

Bridge path is runtime-only:

- Minecraft chat -> Mineflayer bridge -> `talk <agent> <message>` -> CLI engine (`src/index.js`)
- Engine stdout `Mara: ...` / `Eli: ...` -> matching bot chat in Minecraft

Current command surfaces (from code):

- CLI parser (`src/commandParsers.js`): `talk`, `god`, `exit`
- CLI banner (`src/index.js`): `talk <agent> <message>`, `god <command>`, `exit`
- God commands (`src/godCommands.js`) used in this playtest:
  - Economy/market: `mint`, `transfer`, `balance`, `market`, `offer`, `trade`
  - Quests/contracts: `quest`, `contract`
  - World: `town`, `clock`, `event`, `mood`, `threat`, `rumor`
  - Inspect/logging: `inspect`, `status`, `chronicle`, `news`
  - Compatibility only: `decision` (deprecated in Trader Mode)

## 1) Setup (3 terminals)

Terminal A (Minecraft server):

1. Start Paper server on `127.0.0.1:25565` (or your configured host/port).

Terminal B (bridge):

```powershell
# optional if not already in .env
$env:MC_HOST="127.0.0.1"
$env:MC_PORT="25565"
$env:MC_VERSION="1.21.11"
$env:BOT_NAMES="mara,eli,nox"
$env:CHAT_PREFIX="!"   # recommended; use !mara hello
npm run bots
```

Terminal C (optional direct CLI for god commands/inspect):

```powershell
node src/index.js
```

Quick check in Terminal C:

```text
god status
god inspect world
god inspect Mara
```

## 2) Smoke test (routing + no echo loops)

Minecraft chat:

```text
mara hello
eli hello
nox hello
```

If `CHAT_PREFIX=!`:

```text
!mara hello
!eli hello
!nox hello
```

Expected:

- 1 reply each for live agents.
- No bot-to-bot loop spam.
- `nox` may be routed but not answered if runtime agent list only includes Mara/Eli.

## 3) Parser edge cases (stability)

In Minecraft chat, try:

```text
mara
mara    
eli        hello with     extra spaces
nox ???!!! -- punctuation -- ???
mara this-is-a-long-message-with-many-characters-repeat-repeat-repeat-repeat-repeat-repeat
```

Expected:

- No bridge or engine crash.
- Invalid/blank payloads ignored or rejected cleanly.
- Valid inputs produce single responses.

## 4) State verification

Terminal C commands:

```text
god inspect world
god inspect Mara
god town list
god town board alpha 10
god market pulse alpha
god market pulse world
god contract list alpha
god rumor list alpha 10
god news tail 20
god chronicle tail 20
```

Verify:

- Town board has `MARKET PULSE`, `ROUTE RISK`, `CONTRACTS`, `RUMOR LEADS`.
- No `OPEN DECISION` section on town board.
- News/chronicle feeds stay bounded and readable.
- No duplicate durable effects on repeated operation IDs.

## 5) Economy + markets scenario

Terminal C:

```text
god mark add alpha_hall 0 64 0 town:alpha
god market add bazaar alpha_hall
god mint Mara 50
god mint Eli 50
god transfer Mara Eli 5
god offer add bazaar Mara sell 3 10
god offer list bazaar
god trade bazaar <offer_id> Eli 1
god balance Mara
god balance Eli
god offer list bazaar
```

Validate:

- Integer-only policy: `god mint Mara 1.5` is rejected.
- Replay safety: repeating same `trade` operation id does not double-pay.
- Offer amount decrements correctly; inactive offers stop trading.

## 6) Quest scenario

Main quest flow:

```text
god quest offer alpha trade_n 2 bazaar 20
god quest list alpha
god quest accept Mara <quest_id>
god quest show <quest_id>
god trade bazaar <offer_id> Mara 1
god trade bazaar <offer_id> Mara 1
god quest complete <quest_id>
god quest show <quest_id>
```

Visit quest flow:

```text
god quest offer alpha visit_town beta 25
god quest list alpha
god quest accept Mara <visit_quest_id>
god quest visit <visit_quest_id>
god quest complete <visit_quest_id>
```

Validate:

- State transitions are stable (`offered -> accepted/in_progress -> completed`).
- Rewards pay once.
- Invalid completions are rejected without mutation.

## 7) Clock / nightfall / event deck scenario

Terminal C:

```text
god event seed 777
god clock
god clock advance 1
god event list
god mood list
god news tail 10
god clock advance 1
god event list
god market pulse world
god contract list
god decision list alpha
```

Validate:

- `clock advance` toggles phase day/night.
- Exactly one event draw occurs on each nightfall transition.
- Event draw + mood/news hooks are deterministic under seed and replay-safe.
- Trader mode: no new decisions are generated (`decision` outputs are deprecated compatibility only).

## 8) Mini soak (manual paste block, 30 lines)

Paste in Minecraft chat in batches:

```text
mara ping 001
eli ping 002
nox ping 003
mara ping 004!
eli ping 005?
nox ping 006...
mara   ping   007
eli    ping   008
nox    ping   009
mara route-check alpha_hall
eli route-check alpha_hall
nox route-check alpha_hall
mara punctuation !!! ??? ###
eli punctuation ;;; ::: ,,,
nox punctuation (( )) [[ ]]
mara long aaaaaaaaaaaaaaaaaaaaaaaaaaaaa
eli long bbbbbbbbbbbbbbbbbbbbbbbbbbbbb
nox long ccccccccccccccccccccccccccccc
mara ping 019
eli ping 020
nox ping 021
mara ping 022!
eli ping 023?
nox ping 024...
mara mixed alpha-beta_gamma.025
eli mixed alpha-beta_gamma.026
nox mixed alpha-beta_gamma.027
mara final-check 028
eli final-check 029
nox final-check 030
```

Validate:

- No disconnects.
- Stable latency.
- No runaway reply loops/spam.

Use `docs/playtest-macro.txt` for a longer 50-100 line run.

## 9) Crash safety quick check (optional)

1. Press `Ctrl+C` in bridge terminal.
2. Restart bridge: `npm run bots`.
3. In Terminal C:

```text
god inspect world
god contract list
god news tail 5
god chronicle tail 5
```

Expected:

- Clean restart.
- State loads and reads correctly.
- No corruption symptoms.

## 10) Playtest report template

```text
Date:
Git commit:
Node version:
Java version:
Minecraft server version:
Bridge env: MC_HOST= / MC_PORT= / MC_VERSION= / BOT_NAMES= / CHAT_PREFIX=

Subsystem Results:
- Routing (mara/eli/nox): PASS/FAIL
- Echo-loop suppression: PASS/FAIL
- Parser edge cases: PASS/FAIL
- State inspect checks: PASS/FAIL
- Economy/market flow: PASS/FAIL
- Contract flow: PASS/FAIL
- Quest flow: PASS/FAIL
- Clock/nightfall/event flow: PASS/FAIL
- Mini soak stability: PASS/FAIL
- Crash/restart check: PASS/FAIL

Errors / Logs:
- Bridge output:
- Engine output:

Gameplay Notes:
- Felt fun:
- Felt broken:
- Highest-priority fix:
```

## Tonight run (short path)

1. Start server (Terminal A).
2. Start bridge (Terminal B): `npm run bots`.
3. Optional direct console (Terminal C): `node src/index.js`.
4. Run Sections 2, 4, 5, 7.
5. If stable, run Section 8 and fill Section 10 report.
