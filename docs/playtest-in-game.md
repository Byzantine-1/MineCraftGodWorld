# In-Game Playtest Protocol (Tonight)

This playtest flow uses the Mineflayer bridge as a runtime adapter:

- Minecraft chat -> bridge -> `talk <agent> <message>` -> CLI engine
- CLI engine stdout `Mara|Eli|Nox: ...` -> bridge -> bot chat

Command surfaces in this repo:

- CLI parser (`src/commandParsers.js`): `talk`, `god`, `exit`
- CLI banner (`src/index.js`): `talk <agent> <message>`, `god <command>`, `exit`
- God command parser (`src/godCommands.js`): `inspect`, `status`, `clock`, `event`, `mood`, `threat`, `faction`, `rep`, `market`, `offer`, `trade`, `quest`, `town`, `chronicle`, `news`, `mark`, `job`, `loop`, `leader`, `freeze`, `unfreeze`, `intent`, `trait`, `title`, `say`

## 1) Setup (3 Terminals)

Terminal A (Minecraft server):

1. Start your Minecraft server on `MC_HOST`/`MC_PORT` (default `127.0.0.1:25565`).

Terminal B (bridge):

```powershell
# optional (or use .env)
$env:MC_HOST="127.0.0.1"
$env:MC_PORT="25565"
$env:MC_VERSION="1.20.1"
$env:BOT_NAMES="mara,eli,nox"
$env:CHAT_PREFIX=""   # set "!" to require !mara hello style input
npm run bots
```

Terminal C (optional direct engine console):

```powershell
node src/index.js
```

Useful direct checks in Terminal C:

```text
god status
god inspect world
god inspect Mara
```

## 2) Smoke Test (Routing + No Echo Loops)

In Minecraft chat, send:

```text
mara hello
eli hello
nox hello
```

Expected:

- Bridge routes each line to `talk mara|eli|nox ...`.
- `mara` and `eli` each produce one reply, no repeated echo spam.
- `nox` path is routed correctly; current CLI runtime in `src/index.js` only defines `Mara` and `Eli`, so `nox` will be rejected by engine (`No agent named "nox".`) unless runtime agent config is expanded.

## 3) Parser Edge Cases (Stability)

Try the following in Minecraft chat:

```text
mara
mara    
eli        hello with     extra spaces
nox ???!!! -- punctuation -- ???
mara this-is-a-long-message-with-many-characters-repeat-repeat-repeat-repeat-repeat-repeat
```

Expected:

- No process crash in bridge or engine.
- Invalid/empty-target payloads are ignored or rejected cleanly.
- Valid lines route once; no loop amplification.

## 4) State Verification

Run from Terminal C:

```text
god inspect world
god inspect Mara
god news tail 20
god chronicle tail 20
god town list
god town board alpha 10
```

What to verify:

- `GOD INSPECT WORLD METRICS` shows sane counters (no runaway duplicates/aborts).
- `GOD NEWS TAIL` and `GOD CHRONICLE TAIL` remain bounded (`total` should not exceed engine cap behavior).
- Town board renders offers/quests/decisions without errors.

## 5) Economy + Markets Scenario

Run from Terminal C:

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

Validation targets:

- Integer-only amount policy: `god mint Mara 1.5` should be rejected (`Invalid amount.`).
- No double-pay on retries: rerun the exact same trade command quickly; duplicate should be ignored, balances should not double-shift.
- Offer inventory decrements correctly (`remaining`, `active`).

## 6) Quest Scenario

Trade quest path (offer/list/accept/progress/complete behavior):

```text
god quest offer alpha trade_n 2 bazaar 20
god quest list alpha
god quest accept Mara <quest_id>
god quest show <quest_id>
god quest complete <quest_id>
god trade bazaar <offer_id> Mara 1
god quest show <quest_id>
god trade bazaar <offer_id_or_next> Mara 1
god quest show <quest_id>
```

Visit quest path:

```text
god quest offer alpha visit_town beta 25
god quest list alpha
god quest accept Mara <visit_quest_id>
god quest visit <visit_quest_id>
god quest show <visit_quest_id>
```

Validation targets:

- Stable state transitions: `offered -> accepted/in_progress -> completed`.
- Reward pays once; replaying completion-style commands should not pay twice.
- `god quest complete <quest_id>` on unsatisfied objective should reject cleanly.

## 7) Clock / Nightfall / Event Deck Scenario

Run from Terminal C:

```text
god event seed 777
god event list
god clock
god clock advance 1
god event list
god decision list alpha
god mood list
god news tail 10
god clock advance 1
god event list
```

Validation targets:

- `god clock advance` flips phase each tick.
- Exactly one event draw occurs on nightfall ticks (`clock_advance` calls one `drawAndApplyWorldEvent` when phase becomes `night`).
- Event deck is deterministic by `seed` + `index`.
- Nightfall draw appends deterministic event/mood/news hooks and opens one mayor decision for the drawn event.

## 8) Mini Soak (Manual Paste Block, 24 Lines)

Paste in Minecraft chat (or use `docs/playtest-macro.txt` for a longer run):

```text
mara ping 001
eli ping 002
nox ping 003
mara ping 004!
eli ping 005?
nox ping 006...
mara    ping    007
eli     ping    008
nox     ping    009
mara route-check alpha_hall
eli route-check alpha_hall
nox route-check alpha_hall
mara punctuation !!! ??? ###
eli punctuation ;;; ::: ,,,
nox punctuation (( )) [[ ]]
mara long aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
eli long bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
nox long cccccccccccccccccccccccccccccccccccccccc
mara ping 019
eli ping 020
nox ping 021
mara ping 022
eli ping 023
nox ping 024
```

Validation targets:

- No disconnects.
- Latency remains stable.
- No runaway spam or bot-to-bot feedback loops.

## 9) Crash Safety Quick Check (Optional)

1. Press `Ctrl+C` in Terminal B (bridge).
2. Restart bridge: `npm run bots`.
3. In Terminal C, run:

```text
god inspect world
god news tail 5
god chronicle tail 5
```

Expected:

- Engine state loads cleanly.
- No corruption symptoms or parser crashes on restart.

## 10) Playtest Report Template

Use this template after run:

```text
Date:
Git commit:
Node version:
Minecraft server version:
Bridge env: MC_HOST= / MC_PORT= / MC_VERSION= / BOT_NAMES= / CHAT_PREFIX=

Subsystem Results:
- Routing (mara/eli/nox): PASS/FAIL
- Echo-loop suppression: PASS/FAIL
- Parser edge cases: PASS/FAIL
- Inspect/state checks: PASS/FAIL
- Economy/market flow: PASS/FAIL
- Quest flow: PASS/FAIL
- Clock/nightfall/event flow: PASS/FAIL
- Mini soak stability: PASS/FAIL
- Crash/restart check: PASS/FAIL

Errors / Logs:
- Paste key bridge stderr/stdout lines:
- Paste key engine output lines:

Gameplay Notes:
- Felt fun:
- Felt broken:
- Highest-priority fix before next playtest:
```

## Tonight Run (Short Path)

1. Start server (Terminal A).
2. Start bridge (Terminal B): `npm run bots`.
3. Optional inspect console (Terminal C): `node src/index.js`.
4. Run Sections 2, 4, 5, 7.
5. If stable, run Section 8 soak and file Section 10 report.
