# Release Gates

This is the fast pre-merge checklist for Trader Mode changes.

## Gate A (Engine) - ~5 minutes

Run from repo root:

```powershell
npm test
node scripts/stressTest.js --agents=3 --tier=2 --timers
```

Pass conditions:

- `npm test` passes.
- Stress output shows:
  - `LOCK_TIMEOUTS: 0`
  - `INTEGRITY_OK: true`
  - `UNHANDLED_REJECTIONS: 0`

## Gate B (Blackbox) - external repo

Run from this repo root:

```powershell
$env:BLACKBOX_TEST_DIR="C:\path\to\minecraft-god-mvp-blackbox"
$env:PUBLIC_REPO_DIR="C:\Users\the10\Projects\minecraft-god-mvp"
npm run test:blackbox
```

Required external suites in the blackbox repo:

- read-only immutability (memory hash unchanged)
- replay storm (duplicate eventIds)
- crash/restart window safety
- invalid-input fuzz (CLI + bridge payloads)
- concurrency exclusion/contention

Artifacts to keep on failure:

- blackbox logs
- memory snapshots before/after
- harness JSON report if available

## Gate C (In-Game Smoke) - ~2 minutes

Terminal A (Paper server):

```powershell
java -Xms1G -Xmx2G -jar .\paper.jar nogui
```

Terminal B (bridge):

```powershell
npm run bots
```

Terminal C (optional CLI):

```powershell
node src/index.js
```

In Minecraft chat:

```text
mara hello
eli hello
```

If `CHAT_PREFIX=!`:

```text
!mara hello
!eli hello
```

Expected:

- Mara and Eli each reply once.
- No echo-loop spam.
- Bots stay connected in TAB list.

Quick contract verification from Terminal C:

```text
god contract list alpha
god contract accept Mara <contract_id>
god quest show <contract_id>
god balance Mara
```

## Macro Usage

Generate macros:

```powershell
npm run gen:macro
```

Outputs:

- `docs/playtest-smoke-macro.txt` (10-15 line smoke run)
- `docs/playtest-macro.txt` (60+ line soak run)

Use chat sections in Minecraft only. Run the CLI section in `node src/index.js`.

## When Gate C Must Be Re-Run

Re-run Gate C when any of these change:

- `src/minecraftBridge.js`
- Mineflayer / protocol dependency versions
- Node runtime version used for bridge
- Paper jar / server version
- chat parsing or `CHAT_PREFIX` behavior
- bot names / host / port configuration

If none of the above changed, Gate C can be run at milestones or before merge.
