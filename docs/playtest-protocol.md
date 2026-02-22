# Mineflayer Bridge Playtest Protocol

This protocol enables in-game playtesting without changing engine semantics. The bridge is runtime-only and forwards chat to the existing CLI engine command plane.

## Scope Guardrails

- No gameplay feature changes.
- No world loop or god command behavior changes.
- No memory schema changes.
- No direct writes to `src/memory.json` from the bridge.

## Preflight (Tonight Gate)

```powershell
npm test
npm ls mineflayer dotenv
```

Expected:

- Tests pass.
- `mineflayer` and `dotenv` are installed.

## Runtime Configuration

- `MC_HOST` default `127.0.0.1`
- `MC_PORT` default `25565`
- `MC_VERSION` optional (unset/blank means auto)
- `BOT_NAMES` default `mara,eli,nox`
- `CHAT_PREFIX` default empty string

## Start Commands

Direct:

```powershell
npm run bots
```

Optional macro:

```powershell
npm run playtest:bots
```

Custom host/port/version with direct launch:

```powershell
$env:MC_HOST="127.0.0.1"
$env:MC_PORT="25565"
$env:MC_VERSION=""   # blank for auto
$env:BOT_NAMES="mara,eli,nox"
$env:CHAT_PREFIX=""
npm run bots
```

## Input Contract (Minecraft -> Engine)

Accepted chat patterns:

- `mara <message>`
- `eli <message>`
- `nox <message>`

When `CHAT_PREFIX` is set (example `!`), require:

- `!mara <message>`
- `!eli <message>`
- `!nox <message>`

Bridge forwarding:

- `talk mara <message>`
- `talk eli <message>`
- `talk nox <message>`

## Output Contract (Engine -> Minecraft)

The bridge relays stdout lines matching:

- `Mara: <text>` (or `> Mara: <text>`)
- `Eli: <text>`
- `Nox: <text>`

Only `<text>` is sent back as bot chat.

## Playtest Procedure

1. Start your Minecraft server and wait until it is accepting joins.
2. Run `npm run bots`.
3. Confirm spawn logs for each bot.
4. In Minecraft chat, issue:
   - `mara hello`
   - `eli status`
   - `nox gather at alpha_hall`
5. Verify each target bot responds once and no bot-to-bot echo loop appears.
6. If using prefix mode, repeat with `!mara hello` style commands.
7. Press `Ctrl+C` in bridge terminal and confirm clean shutdown logs.

## Runtime Safety Checks

- Self-chat is ignored.
- Messages from other configured bot usernames are ignored.
- Repeated identical reply per bot is deduplicated.
- `Ctrl+C` sends `exit` to engine and quits bots.

## Troubleshooting

- No bot spawn:
  - Verify `MC_HOST`, `MC_PORT`, and server reachability.
  - If protocol mismatch, set explicit `MC_VERSION`.
- Chat not forwarded:
  - If `CHAT_PREFIX` is non-empty, ensure prefix is present.
  - Ensure target is one of `BOT_NAMES`.
- Bot not replying:
  - Confirm engine is running (bridge process starts `src/index.js`).
  - Inspect bridge stderr for `[Engine STDERR]` lines.
- Unexpected relay format:
  - Adjust bridge `replyRegex` only after observing real engine stdout.

