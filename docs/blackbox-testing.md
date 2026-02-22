# Blackbox Testing

## Purpose

Blackbox tests live outside this repository to reduce overfitting and validate behavior from an external harness.

This engine repo provides:

- `npm run test:blackbox` runner hook
- stable command and durability invariants that blackbox tests can target

## Required Environment Variables

- `BLACKBOX_TEST_DIR`: absolute path to the blackbox repo.
- `PUBLIC_REPO_DIR`: absolute path to this engine repo.

## Local Usage (PowerShell)

```powershell
$env:BLACKBOX_TEST_DIR="C:\path\to\minecraft-god-mvp-blackbox"
$env:PUBLIC_REPO_DIR="C:\path\to\minecraft-god-mvp"
npm run test:blackbox
```

If `BLACKBOX_TEST_DIR` is missing, runner output is:

`BLACKBOX_TESTS: SKIPPED (BLACKBOX_TEST_DIR not set)`

and exits successfully.

## CI Usage Pattern

1. Check out this repository.
2. Check out private blackbox tests (for example `./.blackbox-tests`).
3. Set:
   - `BLACKBOX_TEST_DIR=./.blackbox-tests`
   - `PUBLIC_REPO_DIR=$PWD`
4. Run:

```bash
npm run test:blackbox
```

## Runner Behavior

- Executes in `BLACKBOX_TEST_DIR`.
- Attempts `npm ci` first.
- Falls back to `npm install` if needed.
- Runs `npm test`.

## Recommended Blackbox Coverage (Current Engine)

For stronger external validation, include suites for:

- replay/idempotency under duplicate eventIds
- read-only command no-mutation guarantees
- rumor/decision/side-quest lifecycle
- title award uniqueness and replay safety
- feed cap and ordering behavior (`news`, `chronicle`)
- stress + concurrency with invariant gates

Suggested invariant assertions:

- `LOCK_TIMEOUTS == 0`
- `INTEGRITY_OK == true`
- `UNHANDLED_REJECTIONS == 0`

## Security Note

Keep blackbox repositories private. Use CI secrets/tokens for authenticated checkout.
