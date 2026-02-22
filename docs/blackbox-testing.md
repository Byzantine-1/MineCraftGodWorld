# Blackbox Testing

## Purpose

Blackbox tests live outside this repository to reduce overfitting in the main project.
This repository only runs that external suite.

## Required Environment Variables

- `BLACKBOX_TEST_DIR`: path to the blackbox test repo.
- `PUBLIC_REPO_DIR`: path to this main repo (used by the blackbox harness to target the correct project).

## Local Usage (PowerShell)

```powershell
$env:BLACKBOX_TEST_DIR="C:\path\to\minecraft-god-mvp-blackbox"
$env:PUBLIC_REPO_DIR="C:\path\to\minecraft-god-mvp"
npm run test:blackbox
```

If `BLACKBOX_TEST_DIR` is not set, runner output is:

`BLACKBOX_TESTS: SKIPPED (BLACKBOX_TEST_DIR not set)`

and it exits successfully.

## CI Usage Pattern

1. Check out this repository.
2. Check out the private blackbox repository (for example `./.blackbox-tests`).
3. Set environment variables:
   - `BLACKBOX_TEST_DIR=./.blackbox-tests`
   - `PUBLIC_REPO_DIR=$PWD`
4. Run:

```bash
npm run test:blackbox
```

## Runner Behavior

- The runner executes in `BLACKBOX_TEST_DIR`.
- It tries `npm ci` first.
- If `npm ci` fails, it falls back to `npm install`.
- Then it runs `npm test`.

## Security Note

Keep the blackbox repository private.
In CI, use secrets/tokens for access.