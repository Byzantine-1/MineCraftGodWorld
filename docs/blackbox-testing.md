# Blackbox Testing

## Purpose

Blackbox tests are stored outside this repository to reduce overfitting during development.
This repository contains only the harness and instructions for running that external suite.

## Local Usage (PowerShell)

```powershell
$env:BLACKBOX_TEST_DIR="C:\path\to\minecraft-god-mvp-blackbox"
npm run test:blackbox
```

If `BLACKBOX_TEST_DIR` is not set, the runner prints:

`BLACKBOX_TESTS: SKIPPED (BLACKBOX_TEST_DIR not set)`

and exits successfully.

## CI Usage Pattern

1. Check out this repository.
2. Check out the private blackbox repository into a local directory such as `./.blackbox-tests`.
3. Set `BLACKBOX_TEST_DIR=./.blackbox-tests`.
4. Run:

```bash
npm run test:blackbox
```

## Security Note

The blackbox repository should be private. In CI, use secrets-based authentication/tokens for access.
