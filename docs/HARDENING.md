# Production Hardening Notes

## Assumptions
- Runtime is Node.js single-threaded event loop.
- Memory persistence uses synchronous file I/O to keep state transitions atomic per operation.
- All externally supplied commands are untrusted and must be validated.

## State Transitions
- CLI and Minecraft chat flow through shared domain services:
  1. Parse command (`src/commandParsers.js`)
  2. Record incoming interaction (`src/turnEngine.js`)
  3. Generate dialogue (`src/dialogue.js`)
  4. Sanitize/validate turn (`src/turnGuard.js`)
  5. Apply deterministic actions (`src/actionEngine.js`)
  6. Persist memory and world effects (`src/memory.js`)

## Invariants
- Memory snapshots returned from store are immutable copies.
- Trust is clamped to `[0, 10]`.
- Player legitimacy is clamped to `[0, 100]`.
- Duplicate operation IDs are idempotent and ignored.
- Structured JSON logs are emitted for all critical operations.

## Failure Conditions
- Corrupt `memory.json` resets to safe defaults and logs warning.
- Memory persistence failure throws fatal `AppError`.
- Recoverable `AppError` is handled with safe user-facing message.
- Unrecoverable failures trigger crash handler and safe shutdown callback.
