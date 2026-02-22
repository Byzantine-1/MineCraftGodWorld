# Production Hardening Notes

## Scope

This document defines hard guarantees for the engine runtime and durable state model.

## Core Assumptions

- Runtime is Node.js with a single-threaded event loop.
- Durable state is persisted through transactional mutation functions in `src/memory.js`.
- Inputs from CLI/chat/automation are untrusted and must be validated before mutation.

## State Mutation Contract

- All durable writes must run inside `memoryStore.transact(...)`.
- Every mutating command must use an eventId guard.
- Replay of the same eventId must be a no-op for durable state.
- Validation failures must perform no durable writes.
- Runtime side effects (chat prints, crier broadcasts, runtime hooks) execute only after commit.

## Read-Only Contract

- Read-only commands must not call `transact`.
- Memory snapshot before/after a read-only command must be identical.

## Additive Schema Contract

Allowed schema evolution is additive and backward compatible only.

Current additive durable domains include:

- `world.economy`
- `world.markets`
- `world.chronicle`
- `world.news`
- `world.clock`
- `world.threat`
- `world.factions`
- `world.moods`
- `world.events`
- `world.quests`
- `world.rumors`
- `world.decisions`
- `agent.profile.job`
- `agent.profile.rep`
- `agent.profile.traits`
- `agent.profile.titles`
- `agent.profile.rumors_completed`

Sanitization rules:

- Invalid entries are dropped or clamped based on field policy.
- Numeric values requiring integer semantics are validated as integers in command handlers.
- Feed lists remain bounded (`chronicle`, `news` caps).

## Invariants To Keep Green

- Memory snapshots returned to callers are defensive copies.
- `trust` is clamped to `[0, 10]`.
- `legitimacy` is clamped to `[0, 100]`.
- Threat and mood meters are clamped to valid ranges.
- Feed caps are enforced.
- Titles are unique per agent.
- Ledger balances remain finite and non-negative.

## Failure Handling

- Corrupt `memory.json` falls back to safe defaults and logs warning.
- Durable persistence failures are fatal `AppError`s.
- Recoverable `AppError`s map to safe user-facing no-op responses.
- Unrecoverable failures trigger safe shutdown pathways.

## Operational Verification

Always gate with:

```powershell
npm test
node scripts/stressTest.js --agents=3 --tier=2 --timers
```

Expected invariant lines:

- `LOCK_TIMEOUTS: 0`
- `INTEGRITY_OK: true`
- `UNHANDLED_REJECTIONS: 0`
