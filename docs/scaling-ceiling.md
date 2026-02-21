# Scaling Ceiling Discovery (File-Backed Transaction Store)

## Run Context
- Date: 2026-02-21
- Host OS: Microsoft Windows 11 Home (10.0.26200, 64-bit)
- CPU: AMD Ryzen 5 7535HS (6 cores / 12 logical processors, 3.30 GHz max)
- RAM: 7.21 GiB total
- Disk: C: NTFS, 475.86 GiB total, 67.94 GiB free

## Scope and Constraints
- No architecture refactor.
- No durability/locking/idempotency/persistence boundary changes during validation.
- Sequential execution only for phase runs used in this document.
- Stress command mix unchanged.

## Phase Results

### Phase 1: Baseline stability (1/3/5 agents, tier 2, repeats=5, timers on)
- Command: `node scripts/scaleValidation.js --fresh-csv --repeats=5 --timers --agent-series=1,3,5 --tier=2`
- Status: `CLEAN`
- Aggregated results:
  - 1 agents: avg 30.70 ms, p95 42.40 ms, p99 54.60 ms, max 121.40 ms
  - 3 agents: avg 28.80 ms, p95 35.60 ms, p99 41.40 ms, max 75.00 ms
  - 5 agents: avg 29.78 ms, p95 38.80 ms, p99 52.20 ms, max 96.80 ms
- Notes:
  - Stddev values were non-zero (measurement variability captured as required).
  - `lock_timeouts=0`, `integrity_ok=true` for all agg rows.

### Phase 2: Agent scaling isolation (1/3/5/7 agents, tier 2, repeats=5, timers on)
- Command: `node scripts/scaleValidation.js --fresh-csv --repeats=5 --timers --agent-series=1,3,5,7 --tier=2`
- Status: `CLEAN`
- Aggregated results:
  - 1 agents: avg 29.01 ms, p95 36.00 ms, p99 42.40 ms
  - 3 agents: avg 29.05 ms, p95 35.20 ms, p99 39.80 ms
  - 5 agents: avg 28.99 ms, p95 35.60 ms, p99 41.40 ms
  - 7 agents: avg 28.69 ms, p95 36.00 ms, p99 42.40 ms
- Timer slope signal:
  - `lock_wait_p95`: 2 -> 3 -> 3 -> 4 ms
  - `lock_wait_p99`: 2.2 -> 4 -> 4.8 -> 5 ms
- Notes:
  - p95/p99 were effectively flat through 7 agents.
  - `lock_timeouts=0`, `integrity_ok=true`.

### Phase 3: Ceiling push tier 2
- 9 agents command: `node scripts/scaleValidation.js --fresh-csv --repeats=3 --timers --agent-series=9 --tier=2`
  - Status: `CLEAN`
  - Aggregate: avg 28.84 ms, p95 35.33 ms, p99 41.67 ms, max 54.00 ms
- 11 agents command: `node scripts/scaleValidation.js --fresh-csv --repeats=3 --timers --agent-series=11 --tier=2`
  - Status: `CLEAN`
  - Aggregate: avg 29.00 ms, p95 36.00 ms, p99 40.33 ms, max 55.67 ms
- Critical stop conditions:
  - No lock timeouts
  - No integrity failures
  - p99 far below 500 ms
  - Slow-tx-rate never approached 0.10 sustained

### Phase 4: Soak at highest clean config (11 agents, tier 2, repeats=10)
- Command: `node scripts/scaleValidation.js --fresh-csv --repeats=10 --timers --agent-series=11 --tier=2`
- Status: `CLEAN`
- Aggregate: avg 27.30 ms, p95 35.00 ms, p99 40.40 ms, max 62.00 ms
- Creep check (first 3 repeats vs last 3 repeats):
  - Heap: 14.06 -> 12.81 MB (no upward creep)
  - Memory JSON size: 118088 -> 119683 bytes (small increase, not runaway)
  - p99: 43.33 -> 36.67 ms
  - max_tx: 74.67 -> 41.00 ms

### Phase 5: Crash-resume validation at highest clean config (11 agents, tier 2)
- Crash simulation:
  - Command: `node scripts/stressTest.js --agents=11 --tier=2 --simulate-crash --timers`
  - Summary: `integrity_ok=true`, `restart_integrity_ok=true`, `lock_timeouts=0`
  - Latency: p95 43 ms, p99 47 ms, max 52 ms
  - Duplicates skipped: 17
- Immediate resume:
  - Command: `node scripts/stressTest.js --agents=11 --tier=2 --timers`
  - Summary: `integrity_ok=true`, `lock_timeouts=0`
  - Latency: p95 35 ms, p99 38 ms, max 49 ms
  - Duplicates skipped: 35
- Interpretation:
  - Crash path preserved durability/integrity.
  - Lower duplicate count during crash-sim run is expected due aborted transactions before commit.
  - No duplicate-mutation drift indicators were observed (integrity remained true).

## Slope Analysis (Tier 2, aggregate means)

Using phase-2 + phase-3 aggregates (agents: 1, 3, 5, 7, 9, 11):

- 1->3: avg +0.14%, p95 -2.22%, p99 -6.13%, max +17.11%
- 3->5: avg -0.21%, p95 +1.14%, p99 +4.02%, max -6.82%
- 5->7: avg -1.03%, p95 +1.12%, p99 +2.42%, max +65.51%
- 7->9: avg +0.52%, p95 -1.86%, p99 -1.72%, max -43.16%
- 9->11: avg +0.55%, p95 +1.90%, p99 -3.22%, max +3.09%

Warning criteria review:
- No p95 step exceeded +30%.
- Slow-tx-rate step deltas stayed below material threshold.

Critical criteria review:
- `lock_timeouts=0` across validation runs.
- `integrity_ok=true` across validation runs.
- p99 never approached 500 ms (observed p99 range: ~35-57 ms).
- No sustained `slow_tx_rate > 0.10`.

## Dominant Phase Timers at Ceiling (11 agents, tier 2 soak aggregate)

From `scale-results-phase4-soak.csv` agg row:
- `lock_wait_p95=4.0 ms`, `lock_wait_p99=5.5 ms` (largest contributor)
- `write_p95=3.0 ms`, `write_p99=3.8 ms`
- `rename_p95=2.2 ms`, `rename_p99=3.0 ms`
- `clone_p95=1.1 ms`, `clone_p99=2.0 ms`
- `stringify_p95=1.0 ms`, `stringify_p99=1.0 ms`

Primary bottleneck at tested ceiling is lock-wait + file IO (write/rename), not clone/stringify.

## Recommended Operational Ceiling and Guardrails

### Recommended ceiling (for this host and this command mix)
- Max tested clean: **11 agents @ tier 2**
- Recommended operating ceiling: **11 agents @ tier 2**

### Guardrails
- Integrity: `integrity_ok` must remain `true`.
- Locking: `lock_timeouts` must remain `0`.
- Tail latency: `p99_tx` must remain `< 500 ms`.
- Slow tx: `slow_tx_rate` must not be sustained `> 0.10`.
- Persistence size: `memory_bytes` should remain `< 130000` for this tier-2 profile.

### Operational response runbook

#### WARN triggers (investigate)
- `p95_tx` step increase is `> +30%` versus last baseline agg, OR
- `slow_tx_rate` is sustained `> 0.10` for 3 consecutive runs, OR
- `memory_bytes` grows above `130000` for 3 consecutive runs (or shows persistent upward trend).

#### WARN actions
- Preserve current artifacts for the session: CSV + logs.
- Re-run baseline validation to confirm regression:
  - `node scripts/scaleValidation.js --fresh-csv --repeats=5 --timers --agent-series=1,3,5 --tier=2`
- Inspect timer contributors in agg rows (`lock_wait` vs `write`/`rename` vs `clone`/`stringify`).
- Reduce operational agents by 2 and revalidate until WARN clears.

#### CRITICAL triggers (stop / rollback load)
- `lock_timeouts > 0`, OR
- `integrity_ok` is `false`, OR
- `p99_tx >= 500 ms`, OR
- `restart_integrity_ok` is `false` (when crash testing).

#### CRITICAL actions
- Immediately stop the run / halt automation at current scale.
- Drop to last known CLEAN configuration (documented ceiling or lower).
- Preserve artifacts (CSV + logs).
- If reproducible, open an incident note in a `docs/scaling-ceiling.md` appendix with `session_id` and findings.

### Validity note
- Applies to this host + tier-2 command mix.
- Revalidate if command mix changes materially.
- Revalidate if storage changes or `memory_bytes` growth behavior changes.

## Output Marker Verification
- Orchestrator logs include:
  - `SCALE_VALIDATION_COMPLETE`
  - `OVERALL_STATUS: CLEAN | WARNING | CRITICAL`
