const fs = require('fs')
const os = require('os')
const path = require('path')
const test = require('node:test')
const assert = require('node:assert/strict')

const {
  CSV_COLUMNS,
  toTierNumber,
  parseStressSummary,
  prepareCsvFile
} = require('../scripts/scaleValidation')

function createTempCsvPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-scale-csv-'))
  return path.join(dir, 'scale-results.csv')
}

function buildSummary(overrides = {}) {
  const base = {
    AGENTS: '3',
    TIER: 'Tier 2',
    TOTAL_COMMANDS: '120',
    PEAK_HEAP_MB: '11.25',
    MEMORY_JSON_BYTES: '100321',
    AVG_TX_MS: '28.32',
    MAX_TX_MS: '55.00',
    P50_TX_MS: '27.00',
    P95_TX_MS: '41.00',
    P99_TX_MS: '48.00',
    SLOW_TX_COUNT: '2',
    LOCK_RETRIES: '0',
    LOCK_TIMEOUTS: '0',
    DUPLICATES_SKIPPED: '16',
    OPENAI_TIMEOUTS: '0',
    INTEGRITY_OK: 'true',
    LOCK_WAIT_P95_MS: '2.00',
    LOCK_WAIT_P99_MS: '3.00',
    CLONE_P95_MS: '0.50',
    CLONE_P99_MS: '0.70',
    STRINGIFY_P95_MS: '1.20',
    STRINGIFY_P99_MS: '1.50',
    WRITE_P95_MS: '0.80',
    WRITE_P99_MS: '1.00',
    RENAME_P95_MS: '0.30',
    RENAME_P99_MS: '0.40',
    TOTAL_TX_P95_MS: '41.00',
    TOTAL_TX_P99_MS: '48.00'
  }
  const fields = { ...base, ...overrides }
  return Object.entries(fields).map(([k, v]) => `${k}: ${v}`).join('\n')
}

test('tier normalization parses both label and numeric forms', () => {
  assert.equal(toTierNumber('Tier 2'), 2)
  assert.equal(toTierNumber('2'), 2)
  assert.equal(toTierNumber('tier=3'), 3)
})

test('prepareCsvFile truncates and rewrites header when freshCsv=true', async () => {
  const csvPath = createTempCsvPath()
  await fs.promises.writeFile(csvPath, 'old,data\n1,2\n', 'utf-8')

  await prepareCsvFile(csvPath, true)
  const content = await fs.promises.readFile(csvPath, 'utf-8')
  assert.equal(content, `${CSV_COLUMNS.join(',')}\n`)
})

test('prepareCsvFile writes header only for empty/non-existent file when freshCsv=false', async () => {
  const csvPath = createTempCsvPath()
  await prepareCsvFile(csvPath, false)
  await prepareCsvFile(csvPath, false)
  const content = await fs.promises.readFile(csvPath, 'utf-8')
  assert.equal(content, `${CSV_COLUMNS.join(',')}\n`)
})

test('parseStressSummary coerces all numeric fields to numbers', () => {
  const parsed = parseStressSummary(buildSummary())
  const numericValues = [
    parsed.agents,
    parsed.tier,
    parsed.totalCommands,
    parsed.peakHeapMb,
    parsed.memoryJsonBytes,
    parsed.avgTxMs,
    parsed.maxTxMs,
    parsed.p50TxMs,
    parsed.p95TxMs,
    parsed.p99TxMs,
    parsed.slowTxCount,
    parsed.lockRetries,
    parsed.lockTimeouts,
    parsed.duplicatesSkipped,
    parsed.openAiTimeouts,
    parsed.timers.lockWaitP95Ms,
    parsed.timers.lockWaitP99Ms,
    parsed.timers.cloneP95Ms,
    parsed.timers.cloneP99Ms,
    parsed.timers.stringifyP95Ms,
    parsed.timers.stringifyP99Ms,
    parsed.timers.writeP95Ms,
    parsed.timers.writeP99Ms,
    parsed.timers.renameP95Ms,
    parsed.timers.renameP99Ms,
    parsed.timers.totalTxP95Ms,
    parsed.timers.totalTxP99Ms
  ]

  for (const value of numericValues) {
    assert.equal(typeof value, 'number')
    assert.ok(Number.isFinite(value))
  }
  assert.equal(parsed.integrityOk, true)
})
