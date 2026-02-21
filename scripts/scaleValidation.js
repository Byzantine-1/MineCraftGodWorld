const fs = require('fs')
const path = require('path')
const { spawn } = require('child_process')

const projectRoot = path.resolve(__dirname, '..')
const stressScript = path.resolve(__dirname, 'stressTest.js')
const csvPath = path.resolve(projectRoot, 'scale-results.csv')
const P99_CRITICAL_CEILING_MS = 500
const RECOMMENDED_CEILING = '11 agents @ tier 2'
const DOCUMENTED_GUARDRAILS = 'lock_timeouts=0, integrity_ok=true, p99_tx<500ms, slow_tx_rate<=0.10, memory_bytes<130000'

const DEFAULT_AGENT_SERIES = [1, 3, 5]
const OPTIONAL_SCENARIO = { agents: 7, tier: 3 }

const REQUIRED_KEYS = [
  'AGENTS',
  'TIER',
  'TOTAL_COMMANDS',
  'PEAK_HEAP_MB',
  'MEMORY_JSON_BYTES',
  'AVG_TX_MS',
  'MAX_TX_MS',
  'P50_TX_MS',
  'P95_TX_MS',
  'P99_TX_MS',
  'SLOW_TX_COUNT',
  'LOCK_RETRIES',
  'LOCK_TIMEOUTS',
  'DUPLICATES_SKIPPED',
  'OPENAI_TIMEOUTS',
  'INTEGRITY_OK'
]

const TIMER_KEYS = [
  'LOCK_WAIT_P95_MS',
  'LOCK_WAIT_P99_MS',
  'CLONE_P95_MS',
  'CLONE_P99_MS',
  'STRINGIFY_P95_MS',
  'STRINGIFY_P99_MS',
  'WRITE_P95_MS',
  'WRITE_P99_MS',
  'RENAME_P95_MS',
  'RENAME_P99_MS',
  'TOTAL_TX_P95_MS',
  'TOTAL_TX_P99_MS'
]

const CSV_COLUMNS = [
  'session_id',
  'run_type',
  'repeat',
  'agents',
  'tier',
  'total_commands',
  'avg_tx',
  'avg_tx_std',
  'max_tx',
  'max_tx_std',
  'p50_tx',
  'p95_tx',
  'p99_tx',
  'slow_tx',
  'slow_tx_std',
  'slow_tx_rate',
  'heap_mb',
  'heap_mb_std',
  'memory_bytes',
  'memory_bytes_std',
  'lock_retries',
  'lock_timeouts',
  'duplicates_skipped',
  'openai_timeouts',
  'integrity_ok',
  'lock_wait_p95',
  'lock_wait_p99',
  'clone_p95',
  'clone_p99',
  'stringify_p95',
  'stringify_p99',
  'write_p95',
  'write_p99',
  'rename_p95',
  'rename_p99',
  'total_tx_p95',
  'total_tx_p99'
]

/**
 * @param {string[]} argv
 */
function parseScaleArgs(argv) {
  const parsed = {
    freshCsv: false,
    repeats: 3,
    timers: false,
    tier: 2,
    agentSeries: [...DEFAULT_AGENT_SERIES],
    optionalTier3: false
  }
  for (const raw of argv) {
    if (raw === '--fresh-csv') {
      parsed.freshCsv = true
      continue
    }
    if (raw === '--timers') {
      parsed.timers = true
      continue
    }
    if (raw === '--optional-tier3') {
      parsed.optionalTier3 = true
      continue
    }
    const [k, v] = raw.split('=')
    if (k === '--repeats') {
      const n = Number(v)
      if (Number.isInteger(n) && n > 0) parsed.repeats = n
      continue
    }
    if (k === '--tier') {
      const t = Number(v)
      if (Number.isInteger(t) && t >= 1 && t <= 3) parsed.tier = t
      continue
    }
    if (k === '--agent-series') {
      const values = String(v || '')
        .split(',')
        .map(x => Number(x.trim()))
        .filter(n => Number.isInteger(n) && n > 0)
      if (values.length > 0) parsed.agentSeries = values
    }
  }
  return parsed
}

/**
 * @param {string} value
 */
function toNumber(value) {
  const n = Number(value)
  if (!Number.isFinite(n)) {
    throw new Error(`Unable to parse numeric value: ${value}`)
  }
  return n
}

/**
 * @param {string} value
 */
function toTierNumber(value) {
  const match = /(\d+)/.exec(String(value))
  if (!match) {
    throw new Error(`Unable to parse tier value: ${value}`)
  }
  return toNumber(match[1])
}

/**
 * @param {string} value
 */
function toBoolean(value) {
  const normalized = String(value).trim().toLowerCase()
  if (normalized === 'true' || normalized === '1') return true
  if (normalized === 'false' || normalized === '0') return false
  throw new Error(`Unable to parse boolean value: ${value}`)
}

/**
 * @param {string} output
 */
function parseStressSummary(output) {
  const fields = {}
  const lines = String(output || '').split(/\r?\n/)
  for (const line of lines) {
    const match = /^([A-Z0-9_]+):\s*(.+)$/.exec(line.trim())
    if (!match) continue
    fields[match[1]] = match[2]
  }

  const missing = REQUIRED_KEYS.filter(key => !(key in fields))
  if (missing.length) {
    throw new Error(`Missing stress summary fields: ${missing.join(', ')}`)
  }

  const timers = {}
  for (const key of TIMER_KEYS) {
    timers[key] = key in fields ? toNumber(fields[key]) : 0
  }

  return {
    agents: toNumber(fields.AGENTS),
    tier: toTierNumber(fields.TIER),
    totalCommands: toNumber(fields.TOTAL_COMMANDS),
    peakHeapMb: toNumber(fields.PEAK_HEAP_MB),
    memoryJsonBytes: toNumber(fields.MEMORY_JSON_BYTES),
    avgTxMs: toNumber(fields.AVG_TX_MS),
    maxTxMs: toNumber(fields.MAX_TX_MS),
    p50TxMs: toNumber(fields.P50_TX_MS),
    p95TxMs: toNumber(fields.P95_TX_MS),
    p99TxMs: toNumber(fields.P99_TX_MS),
    slowTxCount: toNumber(fields.SLOW_TX_COUNT),
    lockRetries: toNumber(fields.LOCK_RETRIES),
    lockTimeouts: toNumber(fields.LOCK_TIMEOUTS),
    duplicatesSkipped: toNumber(fields.DUPLICATES_SKIPPED),
    openAiTimeouts: toNumber(fields.OPENAI_TIMEOUTS),
    integrityOk: toBoolean(fields.INTEGRITY_OK),
    timers: {
      lockWaitP95Ms: timers.LOCK_WAIT_P95_MS,
      lockWaitP99Ms: timers.LOCK_WAIT_P99_MS,
      cloneP95Ms: timers.CLONE_P95_MS,
      cloneP99Ms: timers.CLONE_P99_MS,
      stringifyP95Ms: timers.STRINGIFY_P95_MS,
      stringifyP99Ms: timers.STRINGIFY_P99_MS,
      writeP95Ms: timers.WRITE_P95_MS,
      writeP99Ms: timers.WRITE_P99_MS,
      renameP95Ms: timers.RENAME_P95_MS,
      renameP99Ms: timers.RENAME_P99_MS,
      totalTxP95Ms: timers.TOTAL_TX_P95_MS,
      totalTxP99Ms: timers.TOTAL_TX_P99_MS
    }
  }
}

/**
 * @param {number} agents
 * @param {number} tier
 * @param {{timers: boolean}} options
 */
function runStress(agents, tier, options) {
  return new Promise((resolve, reject) => {
    const args = [stressScript, `--agents=${agents}`, `--tier=${tier}`]
    if (options.timers) args.push('--timers')

    const child = spawn(process.execPath, args, {
      cwd: projectRoot,
      stdio: ['ignore', 'pipe', 'pipe']
    })

    let stdout = ''
    let stderr = ''
    child.stdout.on('data', (chunk) => {
      const text = String(chunk)
      stdout += text
      process.stdout.write(text)
    })
    child.stderr.on('data', (chunk) => {
      const text = String(chunk)
      stderr += text
      process.stderr.write(text)
    })
    child.on('error', reject)
    child.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`stressTest exited with code ${code}\n${stderr}`))
        return
      }
      try {
        resolve(parseStressSummary(`${stdout}\n${stderr}`))
      } catch (err) {
        reject(err)
      }
    })
  })
}

/**
 * @param {string} text
 */
function csvEscape(text) {
  const value = String(text)
  if (!/[,"\n]/.test(value)) return value
  return `"${value.replace(/"/g, '""')}"`
}

/**
 * @param {string} filePath
 * @param {boolean} freshCsv
 * @param {typeof fs.promises} [fsPromises]
 */
async function prepareCsvFile(filePath, freshCsv, fsPromises = fs.promises) {
  const header = `${CSV_COLUMNS.join(',')}\n`
  if (freshCsv) {
    await fsPromises.writeFile(filePath, header, 'utf-8')
    return
  }

  let hasData = false
  try {
    const stat = await fsPromises.stat(filePath)
    hasData = Number(stat.size || 0) > 0
  } catch (err) {
    if (!err || typeof err !== 'object' || err.code !== 'ENOENT') throw err
  }
  if (!hasData) await fsPromises.appendFile(filePath, header, 'utf-8')
}

/**
 * @param {number[]} values
 */
function meanStd(values) {
  if (!values.length) return { mean: 0, stddev: 0 }
  const mean = values.reduce((sum, v) => sum + v, 0) / values.length
  const variance = values.reduce((sum, v) => sum + ((v - mean) ** 2), 0) / values.length
  return { mean, stddev: Math.sqrt(variance) }
}

/**
 * @param {Array<ReturnType<typeof parseStressSummary>>} repeats
 */
function aggregateRepeats(repeats) {
  const first = repeats[0]
  const slowRates = repeats.map(r => (r.totalCommands > 0 ? r.slowTxCount / r.totalCommands : 0))
  const avgTx = meanStd(repeats.map(r => r.avgTxMs))
  const maxTx = meanStd(repeats.map(r => r.maxTxMs))
  const slowTx = meanStd(repeats.map(r => r.slowTxCount))
  const heap = meanStd(repeats.map(r => r.peakHeapMb))
  const memory = meanStd(repeats.map(r => r.memoryJsonBytes))
  const p50 = meanStd(repeats.map(r => r.p50TxMs))
  const p95 = meanStd(repeats.map(r => r.p95TxMs))
  const p99 = meanStd(repeats.map(r => r.p99TxMs))
  const lockRetries = meanStd(repeats.map(r => r.lockRetries))
  const lockTimeouts = Math.max(...repeats.map(r => r.lockTimeouts))
  const duplicates = meanStd(repeats.map(r => r.duplicatesSkipped))
  const openAiTimeouts = meanStd(repeats.map(r => r.openAiTimeouts))
  const slowTxRate = meanStd(slowRates)

  const lockWaitP95 = meanStd(repeats.map(r => r.timers.lockWaitP95Ms))
  const lockWaitP99 = meanStd(repeats.map(r => r.timers.lockWaitP99Ms))
  const cloneP95 = meanStd(repeats.map(r => r.timers.cloneP95Ms))
  const cloneP99 = meanStd(repeats.map(r => r.timers.cloneP99Ms))
  const stringifyP95 = meanStd(repeats.map(r => r.timers.stringifyP95Ms))
  const stringifyP99 = meanStd(repeats.map(r => r.timers.stringifyP99Ms))
  const writeP95 = meanStd(repeats.map(r => r.timers.writeP95Ms))
  const writeP99 = meanStd(repeats.map(r => r.timers.writeP99Ms))
  const renameP95 = meanStd(repeats.map(r => r.timers.renameP95Ms))
  const renameP99 = meanStd(repeats.map(r => r.timers.renameP99Ms))
  const totalTxP95 = meanStd(repeats.map(r => r.timers.totalTxP95Ms))
  const totalTxP99 = meanStd(repeats.map(r => r.timers.totalTxP99Ms))

  return {
    agents: first.agents,
    tier: first.tier,
    totalCommands: first.totalCommands,
    avgTx,
    maxTx,
    p50,
    p95,
    p99,
    slowTx,
    slowTxRate,
    heap,
    memory,
    lockRetries,
    lockTimeouts,
    duplicates,
    openAiTimeouts,
    integrityOk: repeats.every(r => r.integrityOk),
    timers: {
      lockWaitP95,
      lockWaitP99,
      cloneP95,
      cloneP99,
      stringifyP95,
      stringifyP99,
      writeP95,
      writeP99,
      renameP95,
      renameP99,
      totalTxP95,
      totalTxP99
    }
  }
}

/**
 * @param {{
 *   sessionId: string,
 *   runType: 'repeat' | 'agg',
 *   repeat: number,
 *   agents: number,
 *   tier: number,
 *   totalCommands: number,
 *   avgTx: number,
 *   avgTxStd: number,
 *   maxTx: number,
 *   maxTxStd: number,
 *   p50Tx: number,
 *   p95Tx: number,
 *   p99Tx: number,
 *   slowTx: number,
 *   slowTxStd: number,
 *   slowTxRate: number,
 *   heapMb: number,
 *   heapMbStd: number,
 *   memoryBytes: number,
 *   memoryBytesStd: number,
 *   lockRetries: number,
 *   lockTimeouts: number,
 *   duplicatesSkipped: number,
 *   openAiTimeouts: number,
 *   integrityOk: boolean,
 *   timers: {
 *     lockWaitP95: number,
 *     lockWaitP99: number,
 *     cloneP95: number,
 *     cloneP99: number,
 *     stringifyP95: number,
 *     stringifyP99: number,
 *     writeP95: number,
 *     writeP99: number,
 *     renameP95: number,
 *     renameP99: number,
 *     totalTxP95: number,
 *     totalTxP99: number
 *   }
 * }} row
 */
function toCsvLine(row) {
  const values = {
    session_id: row.sessionId,
    run_type: row.runType,
    repeat: row.repeat,
    agents: row.agents,
    tier: row.tier,
    total_commands: row.totalCommands,
    avg_tx: row.avgTx,
    avg_tx_std: row.avgTxStd,
    max_tx: row.maxTx,
    max_tx_std: row.maxTxStd,
    p50_tx: row.p50Tx,
    p95_tx: row.p95Tx,
    p99_tx: row.p99Tx,
    slow_tx: row.slowTx,
    slow_tx_std: row.slowTxStd,
    slow_tx_rate: row.slowTxRate,
    heap_mb: row.heapMb,
    heap_mb_std: row.heapMbStd,
    memory_bytes: row.memoryBytes,
    memory_bytes_std: row.memoryBytesStd,
    lock_retries: row.lockRetries,
    lock_timeouts: row.lockTimeouts,
    duplicates_skipped: row.duplicatesSkipped,
    openai_timeouts: row.openAiTimeouts,
    integrity_ok: row.integrityOk,
    lock_wait_p95: row.timers.lockWaitP95,
    lock_wait_p99: row.timers.lockWaitP99,
    clone_p95: row.timers.cloneP95,
    clone_p99: row.timers.cloneP99,
    stringify_p95: row.timers.stringifyP95,
    stringify_p99: row.timers.stringifyP99,
    write_p95: row.timers.writeP95,
    write_p99: row.timers.writeP99,
    rename_p95: row.timers.renameP95,
    rename_p99: row.timers.renameP99,
    total_tx_p95: row.timers.totalTxP95,
    total_tx_p99: row.timers.totalTxP99
  }
  return CSV_COLUMNS.map(col => csvEscape(values[col])).join(',')
}

/**
 * @param {ReturnType<typeof parseStressSummary>} parsed
 * @param {string} sessionId
 * @param {number} repeat
 */
function buildRepeatRow(parsed, sessionId, repeat) {
  return {
    sessionId,
    runType: 'repeat',
    repeat,
    agents: parsed.agents,
    tier: parsed.tier,
    totalCommands: parsed.totalCommands,
    avgTx: parsed.avgTxMs,
    avgTxStd: 0,
    maxTx: parsed.maxTxMs,
    maxTxStd: 0,
    p50Tx: parsed.p50TxMs,
    p95Tx: parsed.p95TxMs,
    p99Tx: parsed.p99TxMs,
    slowTx: parsed.slowTxCount,
    slowTxStd: 0,
    slowTxRate: parsed.totalCommands > 0 ? parsed.slowTxCount / parsed.totalCommands : 0,
    heapMb: parsed.peakHeapMb,
    heapMbStd: 0,
    memoryBytes: parsed.memoryJsonBytes,
    memoryBytesStd: 0,
    lockRetries: parsed.lockRetries,
    lockTimeouts: parsed.lockTimeouts,
    duplicatesSkipped: parsed.duplicatesSkipped,
    openAiTimeouts: parsed.openAiTimeouts,
    integrityOk: parsed.integrityOk,
    timers: {
      lockWaitP95: parsed.timers.lockWaitP95Ms,
      lockWaitP99: parsed.timers.lockWaitP99Ms,
      cloneP95: parsed.timers.cloneP95Ms,
      cloneP99: parsed.timers.cloneP99Ms,
      stringifyP95: parsed.timers.stringifyP95Ms,
      stringifyP99: parsed.timers.stringifyP99Ms,
      writeP95: parsed.timers.writeP95Ms,
      writeP99: parsed.timers.writeP99Ms,
      renameP95: parsed.timers.renameP95Ms,
      renameP99: parsed.timers.renameP99Ms,
      totalTxP95: parsed.timers.totalTxP95Ms,
      totalTxP99: parsed.timers.totalTxP99Ms
    }
  }
}

/**
 * @param {ReturnType<typeof aggregateRepeats>} agg
 * @param {string} sessionId
 */
function buildAggregateRow(agg, sessionId) {
  return {
    sessionId,
    runType: 'agg',
    repeat: 0,
    agents: agg.agents,
    tier: agg.tier,
    totalCommands: agg.totalCommands,
    avgTx: agg.avgTx.mean,
    avgTxStd: agg.avgTx.stddev,
    maxTx: agg.maxTx.mean,
    maxTxStd: agg.maxTx.stddev,
    p50Tx: agg.p50.mean,
    p95Tx: agg.p95.mean,
    p99Tx: agg.p99.mean,
    slowTx: agg.slowTx.mean,
    slowTxStd: agg.slowTx.stddev,
    slowTxRate: agg.slowTxRate.mean,
    heapMb: agg.heap.mean,
    heapMbStd: agg.heap.stddev,
    memoryBytes: agg.memory.mean,
    memoryBytesStd: agg.memory.stddev,
    lockRetries: agg.lockRetries.mean,
    lockTimeouts: agg.lockTimeouts,
    duplicatesSkipped: agg.duplicates.mean,
    openAiTimeouts: agg.openAiTimeouts.mean,
    integrityOk: agg.integrityOk,
    timers: {
      lockWaitP95: agg.timers.lockWaitP95.mean,
      lockWaitP99: agg.timers.lockWaitP99.mean,
      cloneP95: agg.timers.cloneP95.mean,
      cloneP99: agg.timers.cloneP99.mean,
      stringifyP95: agg.timers.stringifyP95.mean,
      stringifyP99: agg.timers.stringifyP99.mean,
      writeP95: agg.timers.writeP95.mean,
      writeP99: agg.timers.writeP99.mean,
      renameP95: agg.timers.renameP95.mean,
      renameP99: agg.timers.renameP99.mean,
      totalTxP95: agg.timers.totalTxP95.mean,
      totalTxP99: agg.timers.totalTxP99.mean
    }
  }
}

/**
 * @param {Array<ReturnType<typeof aggregateRepeats>>} aggregates
 */
function computeGrowth(aggregates) {
  const steps = []
  for (let i = 1; i < aggregates.length; i += 1) {
    const prev = aggregates[i - 1]
    const curr = aggregates[i]
    steps.push({
      from: prev.agents,
      to: curr.agents,
      p95Ratio: prev.p95.mean === 0 ? 0 : curr.p95.mean / prev.p95.mean,
      p99Ratio: prev.p99.mean === 0 ? 0 : curr.p99.mean / prev.p99.mean,
      maxTxRatio: prev.maxTx.mean === 0 ? 0 : curr.maxTx.mean / prev.maxTx.mean,
      slowRateDelta: curr.slowTxRate.mean - prev.slowTxRate.mean
    })
  }
  return steps
}

/**
 * @param {ReturnType<typeof aggregateRepeats>[]} aggregates
 */
function evaluateStatus(aggregates) {
  const warnings = []
  const critical = []

  for (const agg of aggregates) {
    if (agg.lockTimeouts > 0) critical.push(`CRITICAL: Lock timeouts detected at agents=${agg.agents}, tier=${agg.tier}.`)
    if (!agg.integrityOk) critical.push(`CRITICAL: Integrity failure at agents=${agg.agents}, tier=${agg.tier}.`)
    if (agg.p99.mean > P99_CRITICAL_CEILING_MS) {
      critical.push(`CRITICAL: p99 transaction latency exceeded ${P99_CRITICAL_CEILING_MS}ms at agents=${agg.agents}, tier=${agg.tier}.`)
    }
  }

  const ordered = [...aggregates].sort((a, b) => a.agents - b.agents)
  for (let i = 1; i < ordered.length; i += 1) {
    const prev = ordered[i - 1]
    const curr = ordered[i]
    const p95Growth = prev.p95.mean === 0 ? 0 : (curr.p95.mean - prev.p95.mean) / prev.p95.mean
    if (p95Growth > 0.3) {
      warnings.push(
        `WARNING: p95_tx grew ${(p95Growth * 100).toFixed(1)}% from ${prev.agents}->${curr.agents} agents.`
      )
    }

    // "Material" slow-rate increase: >2 percentage points and >50% relative growth.
    const slowRateDelta = curr.slowTxRate.mean - prev.slowTxRate.mean
    if (slowRateDelta > 0.02 && curr.slowTxRate.mean > (prev.slowTxRate.mean * 1.5)) {
      warnings.push(
        `WARNING: slow_tx_rate materially increased from ${prev.agents}->${curr.agents} agents.`
      )
    }
  }

  const overallStatus = critical.length > 0
    ? 'CRITICAL'
    : (warnings.length > 0 ? 'WARNING' : 'CLEAN')

  return { warnings, critical, overallStatus }
}

/**
 * @param {{
 *  scenarios: Array<{agents: number, tier: number}>,
 *  repeats: number,
 *  timers: boolean,
 *  sessionId: string
 * }} options
 * @param {(row: ReturnType<typeof buildRepeatRow> | ReturnType<typeof buildAggregateRow>) => Promise<void>} appendCsv
 */
async function executeScenarios(options, appendCsv) {
  /** @type {ReturnType<typeof aggregateRepeats>[]} */
  const aggregates = []
  for (const scenario of options.scenarios) {
    /** @type {Array<ReturnType<typeof parseStressSummary>>} */
    const repeats = []
    for (let repeat = 1; repeat <= options.repeats; repeat += 1) {
      console.log(`\n=== Running --agents=${scenario.agents} --tier=${scenario.tier} repeat ${repeat}/${options.repeats} ===`)
      const parsed = await runStress(scenario.agents, scenario.tier, { timers: options.timers })
      repeats.push(parsed)
      await appendCsv(buildRepeatRow(parsed, options.sessionId, repeat))
    }
    const agg = aggregateRepeats(repeats)
    aggregates.push(agg)
    await appendCsv(buildAggregateRow(agg, options.sessionId))
  }
  return aggregates
}

async function main() {
  const startedAt = Date.now()
  const args = parseScaleArgs(process.argv.slice(2))
  const sessionId = new Date().toISOString()
  const scenarios = args.agentSeries.map(agents => ({ agents, tier: args.tier }))

  await prepareCsvFile(csvPath, args.freshCsv)

  const appendCsv = async (row) => {
    await fs.promises.appendFile(csvPath, `${toCsvLine(row)}\n`, 'utf-8')
  }

  const baseAggregates = await executeScenarios({
    scenarios,
    repeats: args.repeats,
    timers: args.timers,
    sessionId
  }, appendCsv)

  let status = evaluateStatus(baseAggregates)
  const allAggregates = [...baseAggregates]

  if (args.optionalTier3 && status.overallStatus === 'CLEAN') {
    console.log(`\n=== Running optional --agents=${OPTIONAL_SCENARIO.agents} --tier=${OPTIONAL_SCENARIO.tier} ===`)
    const optional = await executeScenarios({
      scenarios: [OPTIONAL_SCENARIO],
      repeats: args.repeats,
      timers: args.timers,
      sessionId
    }, appendCsv)
    allAggregates.push(...optional)
    status = evaluateStatus(allAggregates)
  } else if (args.optionalTier3) {
    console.log('\nSkipping optional --agents=7 --tier=3 because initial aggregated status is not CLEAN.')
  }

  console.log('\nAgents | Tier | AvgTx (mean±std) | MaxTx (mean±std) | P95Tx | P99Tx | SlowTx (mean±std) | PeakHeap (mean±std) | MemoryBytes (mean±std)')
  for (const agg of allAggregates) {
    console.log(
      `${agg.agents} | ${agg.tier} | ${agg.avgTx.mean.toFixed(2)} ± ${agg.avgTx.stddev.toFixed(2)} | ${agg.maxTx.mean.toFixed(2)} ± ${agg.maxTx.stddev.toFixed(2)} | ${agg.p95.mean.toFixed(2)} | ${agg.p99.mean.toFixed(2)} | ${agg.slowTx.mean.toFixed(2)} ± ${agg.slowTx.stddev.toFixed(2)} | ${agg.heap.mean.toFixed(2)} ± ${agg.heap.stddev.toFixed(2)} | ${agg.memory.mean.toFixed(2)} ± ${agg.memory.stddev.toFixed(2)}`
    )
  }

  console.log('\nGrowth / Delta (aggregate means)')
  const growth = computeGrowth(allAggregates)
  for (const step of growth) {
    console.log(
      `${step.from}->${step.to} agents: p95 ${step.p95Ratio.toFixed(2)}x, p99 ${step.p99Ratio.toFixed(2)}x, maxTx ${step.maxTxRatio.toFixed(2)}x, slowRate ${(step.slowRateDelta * 100).toFixed(2)}pp`
    )
  }

  for (const warning of status.warnings) console.log(warning)
  for (const critical of status.critical) console.log(critical)

  const elapsedMs = Date.now() - startedAt
  console.log(`\nSESSION_ID: ${sessionId}`)
  console.log(`Results written to ${csvPath}`)
  console.log(`Total orchestrator runtime (ms): ${elapsedMs}`)
  console.log(`RECOMMENDED_CEILING: "${RECOMMENDED_CEILING}"`)
  console.log(`GUARDRAILS: ${DOCUMENTED_GUARDRAILS}`)
  console.log('SCALE_VALIDATION_COMPLETE')
  console.log(`OVERALL_STATUS: ${status.overallStatus}`)
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err instanceof Error ? err.stack || err.message : String(err))
    process.exitCode = 1
  })
}

module.exports = {
  CSV_COLUMNS,
  parseScaleArgs,
  toNumber,
  toTierNumber,
  parseStressSummary,
  prepareCsvFile,
  toCsvLine
}
