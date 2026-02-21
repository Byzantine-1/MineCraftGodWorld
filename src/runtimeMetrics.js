const METRIC_KEYS = [
  'eventsProcessed',
  'duplicateEventsSkipped',
  'lockRetries',
  'lockTimeouts',
  'transactionsCommitted',
  'transactionsAborted',
  'openAiRequests',
  'openAiTimeouts'
]

const runtimeMetrics = {
  eventsProcessed: 0,
  duplicateEventsSkipped: 0,
  lockRetries: 0,
  lockTimeouts: 0,
  transactionsCommitted: 0,
  transactionsAborted: 0,
  openAiRequests: 0,
  openAiTimeouts: 0
}

const observability = {
  txDurationTotalMs: 0,
  txDurationCount: 0,
  txDurationMaxMs: 0,
  slowTransactionCount: 0,
  lockAcquisitionTotalMs: 0,
  lockAcquisitionCount: 0,
  txDurationSamples: [],
  txPhaseSamples: {
    lockWaitMs: [],
    cloneMs: [],
    stringifyMs: [],
    writeMs: [],
    renameMs: [],
    totalTxMs: []
  }
}

let reporterTimer = null
const MAX_SAMPLE_WINDOW = 10000

/**
 * @param {number[]} samples
 * @param {number} value
 */
function pushSample(samples, value) {
  samples.push(Number(value || 0))
  if (samples.length > MAX_SAMPLE_WINDOW) samples.shift()
}

/**
 * @param {number[]} samples
 * @param {number} percentile
 */
function percentile(samples, percentile) {
  if (!Array.isArray(samples) || samples.length === 0) return 0
  const sorted = [...samples].sort((a, b) => a - b)
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil((percentile / 100) * sorted.length) - 1))
  return sorted[idx]
}

/**
 * @param {keyof typeof runtimeMetrics} key
 * @param {number} [amount]
 */
function incrementMetric(key, amount = 1) {
  if (!METRIC_KEYS.includes(key)) return
  runtimeMetrics[key] += Number(amount || 0)
}

function getRuntimeMetrics() {
  return { ...runtimeMetrics }
}

/**
 * @param {number} durationMs
 * @param {boolean | {isSlow?: boolean, phaseDurations?: {
 *   lockWaitMs?: number,
 *   cloneMs?: number,
 *   stringifyMs?: number,
 *   writeMs?: number,
 *   renameMs?: number,
 *   totalTxMs?: number
 * }}} [details]
 */
function recordTransactionDuration(durationMs, details = false) {
  const ms = Number(durationMs || 0)
  const isSlow = typeof details === 'boolean' ? details : !!details?.isSlow
  observability.txDurationTotalMs += ms
  observability.txDurationCount += 1
  observability.txDurationMaxMs = Math.max(observability.txDurationMaxMs, ms)
  pushSample(observability.txDurationSamples, ms)
  if (isSlow) observability.slowTransactionCount += 1

  const phaseDurations = (details && typeof details === 'object') ? details.phaseDurations : null
  if (!phaseDurations) return
  pushSample(observability.txPhaseSamples.lockWaitMs, Number(phaseDurations.lockWaitMs || 0))
  pushSample(observability.txPhaseSamples.cloneMs, Number(phaseDurations.cloneMs || 0))
  pushSample(observability.txPhaseSamples.stringifyMs, Number(phaseDurations.stringifyMs || 0))
  pushSample(observability.txPhaseSamples.writeMs, Number(phaseDurations.writeMs || 0))
  pushSample(observability.txPhaseSamples.renameMs, Number(phaseDurations.renameMs || 0))
  pushSample(observability.txPhaseSamples.totalTxMs, Number(phaseDurations.totalTxMs || 0))
}

/**
 * @param {number} durationMs
 */
function recordLockAcquisition(durationMs) {
  const ms = Number(durationMs || 0)
  observability.lockAcquisitionTotalMs += ms
  observability.lockAcquisitionCount += 1
}

function getObservabilitySnapshot() {
  return {
    txDurationTotalMs: observability.txDurationTotalMs,
    txDurationCount: observability.txDurationCount,
    txDurationMaxMs: observability.txDurationMaxMs,
    slowTransactionCount: observability.slowTransactionCount,
    lockAcquisitionTotalMs: observability.lockAcquisitionTotalMs,
    lockAcquisitionCount: observability.lockAcquisitionCount,
    txDurationP50Ms: percentile(observability.txDurationSamples, 50),
    txDurationP95Ms: percentile(observability.txDurationSamples, 95),
    txDurationP99Ms: percentile(observability.txDurationSamples, 99),
    txPhaseP95Ms: {
      lockWaitMs: percentile(observability.txPhaseSamples.lockWaitMs, 95),
      cloneMs: percentile(observability.txPhaseSamples.cloneMs, 95),
      stringifyMs: percentile(observability.txPhaseSamples.stringifyMs, 95),
      writeMs: percentile(observability.txPhaseSamples.writeMs, 95),
      renameMs: percentile(observability.txPhaseSamples.renameMs, 95),
      totalTxMs: percentile(observability.txPhaseSamples.totalTxMs, 95)
    },
    txPhaseP99Ms: {
      lockWaitMs: percentile(observability.txPhaseSamples.lockWaitMs, 99),
      cloneMs: percentile(observability.txPhaseSamples.cloneMs, 99),
      stringifyMs: percentile(observability.txPhaseSamples.stringifyMs, 99),
      writeMs: percentile(observability.txPhaseSamples.writeMs, 99),
      renameMs: percentile(observability.txPhaseSamples.renameMs, 99),
      totalTxMs: percentile(observability.txPhaseSamples.totalTxMs, 99)
    }
  }
}

/**
 * @param {ReturnType<import('./logger').createLogger> | null | undefined} logger
 * @param {number} [intervalMs]
 */
function startRuntimeMetricsReporter(logger, intervalMs = 60000) {
  if (reporterTimer) return
  reporterTimer = setInterval(() => {
    const snapshot = getRuntimeMetrics()
    if (logger && typeof logger.info === 'function') {
      logger.info('runtime_metrics', snapshot)
      return
    }
    process.stdout.write(`${JSON.stringify({ message: 'runtime_metrics', ...snapshot })}\n`)
  }, intervalMs)
  if (typeof reporterTimer.unref === 'function') reporterTimer.unref()
}

function stopRuntimeMetricsReporter() {
  if (!reporterTimer) return
  clearInterval(reporterTimer)
  reporterTimer = null
}

module.exports = {
  incrementMetric,
  getRuntimeMetrics,
  recordTransactionDuration,
  recordLockAcquisition,
  getObservabilitySnapshot,
  startRuntimeMetricsReporter,
  stopRuntimeMetricsReporter
}
