require('dotenv').config()

const fs = require('fs')
const os = require('os')
const path = require('path')

const Agent = require('../src/agent')
const { createMemoryStore } = require('../src/memory')
const { createDialogueService } = require('../src/dialogue')
const { createActionEngine } = require('../src/actionEngine')
const { createTurnEngine } = require('../src/turnEngine')
const { createGodCommandService } = require('../src/godCommands')
const { parseCliInput } = require('../src/commandParsers')
const { createLogger } = require('../src/logger')
const { createKeyedQueue, deriveOperationId } = require('../src/flowControl')
const { startRuntimeMetricsReporter, getObservabilitySnapshot } = require('../src/runtimeMetrics')

const startedAt = Date.now()
const logger = createLogger({ component: 'stress_test' })

/**
 * @param {number} ms
 */
function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

/**
 * @param {number} min
 * @param {number} max
 */
function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min
}

function parseArgs() {
  const args = process.argv.slice(2)
  const parsed = {
    agents: 2,
    tier: 1,
    simulateCrash: false,
    timers: false
  }
  for (const raw of args) {
    if (raw === '--simulate-crash') {
      parsed.simulateCrash = true
      continue
    }
    if (raw === '--timers') {
      parsed.timers = true
      continue
    }
    const [k, v] = raw.split('=')
    if (k === '--agents') {
      const n = Number(v)
      if (Number.isInteger(n) && n > 0) parsed.agents = n
      continue
    }
    if (k === '--tier') {
      const t = Number(v)
      if (Number.isInteger(t) && t >= 1 && t <= 3) parsed.tier = t
    }
  }
  return parsed
}

const options = parseArgs()
startRuntimeMetricsReporter(logger.child({ subsystem: 'metrics' }), 60000)

function createTempMemoryPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-stress-'))
  return path.join(dir, 'memory.json')
}

const filePath = createTempMemoryPath()
const memoryStore = createMemoryStore({
  filePath,
  logger: logger.child({ subsystem: 'memory' }),
  enableTxTimers: options.timers
})
const dialogueService = createDialogueService({
  memoryStore,
  logger: logger.child({ subsystem: 'dialogue' })
})
const actionEngine = createActionEngine({
  memoryStore,
  logger: logger.child({ subsystem: 'action_engine' })
})
const turnEngine = createTurnEngine({
  memoryStore,
  actionEngine,
  logger: logger.child({ subsystem: 'turn_engine' })
})
const godCommandService = createGodCommandService({
  memoryStore,
  logger: logger.child({ subsystem: 'god_commands' })
})
const runSerial = createKeyedQueue()

/** @type {Record<string, Agent>} */
const agents = {}
for (let i = 0; i < options.agents; i += 1) {
  const key = `a${String(i + 1).padStart(3, '0')}`
  const name = `Agent_${String(i + 1).padStart(3, '0')}`
  agents[key] = new Agent({ name, role: 'Scout', faction: `Faction_${(i % 5) + 1}` })
}
const agentKeys = Object.keys(agents)
const godCommands = ['declare_war', 'make_peace', 'bless_people']

memoryStore.loadAllMemory()

/**
 * Uses the same command parser/handlers as the CLI flow.
 * @param {string} rawInput
 */
async function executeCommand(rawInput) {
  const parsed = parseCliInput(rawInput)
  if (parsed.type === 'noop') return { type: 'noop' }
  if (parsed.type === 'error') return { type: 'error', reason: parsed.message }
  if (parsed.type === 'unknown') return { type: 'unknown' }
  if (parsed.type === 'exit') return { type: 'exit' }

  if (parsed.type === 'talk') {
    const agent = agents[parsed.target]
    if (!agent) return { type: 'error', reason: `unknown_agent:${parsed.target}` }
    if (memoryStore.recallWorld().player.alive === false) return { type: 'talk', skipped: true, reason: 'player_dead' }

    return runSerial(`agent:${agent.name}`, async () => {
      const operationId = deriveOperationId(['stress', 'talk', agent.name, parsed.message], { windowMs: 5000 })
      await turnEngine.recordIncoming({
        agent,
        playerName: null,
        message: parsed.message,
        operationId
      })
      const rawTurn = await dialogueService.generateDialogue(agent, parsed.message)
      const applied = await turnEngine.applyTurn({
        agent,
        rawTurn,
        fallbackTurn: dialogueService.fallbackTurn(agent),
        operationId
      })
      return { type: 'talk', skipped: applied.skipped, outcomes: applied.outcomes.length }
    })
  }

  if (parsed.type === 'god') {
    const operationId = deriveOperationId(['stress', 'god', parsed.command], { windowMs: 5000 })
    const result = await godCommandService.applyGodCommand({
      agents: Object.values(agents),
      command: parsed.command,
      operationId
    })
    if (!result.applied) return { type: 'god', applied: false, reason: result.reason }

    for (const agent of Object.values(agents)) {
      await memoryStore.rememberAgent(agent.name, `God issued command "${parsed.command}".`, true, `${operationId}:audit`)
      await memoryStore.rememberFaction(agent.faction, `God issued command "${parsed.command}".`, true, `${operationId}:audit`)
    }
    await memoryStore.rememberWorld(`God issued command "${parsed.command}".`, true, `${operationId}:audit`)
    return { type: 'god', applied: true }
  }

  return { type: 'unknown' }
}

function pickAgentKey() {
  return agentKeys[randInt(0, agentKeys.length - 1)]
}

function buildTierCommands(tier) {
  if (tier === 1) {
    const identicalTalk = Array.from({ length: 20 }, () => `talk ${pickAgentKey()} identical stress line`)
    const variedTalk = Array.from({ length: 20 }, (_, i) => `talk ${pickAgentKey()} varied stress line ${i}`)
    const rapidGod = Array.from({ length: 5 }, () => `god ${godCommands[randInt(0, godCommands.length - 1)]}`)
    return {
      tierLabel: 'Tier 1',
      delayMaxMs: 0,
      commands: [...identicalTalk, ...variedTalk, ...rapidGod]
    }
  }

  if (tier === 2) {
    const variedTalk = Array.from({ length: 100 }, (_, i) => `talk ${pickAgentKey()} tier2 varied line ${i}`)
    const gods = Array.from({ length: 20 }, () => `god ${godCommands[randInt(0, godCommands.length - 1)]}`)
    return {
      tierLabel: 'Tier 2',
      delayMaxMs: 50,
      commands: [...variedTalk, ...gods]
    }
  }

  const mixed = Array.from({ length: 250 }, (_, i) => {
    if (Math.random() < 0.35) return `talk ${pickAgentKey()} mixed repeated phrase`
    return `talk ${pickAgentKey()} tier3 mixed line ${i}-${randInt(0, 9999)}`
  })
  const gods = Array.from({ length: 50 }, () => `god ${godCommands[randInt(0, godCommands.length - 1)]}`)
  return {
    tierLabel: 'Tier 3',
    delayMaxMs: 10,
    commands: [...mixed, ...gods]
  }
}

/**
 * @param {string[]} commands
 * @param {number} delayMaxMs
 */
async function runCommands(commands, delayMaxMs) {
  const results = await Promise.all(commands.map(async (command) => {
    if (delayMaxMs > 0) await wait(randInt(0, delayMaxMs))
    try {
      return await executeCommand(command)
    } catch (err) {
      logger.warn('stress_command_failed', { command, error: err instanceof Error ? err.message : String(err) })
      return { type: 'error', reason: 'exception' }
    }
  }))
  return results
}

/**
 * @param {string} targetPath
 */
async function fileSizeBytes(targetPath) {
  try {
    const st = await fs.promises.stat(targetPath)
    return Number(st.size || 0)
  } catch (err) {
    if (err && typeof err === 'object' && err.code === 'ENOENT') return 0
    throw err
  }
}

async function main() {
  let unhandledRejectionCount = 0
  const onUnhandledRejection = (reason) => {
    unhandledRejectionCount += 1
    logger.error('unhandled_promise_rejection', {
      error: reason instanceof Error ? reason.message : String(reason)
    })
  }
  process.on('unhandledRejection', onUnhandledRejection)

  const initialBytes = await fileSizeBytes(filePath)
  let finalBytes = initialBytes
  let peakHeapUsed = process.memoryUsage().heapUsed

  const sampleRuntimeUsage = async () => {
    const heapUsed = process.memoryUsage().heapUsed
    if (heapUsed > peakHeapUsed) peakHeapUsed = heapUsed
    finalBytes = await fileSizeBytes(filePath)
  }

  const usageTimer = setInterval(() => {
    void sampleRuntimeUsage().catch((err) => {
      logger.warn('usage_sample_failed', { error: err instanceof Error ? err.message : String(err) })
    })
  }, 5000)

  const tierPlan = buildTierCommands(options.tier)
  logger.info('stress_test_start', {
    filePath,
    simulateCrash: options.simulateCrash,
    agents: options.agents,
    tier: tierPlan.tierLabel,
    totalCommands: tierPlan.commands.length
  })
  process.stdout.write(`Running ${tierPlan.tierLabel}\n`)

  try {
    await runCommands(tierPlan.commands, tierPlan.delayMaxMs)
    await memoryStore.saveAllMemory()
    await sampleRuntimeUsage()
  } finally {
    clearInterval(usageTimer)
    process.off('unhandledRejection', onUnhandledRejection)
  }

  const integrity = memoryStore.validateMemoryIntegrity()
  const snapshot = memoryStore.getSnapshot()
  const runtime = memoryStore.getRuntimeMetrics()
  const observability = getObservabilitySnapshot()
  const runtimeMs = Date.now() - startedAt
  let restartIntegrityOk = null
  if (options.simulateCrash) {
    const restartStore = createMemoryStore({
      filePath,
      logger: logger.child({ subsystem: 'restart_validation' })
    })
    restartStore.loadAllMemory({ reload: true })
    restartIntegrityOk = restartStore.validateMemoryIntegrity().ok
  }

  const avgTxMs = observability.txDurationCount > 0
    ? observability.txDurationTotalMs / observability.txDurationCount
    : 0
  const avgLockAcqMs = observability.lockAcquisitionCount > 0
    ? observability.lockAcquisitionTotalMs / observability.lockAcquisitionCount
    : 0

  const peakHeapMb = peakHeapUsed / (1024 * 1024)
  const memoryDelta = finalBytes - initialBytes

  process.stdout.write(`AGENTS: ${options.agents}\n`)
  process.stdout.write(`TIER: ${options.tier}\n`)
  process.stdout.write(`TOTAL_COMMANDS: ${tierPlan.commands.length}\n`)
  process.stdout.write(`PEAK_HEAP_MB: ${peakHeapMb.toFixed(2)}\n`)
  process.stdout.write(`MEMORY_JSON_BYTES: ${finalBytes}\n`)
  process.stdout.write(`AVG_TX_MS: ${avgTxMs.toFixed(2)}\n`)
  process.stdout.write(`MAX_TX_MS: ${observability.txDurationMaxMs.toFixed(2)}\n`)
  process.stdout.write(`P50_TX_MS: ${observability.txDurationP50Ms.toFixed(2)}\n`)
  process.stdout.write(`P95_TX_MS: ${observability.txDurationP95Ms.toFixed(2)}\n`)
  process.stdout.write(`P99_TX_MS: ${observability.txDurationP99Ms.toFixed(2)}\n`)
  process.stdout.write(`SLOW_TX_COUNT: ${observability.slowTransactionCount}\n`)
  process.stdout.write(`LOCK_RETRIES: ${runtime.lockRetries}\n`)
  process.stdout.write(`LOCK_TIMEOUTS: ${runtime.lockTimeouts}\n`)
  process.stdout.write(`DUPLICATES_SKIPPED: ${runtime.duplicateEventsSkipped}\n`)
  process.stdout.write(`OPENAI_TIMEOUTS: ${runtime.openAiTimeouts}\n`)
  process.stdout.write(`INTEGRITY_OK: ${integrity.ok}\n`)
  process.stdout.write(`LOCK_AVG_ACQ_MS: ${avgLockAcqMs.toFixed(2)}\n`)
  process.stdout.write(`MEMORY_JSON_DELTA_BYTES: ${memoryDelta}\n`)
  process.stdout.write(`MEMORY_AGENT_COUNT: ${Object.keys(snapshot.agents || {}).length}\n`)
  process.stdout.write(`UNHANDLED_REJECTIONS: ${unhandledRejectionCount}\n`)
  if (options.timers) {
    process.stdout.write(`LOCK_WAIT_P95_MS: ${observability.txPhaseP95Ms.lockWaitMs.toFixed(2)}\n`)
    process.stdout.write(`LOCK_WAIT_P99_MS: ${observability.txPhaseP99Ms.lockWaitMs.toFixed(2)}\n`)
    process.stdout.write(`CLONE_P95_MS: ${observability.txPhaseP95Ms.cloneMs.toFixed(2)}\n`)
    process.stdout.write(`CLONE_P99_MS: ${observability.txPhaseP99Ms.cloneMs.toFixed(2)}\n`)
    process.stdout.write(`STRINGIFY_P95_MS: ${observability.txPhaseP95Ms.stringifyMs.toFixed(2)}\n`)
    process.stdout.write(`STRINGIFY_P99_MS: ${observability.txPhaseP99Ms.stringifyMs.toFixed(2)}\n`)
    process.stdout.write(`WRITE_P95_MS: ${observability.txPhaseP95Ms.writeMs.toFixed(2)}\n`)
    process.stdout.write(`WRITE_P99_MS: ${observability.txPhaseP99Ms.writeMs.toFixed(2)}\n`)
    process.stdout.write(`RENAME_P95_MS: ${observability.txPhaseP95Ms.renameMs.toFixed(2)}\n`)
    process.stdout.write(`RENAME_P99_MS: ${observability.txPhaseP99Ms.renameMs.toFixed(2)}\n`)
    process.stdout.write(`TOTAL_TX_P95_MS: ${observability.txPhaseP95Ms.totalTxMs.toFixed(2)}\n`)
    process.stdout.write(`TOTAL_TX_P99_MS: ${observability.txPhaseP99Ms.totalTxMs.toFixed(2)}\n`)
  }
  if (restartIntegrityOk !== null) process.stdout.write(`RESTART_INTEGRITY_OK: ${restartIntegrityOk}\n`)
  process.stdout.write(`TOTAL_RUNTIME_MS: ${runtimeMs}\n`)

  if (!integrity.ok) {
    logger.error('memory_integrity_failed', { issues: integrity.issues })
    process.exitCode = 1
  }
}

main().catch((err) => {
  logger.errorWithStack('stress_test_failed', err)
  process.exitCode = 1
})
