require('dotenv').config()

const fs = require('fs')
const path = require('path')
const readline = require('readline')

const Agent = require('./agent')
const { createMemoryStore } = require('./memory')
const { createDialogueService } = require('./dialogue')
const { createActionEngine } = require('./actionEngine')
const { createTurnEngine } = require('./turnEngine')
const { createGodCommandService } = require('./godCommands')
const { createExecutionAdapter, parseExecutionHandoffLine } = require('./executionAdapter')
const { createExecutionStore, createExecutionPersistenceBackend } = require('./executionStore')
const { createWorldMemoryContextForRequest, parseWorldMemoryRequestLine } = require('./worldMemoryContext')
const { parseCliInput } = require('./commandParsers')
const { createLogger } = require('./logger')
const { installCrashHandlers } = require('./crashHandlers')
const { AppError } = require('./errors')
const { createKeyedQueue, deriveOperationId, hashText } = require('./flowControl')
const { startRuntimeMetricsReporter, getObservabilitySnapshot } = require('./runtimeMetrics')
const { createWorldLoop } = require('./worldLoop')

const logger = createLogger({ component: 'cli' })
startRuntimeMetricsReporter(logger.child({ subsystem: 'metrics' }), 60000)
const memoryStore = createMemoryStore({
  filePath: path.resolve(__dirname, './memory.json'),
  logger: logger.child({ subsystem: 'memory' })
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
const runSerial = createKeyedQueue()

/** @type {Record<string, Agent>} */
const agents = {
  mara: new Agent({ name: 'Mara', role: 'Scout', faction: 'Pilgrims' }),
  eli: new Agent({ name: 'Eli', role: 'Guard', faction: 'Pilgrims' })
}

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: '> '
})

let shuttingDown = false

/**
 * @param {string} text
 * @param {Record<string, unknown>} [context]
 */
function writeLine(text, context = {}) {
  process.stdout.write(`${text}\n`)
  logger.info('user_message', { text, ...context })
}

function parseJsonObjectEnv(name) {
  const raw = process.env[name]
  if (!raw) {
    return undefined
  }

  try {
    const parsed = JSON.parse(raw)
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : undefined
  } catch (error) {
    logger.warn('invalid_json_env', {
      name,
      error: error instanceof Error ? error.message : String(error)
    })
    return undefined
  }
}

function selectExecutionPersistenceConfig() {
  const backend = String(process.env.EXECUTION_PERSISTENCE_BACKEND || 'memory').trim().toLowerCase() || 'memory'
  return {
    backend,
    sqliteDbPath: backend === 'sqlite'
      ? path.resolve(process.env.EXECUTION_PERSISTENCE_SQLITE_PATH || path.resolve(__dirname, './execution.sqlite3'))
      : undefined,
    sqliteCommand: backend === 'sqlite'
      ? (String(process.env.EXECUTION_PERSISTENCE_SQLITE_COMMAND || 'sqlite3').trim() || 'sqlite3')
      : undefined
  }
}

/**
 * @returns {{loopStatus: any, agentsOnline: number, avgTxMs: number, p95TxMs: number, p99TxMs: number, lockWaitP95Ms: number, lockWaitP99Ms: number, memoryBytes: number, heapMb: number, guardrailFlags: string[]}}
 */
function buildGodStatusSnapshot() {
  const loopStatus = worldLoop.getWorldLoopStatus()
  const observability = getObservabilitySnapshot()
  const runtime = memoryStore.getRuntimeMetrics()
  const avgTxMs = observability.txDurationCount > 0
    ? observability.txDurationTotalMs / observability.txDurationCount
    : 0
  const slowRate = observability.txDurationCount > 0
    ? observability.slowTransactionCount / observability.txDurationCount
    : 0
  const guardrailFlags = []
  const integrity = memoryStore.validateMemoryIntegrity()
  if (!integrity.ok) guardrailFlags.push('CRITICAL:integrity_failed')
  if (runtime.lockTimeouts > 0) guardrailFlags.push('CRITICAL:lock_timeouts')
  if (observability.txDurationP99Ms >= 500) guardrailFlags.push('CRITICAL:p99_tx_ge_500')
  if (slowRate > 0.10) guardrailFlags.push('WARN:slow_tx_rate_gt_0_10')
  if (loopStatus.backpressure) guardrailFlags.push(`WARN:backpressure:${loopStatus.reason}`)

  let memoryBytes = 0
  try {
    memoryBytes = fs.statSync(path.resolve(__dirname, './memory.json')).size
  } catch (err) {
    memoryBytes = Buffer.byteLength(JSON.stringify(memoryStore.getSnapshot()), 'utf-8')
  }

  return {
    loopStatus,
    agentsOnline: Object.keys(agents).length,
    avgTxMs,
    p95TxMs: observability.txDurationP95Ms,
    p99TxMs: observability.txDurationP99Ms,
    lockWaitP95Ms: observability.txPhaseP95Ms.lockWaitMs,
    lockWaitP99Ms: observability.txPhaseP99Ms.lockWaitMs,
    memoryBytes,
    heapMb: process.memoryUsage().heapUsed / (1024 * 1024),
    guardrailFlags
  }
}

const worldLoop = createWorldLoop({
  memoryStore,
  getAgents: () => Object.values(agents),
  logger: logger.child({ subsystem: 'world_loop' }),
  runtimeActions: {
    onWander: ({ agent, direction }) => {
      writeLine(`[LOOP] ${agent.name} wanders ${direction}.`)
    },
    onFollow: ({ agent, leaderName }) => {
      if (!leaderName) return
      writeLine(`[LOOP] ${agent.name} follows ${leaderName}.`)
    },
    onRespond: ({ agent, message }) => {
      writeLine(`${agent.name}: ${message}`)
    },
    onNews: ({ line }) => {
      writeLine(line)
    }
  }
})

const godCommandService = createGodCommandService({
  memoryStore,
  logger: logger.child({ subsystem: 'god_commands' }),
  worldLoop,
  runtimeSay: ({ agent, message }) => {
    writeLine(`${agent.name}: ${message}`)
  },
  getStatusSnapshot: () => buildGodStatusSnapshot()
})
const executionPersistenceConfig = selectExecutionPersistenceConfig()
const executionPersistenceBackend = createExecutionPersistenceBackend({
  backend: executionPersistenceConfig.backend,
  memoryStore,
  sqliteDbPath: executionPersistenceConfig.sqliteDbPath,
  sqliteCommand: executionPersistenceConfig.sqliteCommand,
  logger: logger.child({ subsystem: 'execution_persistence' })
})
const executionStore = createExecutionStore({
  memoryStore,
  logger: logger.child({ subsystem: 'execution_store' }),
  persistenceBackend: executionPersistenceBackend
})
const executionAdapter = createExecutionAdapter({
  memoryStore,
  executionStore,
  godCommandService,
  logger: logger.child({ subsystem: 'execution_adapter' }),
  townIdAliases: parseJsonObjectEnv('EXECUTION_ADAPTER_TOWN_MAP')
})

memoryStore.loadAllMemory()
const startupExecutionRecovery = executionAdapter.recoverInterruptedExecutions()
  .catch((error) => {
    logger.errorWithStack('execution_recovery_failed', error)
    throw error
  })

async function shutdown(reason) {
  if (shuttingDown) return
  shuttingDown = true
  logger.info('shutdown_start', { reason })
  try {
    worldLoop.stopWorldLoop()
  } catch (err) {
    logger.warn('shutdown_loop_stop_failed', { error: err instanceof Error ? err.message : String(err) })
  }
  try {
    await memoryStore.saveAllMemory()
  } catch (err) {
    logger.errorWithStack('shutdown_save_failed', err)
  }
  try {
    rl.close()
  } catch (err) {
    logger.warn('shutdown_close_failed', { error: err instanceof Error ? err.message : String(err) })
  }
}

installCrashHandlers({
  component: 'cli',
  logger,
  onFatal: async () => {
    await shutdown('fatal')
  },
  exitOnFatal: true
})

writeLine('--- WORLD ONLINE ---')
writeLine('Commands:')
writeLine(' talk <agent> <message>')
writeLine(' god <command>')
writeLine(' {"type":"world-memory-request.v1","schemaVersion":1,...}')
writeLine(' {"schemaVersion":"execution-handoff.v1",...}')
writeLine(' exit')
writeLine('---------------------')
rl.prompt()

/**
 * @param {string} rawInput
 */
async function handleLine(rawInput) {
  await startupExecutionRecovery

  const worldMemoryRequest = parseWorldMemoryRequestLine(rawInput)
  if (worldMemoryRequest) {
    const worldMemoryContext = createWorldMemoryContextForRequest({
      executionStore,
      request: worldMemoryRequest
    })
    writeLine(JSON.stringify(worldMemoryContext), {
      schema: worldMemoryContext.type,
      townId: worldMemoryContext.scope.townId || undefined,
      factionId: worldMemoryContext.scope.factionId || undefined
    })
    return
  }

  const handoff = parseExecutionHandoffLine(rawInput)
  if (handoff) {
    const result = await executionAdapter.executeHandoff({
      handoff,
      agents: Object.values(agents)
    })
    writeLine(JSON.stringify(result), {
      schema: result.type,
      executionId: result.executionId,
      handoffId: result.handoffId
    })
    return
  }

  const parsed = parseCliInput(rawInput)
  if (parsed.type === 'noop') return

  if (parsed.type === 'error') {
    writeLine(parsed.message)
    return
  }

  if (parsed.type === 'unknown') {
    writeLine('Unknown command.')
    return
  }

  if (parsed.type === 'exit') {
    writeLine('World saved. Exiting.')
    await shutdown('user_exit')
    return
  }

  if (parsed.type === 'talk') {
    const agent = agents[parsed.target]
    if (!agent) {
      writeLine(`No agent named "${parsed.target}".`)
      return
    }

    const world = memoryStore.recallWorld()
    if (world.player.alive === false) {
      writeLine('The player is dead. Restart after setting world.player.alive=true in src/memory.json.')
      return
    }

    await runSerial(`agent:${agent.name}`, async () => {
      const operationId = deriveOperationId(['cli', 'talk', agent.name, parsed.message], { windowMs: 5000 })
      const turnLogger = logger.child({ operationId, command: 'talk', agent: agent.name })
      turnLogger.info('turn_start', { message_len: parsed.message.length, message_hash: hashText(parsed.message) })

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

      writeLine(`${agent.name}: ${applied.turn.say}`)
      if (!applied.playerAlive) {
        writeLine('The world turns on you. Your character has been killed.')
      }

      turnLogger.info('turn_complete', {
        skipped: applied.skipped,
        trust: agent.trust,
        mood: agent.mood,
        outcomes: applied.outcomes
      })
    })
    return
  }

  if (parsed.type === 'god') {
    const operationId = deriveOperationId(['cli', 'god', parsed.command], { windowMs: 5000 })
    const result = await godCommandService.applyGodCommand({
      agents: Object.values(agents),
      command: parsed.command,
      operationId
    })
    if (!result.applied) {
      writeLine(`GOD COMMAND IGNORED: ${parsed.command} (${result.reason})`)
      return
    }

    if (result.audit) {
      for (const agent of Object.values(agents)) {
        await memoryStore.rememberAgent(agent.name, `God issued command "${parsed.command}".`, true, `${operationId}:audit`)
        await memoryStore.rememberFaction(agent.faction, `God issued command "${parsed.command}".`, true, `${operationId}:audit`)
      }
      await memoryStore.rememberWorld(`God issued command "${parsed.command}".`, true, `${operationId}:audit`)
    }

    if (Array.isArray(result.outputLines) && result.outputLines.length > 0) {
      result.outputLines.forEach(line => writeLine(line))
      return
    }
    writeLine(`GOD COMMAND APPLIED: ${parsed.command}`)
  }
}

rl.on('line', (input) => {
  void handleLine(input)
    .catch(async (err) => {
      if (err instanceof AppError && err.recoverable) {
        logger.warn('command_failed_recoverable', { code: err.code, message: err.message, metadata: err.metadata })
        writeLine('Command rejected. Please check input and retry.')
        return
      }

      logger.errorWithStack('command_failed_fatal', err)
      writeLine('Internal fatal error occurred. Shutting down.')
      await shutdown('fatal_command_error')
      process.exit(1)
    })
    .finally(() => {
      if (shuttingDown) return
      rl.prompt()
    })
})

rl.on('close', () => {
  logger.info('session_closed')
})
