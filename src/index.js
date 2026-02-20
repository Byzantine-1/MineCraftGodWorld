require('dotenv').config()

const path = require('path')
const readline = require('readline')

const Agent = require('./agent')
const { createMemoryStore } = require('./memory')
const { createDialogueService } = require('./dialogue')
const { createActionEngine } = require('./actionEngine')
const { createTurnEngine } = require('./turnEngine')
const { createGodCommandService } = require('./godCommands')
const { parseCliInput } = require('./commandParsers')
const { createLogger } = require('./logger')
const { installCrashHandlers } = require('./crashHandlers')
const { AppError } = require('./errors')
const { createKeyedQueue, deriveOperationId, hashText } = require('./flowControl')

const logger = createLogger({ component: 'cli' })
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
const godCommandService = createGodCommandService({
  memoryStore,
  logger: logger.child({ subsystem: 'god_commands' })
})
const runSerial = createKeyedQueue()

/** @type {Record<string, Agent>} */
const agents = {
  mara: new Agent({ name: 'Mara', role: 'Scout', faction: 'Pilgrims' }),
  eli: new Agent({ name: 'Eli', role: 'Guard', faction: 'Pilgrims' })
}

memoryStore.loadAllMemory()

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

async function shutdown(reason) {
  if (shuttingDown) return
  shuttingDown = true
  logger.info('shutdown_start', { reason })
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
writeLine(' exit')
writeLine('---------------------')
rl.prompt()

/**
 * @param {string} rawInput
 */
async function handleLine(rawInput) {
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

    for (const agent of Object.values(agents)) {
      await memoryStore.rememberAgent(agent.name, `God issued command "${parsed.command}".`, true, `${operationId}:audit`)
      await memoryStore.rememberFaction(agent.faction, `God issued command "${parsed.command}".`, true, `${operationId}:audit`)
    }
    await memoryStore.rememberWorld(`God issued command "${parsed.command}".`, true, `${operationId}:audit`)

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
