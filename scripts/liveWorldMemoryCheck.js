const assert = require('node:assert/strict')
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawn } = require('node:child_process')

const { createExecutionAdapter } = require('../src/executionAdapter')
const {
  createExecutionStore,
  createMemoryExecutionPersistence,
  createSqliteExecutionPersistence
} = require('../src/executionStore')
const { createGodCommandService } = require('../src/godCommands')
const { createLogger } = require('../src/logger')
const { createMemoryStore } = require('../src/memory')
const {
  createWorldMemoryRequest,
  MAX_CONTEXT_CHRONICLE_RECORDS,
  MAX_CONTEXT_HISTORY_RECORDS
} = require('../src/worldMemoryContext')
const { createAuthoritativeSnapshotProjection } = require('../src/worldSnapshotProjection')

const REPO_ROOT = path.resolve(__dirname, '..')
const ENGINE_ENTRYPOINT = path.resolve(REPO_ROOT, 'src', 'index.js')

function createSilentLogger(component) {
  return createLogger({
    component,
    minLevel: 'error',
    sink: {
      log() {},
      error() {}
    }
  })
}

function createTempPaths(prefix) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix))
  return {
    dir,
    memoryPath: path.join(dir, 'memory.json'),
    sqlitePath: path.join(dir, 'execution.sqlite3')
  }
}

function fixedNowFactory() {
  return () => Date.parse('2026-02-25T00:00:00.000Z')
}

function createAgents() {
  return [
    { name: 'Mara', faction: 'Pilgrims', applyGodCommand: () => {} },
    { name: 'Eli', faction: 'Pilgrims', applyGodCommand: () => {} }
  ]
}

function buildId(prefix, payload) {
  return `${prefix}_${crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex')}`
}

function snapshotHashForStore(memoryStore) {
  return createAuthoritativeSnapshotProjection(memoryStore.recallWorld()).snapshotHash
}

function createHandoff({
  proposalType,
  command,
  args,
  townId = 'alpha',
  actorId = 'mara',
  decisionEpoch = 1,
  snapshotHash = 'a'.repeat(64),
  preconditions = []
}) {
  const proposalId = buildId('proposal', {
    proposalType,
    command,
    args,
    townId,
    actorId,
    decisionEpoch
  })
  const handoffId = buildId('handoff', {
    proposalId,
    command
  })

  return {
    schemaVersion: 'execution-handoff.v1',
    handoffId,
    advisory: true,
    proposalId,
    idempotencyKey: proposalId,
    snapshotHash,
    decisionEpoch,
    proposal: {
      schemaVersion: 'proposal.v2',
      proposalId,
      snapshotHash,
      decisionEpoch,
      type: proposalType,
      actorId,
      townId,
      priority: 0.9,
      reason: 'Live world-memory CLI validation.',
      reasonTags: ['integration-test'],
      args
    },
    command,
    executionRequirements: {
      expectedSnapshotHash: snapshotHash,
      expectedDecisionEpoch: decisionEpoch,
      preconditions
    }
  }
}

function createSeedingContext(backend) {
  const { dir, memoryPath, sqlitePath } = createTempPaths(`mvp-live-world-memory-${backend}-`)
  const now = fixedNowFactory()
  const logger = createSilentLogger('live_world_memory_seed')
  const memoryStore = createMemoryStore({
    filePath: memoryPath,
    now,
    logger: logger.child({ subsystem: 'memory' })
  })
  const persistenceBackend = backend === 'sqlite'
    ? createSqliteExecutionPersistence({ dbPath: sqlitePath, now })
    : createMemoryExecutionPersistence({ memoryStore })
  const executionStore = createExecutionStore({
    memoryStore,
    persistenceBackend,
    logger: logger.child({ subsystem: 'execution_store' })
  })
  const godCommandService = createGodCommandService({
    memoryStore,
    logger: logger.child({ subsystem: 'god_commands' })
  })
  const executionAdapter = createExecutionAdapter({
    memoryStore,
    godCommandService,
    executionStore,
    logger: logger.child({ subsystem: 'execution_adapter' })
  })

  return {
    backend,
    dir,
    memoryPath,
    sqlitePath,
    memoryStore,
    executionStore,
    godCommandService,
    executionAdapter
  }
}

async function seedWorldMemory(context) {
  const agents = createAgents()

  await context.godCommandService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'live-world-memory:seed-town-alpha'
  })
  await context.godCommandService.applyGodCommand({
    agents,
    command: 'project start alpha lantern_line',
    operationId: 'live-world-memory:seed-project-alpha'
  })
  await context.memoryStore.transact((memory) => {
    memory.world.factions.iron_pact = {
      name: 'iron_pact',
      towns: ['alpha'],
      doctrine: 'Order through steel.',
      rivals: ['veil_church'],
      hostilityToPlayer: 22,
      stability: 74
    }
    memory.world.chronicle.push(
      {
        id: 'c_alpha_01',
        type: 'mission',
        msg: 'Alpha mayor briefed a new mission.',
        at: 9000000000101,
        town: 'alpha',
        meta: { factionId: 'iron_pact', missionId: 'mm_alpha_1' }
      },
      {
        id: 'c_alpha_02',
        type: 'project',
        msg: 'Alpha raised the first lantern posts.',
        at: 9000000000102,
        town: 'alpha',
        meta: { factionId: 'iron_pact', projectId: 'pr_alpha_1' }
      }
    )
  }, { eventId: 'live-world-memory:chronicle-seed' })

  const projectId = context.memoryStore.getSnapshot().world.projects[0].id
  await context.executionAdapter.executeHandoff({
    agents,
    handoff: createHandoff({
      proposalType: 'PROJECT_ADVANCE',
      command: `project advance alpha ${projectId}`,
      args: { projectId },
      snapshotHash: snapshotHashForStore(context.memoryStore),
      preconditions: [{ kind: 'project_exists', targetId: projectId }]
    })
  })
  await context.executionAdapter.executeHandoff({
    agents,
    handoff: createHandoff({
      proposalType: 'SALVAGE_PLAN',
      command: 'salvage initiate alpha scarcity',
      args: { focus: 'scarcity' },
      decisionEpoch: 2,
      snapshotHash: snapshotHashForStore(context.memoryStore),
      preconditions: [{ kind: 'salvage_focus_supported', expected: 'scarcity' }]
    })
  })

  context.executionStore.syncWorldMemory()
}

function stripPrompt(line) {
  let trimmed = line.trim()
  while (trimmed.startsWith('>')) {
    trimmed = trimmed.slice(1).trimStart()
  }
  return trimmed
}

function parseJsonLine(line) {
  const trimmed = stripPrompt(line)
  if (!trimmed.startsWith('{')) {
    return null
  }

  try {
    return JSON.parse(trimmed)
  } catch {
    return null
  }
}

function waitForChildClose(child, timeoutMs) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      child.kill('SIGTERM')
      reject(new Error(`Live world-memory check timed out after ${timeoutMs}ms`))
    }, timeoutMs)

    child.once('error', (error) => {
      clearTimeout(timeout)
      reject(error)
    })

    child.once('close', (code, signal) => {
      clearTimeout(timeout)
      resolve({ code, signal })
    })
  })
}

async function runLiveWorldMemoryCheck(options = {}) {
  const backend = options.backend === 'sqlite' ? 'sqlite' : 'memory'
  const timeoutMs = Number.isInteger(options.timeoutMs) ? options.timeoutMs : 15000
  const context = createSeedingContext(backend)
  await seedWorldMemory(context)

  const request = createWorldMemoryRequest({
    townId: 'alpha',
    factionId: 'iron_pact',
    chronicleLimit: 2,
    historyLimit: 3
  })
  const child = spawn(process.execPath, [ENGINE_ENTRYPOINT], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      LOG_MIN_LEVEL: 'error',
      MEMORY_STORE_FILE_PATH: context.memoryPath,
      EXECUTION_PERSISTENCE_BACKEND: backend,
      ...(backend === 'sqlite'
        ? { EXECUTION_PERSISTENCE_SQLITE_PATH: context.sqlitePath }
        : {})
    },
    stdio: ['pipe', 'pipe', 'pipe']
  })

  const stdoutLines = []
  const stderrLines = []
  const responsePayloads = []
  let stdoutBuffer = ''
  let stderrBuffer = ''
  let started = false
  let requestsSent = 0
  let exitSent = false

  function maybeSendRequest() {
    if (!started || requestsSent >= 2) {
      return
    }
    child.stdin.write(`${JSON.stringify(request)}\n`)
    requestsSent += 1
  }

  function maybeSendExit() {
    if (exitSent) {
      return
    }
    exitSent = true
    child.stdin.write('exit\n')
    child.stdin.end()
  }

  function handleStdoutLine(rawLine) {
    stdoutLines.push(rawLine)
    const parsed = parseJsonLine(rawLine)
    if (!parsed) {
      return
    }
    if (parsed.type !== 'world-memory-context.v1') {
      return
    }
    responsePayloads.push(parsed)
    if (responsePayloads.length < 2) {
      maybeSendRequest()
      return
    }
    maybeSendExit()
  }

  child.stdout.on('data', (chunk) => {
    const text = String(chunk)
    stdoutBuffer += text
    if (!started && stdoutBuffer.includes('--- WORLD ONLINE ---')) {
      started = true
      maybeSendRequest()
    }
    while (stdoutBuffer.includes('\n')) {
      const newlineIndex = stdoutBuffer.indexOf('\n')
      const line = stdoutBuffer.slice(0, newlineIndex)
      stdoutBuffer = stdoutBuffer.slice(newlineIndex + 1)
      handleStdoutLine(line)
    }
  })

  child.stderr.on('data', (chunk) => {
    stderrBuffer += String(chunk)
    while (stderrBuffer.includes('\n')) {
      const newlineIndex = stderrBuffer.indexOf('\n')
      const line = stderrBuffer.slice(0, newlineIndex)
      stderrBuffer = stderrBuffer.slice(newlineIndex + 1)
      stderrLines.push(line)
    }
  })

  const closeResult = await waitForChildClose(child, timeoutMs)

  assert.equal(started, true, 'engine did not reach ready state')
  assert.equal(closeResult.code, 0, `engine exited with code ${closeResult.code}`)
  assert.equal(responsePayloads.length, 2, 'expected exactly one world-memory-context.v1 response per request')

  const firstResponse = responsePayloads[0]
  const secondResponse = responsePayloads[1]
  assert.equal(firstResponse.type, 'world-memory-context.v1')
  assert.equal(firstResponse.schemaVersion, 1)
  assert.deepEqual(firstResponse.scope, request.scope)
  assert(Array.isArray(firstResponse.recentChronicle))
  assert(Array.isArray(firstResponse.recentHistory))
  assert(firstResponse.recentChronicle.length <= request.scope.chronicleLimit)
  assert(firstResponse.recentHistory.length <= request.scope.historyLimit)
  assert(firstResponse.recentChronicle.length <= MAX_CONTEXT_CHRONICLE_RECORDS)
  assert(firstResponse.recentHistory.length <= MAX_CONTEXT_HISTORY_RECORDS)
  assert.deepEqual(secondResponse, firstResponse, 'same live request against same seeded state should be deterministic')

  return {
    backend,
    request,
    response: firstResponse,
    responsesCapturedLive: responsePayloads.length,
    stdoutLines,
    stderrLines
  }
}

function parseCliOptions(argv) {
  const options = {}
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index]
    if (token === '--backend') {
      options.backend = argv[index + 1]
      index += 1
      continue
    }
    if (token === '--timeout-ms') {
      options.timeoutMs = Number(argv[index + 1])
      index += 1
    }
  }
  return options
}

module.exports = {
  runLiveWorldMemoryCheck
}

if (require.main === module) {
  runLiveWorldMemoryCheck(parseCliOptions(process.argv.slice(2)))
    .then((result) => {
      process.stdout.write(`Live world-memory check passed. Captured ${result.responsesCapturedLive} live canonical response(s).\n`)
      process.stdout.write(`${JSON.stringify(result.response)}\n`)
    })
    .catch((error) => {
      process.stderr.write(`${error instanceof Error ? error.stack || error.message : String(error)}\n`)
      process.exitCode = 1
    })
}
