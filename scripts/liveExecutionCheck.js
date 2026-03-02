const assert = require('node:assert/strict')
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawn } = require('node:child_process')

const { createExecutionAdapter, isValidExecutionResult } = require('../src/executionAdapter')
const {
  createExecutionStore,
  createMemoryExecutionPersistence,
  createSqliteExecutionPersistence
} = require('../src/executionStore')
const { createGodCommandService } = require('../src/godCommands')
const { createLogger } = require('../src/logger')
const { createMemoryStore } = require('../src/memory')
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
      reason: 'Live execution CLI validation.',
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
  const { dir, memoryPath, sqlitePath } = createTempPaths(`mvp-live-execution-${backend}-`)
  const now = fixedNowFactory()
  const logger = createSilentLogger('live_execution_seed')
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

async function seedExecutionState(context) {
  const agents = createAgents()

  await context.memoryStore.transact((memory) => {
    memory.world.clock.updated_at = '2026-02-25T00:00:00.000Z'
  }, { eventId: 'live-execution:seed-clock' })

  await context.godCommandService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'live-execution:seed-town-alpha'
  })
  await context.godCommandService.applyGodCommand({
    agents,
    command: 'project start alpha lantern_line',
    operationId: 'live-execution:seed-project-alpha'
  })

  const projectId = context.memoryStore.getSnapshot().world.projects[0].id
  const handoff = createHandoff({
    proposalType: 'PROJECT_ADVANCE',
    command: `project advance alpha ${projectId}`,
    args: { projectId },
    snapshotHash: snapshotHashForStore(context.memoryStore),
    preconditions: [{ kind: 'project_exists', targetId: projectId }]
  })

  return {
    projectId,
    handoff
  }
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
      reject(new Error(`Live execution check timed out after ${timeoutMs}ms`))
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

function createReloadedExecutionStore(context) {
  const logger = createSilentLogger('live_execution_verify')
  const memoryStore = createMemoryStore({
    filePath: context.memoryPath,
    logger: logger.child({ subsystem: 'memory' })
  })
  const persistenceBackend = context.backend === 'sqlite'
    ? createSqliteExecutionPersistence({ dbPath: context.sqlitePath })
    : createMemoryExecutionPersistence({ memoryStore })
  return {
    memoryStore,
    executionStore: createExecutionStore({
      memoryStore,
      persistenceBackend,
      logger: logger.child({ subsystem: 'execution_store' })
    })
  }
}

function pickStableResultProjection(result) {
  return {
    type: result.type,
    schemaVersion: result.schemaVersion,
    handoffId: result.handoffId,
    proposalId: result.proposalId,
    idempotencyKey: result.idempotencyKey,
    snapshotHash: result.snapshotHash,
    decisionEpoch: result.decisionEpoch,
    actorId: result.actorId,
    townId: result.townId,
    proposalType: result.proposalType,
    command: result.command,
    authorityCommands: result.authorityCommands,
    status: result.status,
    accepted: result.accepted,
    executed: result.executed,
    reasonCode: result.reasonCode,
    evaluation: result.evaluation,
    worldState: result.worldState,
    ...(Object.prototype.hasOwnProperty.call(result, 'embodiment')
      ? { embodiment: result.embodiment }
      : {})
  }
}

async function runSingleLiveExecution(options = {}) {
  const backend = options.backend === 'sqlite' ? 'sqlite' : 'memory'
  const timeoutMs = Number.isInteger(options.timeoutMs) ? options.timeoutMs : 15000
  const context = createSeedingContext(backend)
  const seeded = await seedExecutionState(context)
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
  let stdoutBuffer = ''
  let stderrBuffer = ''
  let started = false
  let handoffSent = false
  let exitSent = false
  let resultPayload = null

  function sendHandoff() {
    if (handoffSent) {
      return
    }
    handoffSent = true
    child.stdin.write(`${JSON.stringify(seeded.handoff)}\n`)
  }

  function sendExit() {
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
    if (!parsed || parsed.type !== 'execution-result.v1') {
      return
    }
    resultPayload = parsed
    sendExit()
  }

  child.stdout.on('data', (chunk) => {
    const text = String(chunk)
    stdoutBuffer += text
    if (!started && stdoutBuffer.includes('--- WORLD ONLINE ---')) {
      started = true
      sendHandoff()
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
  assert(resultPayload, 'expected one canonical execution-result.v1 response')
  assert.equal(resultPayload.type, 'execution-result.v1')
  assert.equal(resultPayload.schemaVersion, 1)
  assert.equal(isValidExecutionResult(resultPayload), true)
  assert.equal(resultPayload.handoffId, seeded.handoff.handoffId)
  assert.equal(resultPayload.proposalId, seeded.handoff.proposalId)
  assert.equal(resultPayload.idempotencyKey, seeded.handoff.idempotencyKey)
  assert.equal(resultPayload.snapshotHash, seeded.handoff.snapshotHash)
  assert.equal(resultPayload.status, 'executed')
  assert.equal(resultPayload.accepted, true)
  assert.equal(resultPayload.executed, true)
  assert.equal(resultPayload.reasonCode, 'EXECUTED')
  assert(Array.isArray(resultPayload.authorityCommands))
  assert.equal(resultPayload.authorityCommands.length, 1)
  assert.equal(resultPayload.authorityCommands[0], `project advance alpha ${seeded.projectId}`)
  assert.match(resultPayload.executionId, /^result_[0-9a-f]{64}$/)
  assert.equal(resultPayload.resultId, resultPayload.executionId)
  assert.match(resultPayload.evaluation.staleCheck.actualSnapshotHash, /^[0-9a-f]{64}$/)
  assert.match(resultPayload.worldState.postExecutionSnapshotHash, /^[0-9a-f]{64}$/)

  const reloaded = createReloadedExecutionStore(context)
  const receipt = reloaded.executionStore.findReceipt({
    handoffId: seeded.handoff.handoffId,
    idempotencyKey: seeded.handoff.idempotencyKey
  })
  const pendingExecutions = reloaded.executionStore.listPendingExecutions()
  const executedProject = reloaded.memoryStore.getSnapshot().world.projects.find((entry) => entry.id === seeded.projectId)

  assert(receipt, 'expected durable execution receipt after live execution')
  assert.equal(receipt.executionId, resultPayload.executionId)
  assert.equal(receipt.status, resultPayload.status)
  assert.equal(pendingExecutions.length, 0, 'pending execution markers should be cleared after success')
  assert(executedProject, 'expected seeded project to exist after live execution')
  assert.equal(executedProject.stage, 2)

  return {
    backend,
    handoff: seeded.handoff,
    result: resultPayload,
    receipt,
    pendingExecutions,
    stdoutLines,
    stderrLines,
    capturedLiveFromChildProcess: true
  }
}

async function runLiveExecutionCheck(options = {}) {
  const firstRun = await runSingleLiveExecution(options)
  const secondRun = await runSingleLiveExecution(options)

  assert.deepEqual(
    pickStableResultProjection(secondRun.result),
    pickStableResultProjection(firstRun.result),
    'same seeded live handoff should yield deterministic canonical execution semantics'
  )

  return {
    backend: firstRun.backend,
    handoff: firstRun.handoff,
    result: firstRun.result,
    receipt: firstRun.receipt,
    pendingExecutions: firstRun.pendingExecutions,
    stdoutLines: firstRun.stdoutLines,
    stderrLines: firstRun.stderrLines,
    capturedLiveFromChildProcess: true,
    deterministicReplayVerified: true
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
  runLiveExecutionCheck
}

if (require.main === module) {
  runLiveExecutionCheck(parseCliOptions(process.argv.slice(2)))
    .then((result) => {
      process.stdout.write('Live execution check passed. Captured canonical execution-result.v1 from a child process.\n')
      process.stdout.write(`${JSON.stringify(result.result)}\n`)
    })
    .catch((error) => {
      process.stderr.write(`${error instanceof Error ? error.stack || error.message : String(error)}\n`)
      process.exitCode = 1
    })
}
