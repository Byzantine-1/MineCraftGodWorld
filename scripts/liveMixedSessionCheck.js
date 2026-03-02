const assert = require('node:assert/strict')
const crypto = require('node:crypto')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
const { spawn } = require('node:child_process')

const { isValidExecutionResult } = require('../src/executionAdapter')
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
const FIXED_NOW_ISO = '2026-02-25T00:00:00.000Z'

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
  return () => Date.parse(FIXED_NOW_ISO)
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
      reason: 'Live mixed-session validation.',
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
  const { dir, memoryPath, sqlitePath } = createTempPaths(`mvp-live-mixed-session-${backend}-`)
  const now = fixedNowFactory()
  const logger = createSilentLogger('live_mixed_session_seed')
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

  return {
    backend,
    dir,
    memoryPath,
    sqlitePath,
    memoryStore,
    executionStore,
    godCommandService
  }
}

async function seedMixedSessionState(context) {
  const agents = createAgents()

  await context.memoryStore.transact((memory) => {
    memory.world.clock.updated_at = FIXED_NOW_ISO
  }, { eventId: 'live-mixed-session:seed-clock' })

  await context.godCommandService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'live-mixed-session:seed-town-alpha'
  })
  await context.godCommandService.applyGodCommand({
    agents,
    command: 'project start alpha lantern_line',
    operationId: 'live-mixed-session:seed-project-alpha'
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
  }, { eventId: 'live-mixed-session:seed-chronicle' })

  const projectId = context.memoryStore.getSnapshot().world.projects[0].id
  const worldMemoryRequest = createWorldMemoryRequest({
    townId: 'alpha',
    factionId: 'iron_pact',
    chronicleLimit: 2,
    historyLimit: 3
  })
  const handoff = createHandoff({
    proposalType: 'PROJECT_ADVANCE',
    command: `project advance alpha ${projectId}`,
    args: { projectId },
    snapshotHash: snapshotHashForStore(context.memoryStore),
    preconditions: [{ kind: 'project_exists', targetId: projectId }]
  })

  return {
    projectId,
    request: worldMemoryRequest,
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
      reject(new Error(`Live mixed-session check timed out after ${timeoutMs}ms`))
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
  const logger = createSilentLogger('live_mixed_session_verify')
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

function pickStableWorldMemoryContext(context) {
  return {
    type: context.type,
    schemaVersion: context.schemaVersion,
    scope: context.scope,
    recentChronicle: context.recentChronicle,
    recentHistory: context.recentHistory,
    townSummary: context.townSummary ?? null,
    factionSummary: context.factionSummary ?? null
  }
}

function pickStableExecutionResult(result) {
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

function pickStableMixedProjection(result) {
  return {
    request: result.request,
    pre: pickStableWorldMemoryContext(result.preExecutionContext),
    execution: pickStableExecutionResult(result.executionResult),
    post: pickStableWorldMemoryContext(result.postExecutionContext),
    projectStage: result.projectStage,
    receipt: {
      executionId: result.receipt.executionId,
      handoffId: result.receipt.handoffId,
      idempotencyKey: result.receipt.idempotencyKey,
      status: result.receipt.status,
      reasonCode: result.receipt.reasonCode,
      postExecutionSnapshotHash: result.receipt.postExecutionSnapshotHash,
      postExecutionDecisionEpoch: result.receipt.postExecutionDecisionEpoch
    }
  }
}

function assertCanonicalWorldMemoryContext(response, request) {
  assert.equal(response.type, 'world-memory-context.v1')
  assert.equal(response.schemaVersion, 1)
  assert.deepEqual(response.scope, request.scope)
  assert(Array.isArray(response.recentChronicle))
  assert(Array.isArray(response.recentHistory))
  assert(response.recentChronicle.length <= request.scope.chronicleLimit)
  assert(response.recentHistory.length <= request.scope.historyLimit)
  assert(response.recentChronicle.length <= MAX_CONTEXT_CHRONICLE_RECORDS)
  assert(response.recentHistory.length <= MAX_CONTEXT_HISTORY_RECORDS)
  assert(response.townSummary)
  assert(response.factionSummary)
}

async function runSingleLiveMixedSession(options = {}) {
  const backend = options.backend === 'sqlite' ? 'sqlite' : 'memory'
  const timeoutMs = Number.isInteger(options.timeoutMs) ? options.timeoutMs : 15000
  const context = createSeedingContext(backend)
  const seeded = await seedMixedSessionState(context)
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
  let stage = 'awaiting-pre-world-memory'
  let exitSent = false
  let preExecutionContext = null
  let executionResult = null
  let postExecutionContext = null
  let requestsSent = 0
  let handoffsSent = 0

  function sendWorldMemoryRequest() {
    child.stdin.write(`${JSON.stringify(seeded.request)}\n`)
    requestsSent += 1
  }

  function sendExecutionHandoff() {
    child.stdin.write(`${JSON.stringify(seeded.handoff)}\n`)
    handoffsSent += 1
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
    if (!parsed) {
      return
    }

    if (parsed.type === 'world-memory-context.v1') {
      if (stage === 'awaiting-pre-world-memory') {
        preExecutionContext = parsed
        stage = 'awaiting-execution-result'
        sendExecutionHandoff()
        return
      }
      if (stage === 'awaiting-post-world-memory') {
        postExecutionContext = parsed
        stage = 'complete'
        sendExit()
      }
      return
    }

    if (parsed.type === 'execution-result.v1' && stage === 'awaiting-execution-result') {
      executionResult = parsed
      stage = 'awaiting-post-world-memory'
      sendWorldMemoryRequest()
    }
  }

  child.stdout.on('data', (chunk) => {
    const text = String(chunk)
    stdoutBuffer += text
    if (!started && stdoutBuffer.includes('--- WORLD ONLINE ---')) {
      started = true
      sendWorldMemoryRequest()
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
  assert.equal(requestsSent, 2, 'expected exactly two canonical world-memory requests')
  assert.equal(handoffsSent, 1, 'expected exactly one canonical execution handoff')
  assert(preExecutionContext, 'expected first canonical world-memory-context.v1 response')
  assert(executionResult, 'expected canonical execution-result.v1 response')
  assert(postExecutionContext, 'expected second canonical world-memory-context.v1 response')

  assertCanonicalWorldMemoryContext(preExecutionContext, seeded.request)
  assertCanonicalWorldMemoryContext(postExecutionContext, seeded.request)

  assert.equal(executionResult.type, 'execution-result.v1')
  assert.equal(executionResult.schemaVersion, 1)
  assert.equal(isValidExecutionResult(executionResult), true)
  assert.equal(executionResult.handoffId, seeded.handoff.handoffId)
  assert.equal(executionResult.proposalId, seeded.handoff.proposalId)
  assert.equal(executionResult.idempotencyKey, seeded.handoff.idempotencyKey)
  assert.equal(executionResult.status, 'executed')
  assert.equal(executionResult.accepted, true)
  assert.equal(executionResult.executed, true)
  assert.equal(executionResult.reasonCode, 'EXECUTED')
  assert.equal(executionResult.command, `project advance alpha ${seeded.projectId}`)
  assert.deepEqual(executionResult.authorityCommands, [`project advance alpha ${seeded.projectId}`])
  assert.match(executionResult.snapshotHash, /^[0-9a-f]{64}$/)
  assert.match(executionResult.evaluation.staleCheck.actualSnapshotHash, /^[0-9a-f]{64}$/)
  assert.match(executionResult.worldState.postExecutionSnapshotHash, /^[0-9a-f]{64}$/)

  assert.equal(preExecutionContext.townSummary.townId, 'alpha')
  assert.equal(preExecutionContext.factionSummary.factionId, 'iron_pact')
  assert.equal(preExecutionContext.recentChronicle.length, 2)
  assert.equal(preExecutionContext.recentHistory.length, 0)
  assert.equal(preExecutionContext.townSummary.historyCount, 0)
  assert.equal(preExecutionContext.townSummary.executionCounts.executed, 0)
  assert.equal(preExecutionContext.townSummary.activeProjectCount, 1)
  assert.equal(preExecutionContext.factionSummary.historyCount, 0)

  assert.equal(postExecutionContext.townSummary.townId, 'alpha')
  assert.equal(postExecutionContext.factionSummary.factionId, 'iron_pact')
  assert(postExecutionContext.recentHistory.length >= 1)
  assert(
    postExecutionContext.recentHistory.some((entry) => (
      entry.handoffId === seeded.handoff.handoffId
      && entry.proposalType === 'PROJECT_ADVANCE'
      && entry.status === 'executed'
      && Array.isArray(entry.authorityCommands)
      && entry.authorityCommands.includes(`project advance alpha ${seeded.projectId}`)
    )),
    'post-execution history should include the executed project advance'
  )
  assert(postExecutionContext.townSummary.historyCount > preExecutionContext.townSummary.historyCount)
  assert.equal(postExecutionContext.townSummary.executionCounts.executed, 1)
  assert(postExecutionContext.factionSummary.historyCount > preExecutionContext.factionSummary.historyCount)
  assert.notDeepEqual(
    pickStableWorldMemoryContext(postExecutionContext),
    pickStableWorldMemoryContext(preExecutionContext),
    'post-execution world memory should reflect the authoritative state change'
  )

  const reloaded = createReloadedExecutionStore(context)
  const receipt = reloaded.executionStore.findReceipt({
    handoffId: seeded.handoff.handoffId,
    idempotencyKey: seeded.handoff.idempotencyKey
  })
  const pendingExecutions = reloaded.executionStore.listPendingExecutions()
  const executedProject = reloaded.memoryStore.getSnapshot().world.projects.find((entry) => entry.id === seeded.projectId)

  assert(receipt, 'expected durable execution receipt after mixed live session')
  assert.equal(receipt.executionId, executionResult.executionId)
  assert.equal(receipt.status, executionResult.status)
  assert.equal(pendingExecutions.length, 0, 'pending execution markers should be cleared after success')
  assert(executedProject, 'expected seeded project to exist after mixed live session')
  assert.equal(executedProject.stage, 2)

  return {
    backend,
    request: seeded.request,
    handoff: seeded.handoff,
    preExecutionContext,
    executionResult,
    postExecutionContext,
    receipt,
    pendingExecutions,
    stdoutLines,
    stderrLines,
    projectStage: executedProject.stage,
    responsesCapturedLiveFromSameChildProcess: true
  }
}

async function runLiveMixedSessionCheck(options = {}) {
  const firstRun = await runSingleLiveMixedSession(options)
  const secondRun = await runSingleLiveMixedSession(options)

  assert.deepEqual(
    pickStableMixedProjection(secondRun),
    pickStableMixedProjection(firstRun),
    'same seeded mixed live session should yield deterministic retrieval/execution semantics'
  )

  return {
    ...firstRun,
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
  runLiveMixedSessionCheck
}

if (require.main === module) {
  runLiveMixedSessionCheck(parseCliOptions(process.argv.slice(2)))
    .then((result) => {
      process.stdout.write('Live mixed-session check passed. Captured retrieval, execution, and retrieval from the same engine child process.\n')
      process.stdout.write(`${JSON.stringify({
        preExecutionContext: result.preExecutionContext,
        executionResult: result.executionResult,
        postExecutionContext: result.postExecutionContext
      })}\n`)
    })
    .catch((error) => {
      process.stderr.write(`${error instanceof Error ? error.stack || error.message : String(error)}\n`)
      process.exitCode = 1
    })
}
