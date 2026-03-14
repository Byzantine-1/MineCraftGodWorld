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
      reason: 'Live major mission session validation.',
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
  const { dir, memoryPath, sqlitePath } = createTempPaths(`mvp-live-mission-session-${backend}-`)
  const now = fixedNowFactory()
  const logger = createSilentLogger('live_mission_session_seed')
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

async function seedMissionSessionState(context) {
  const agents = createAgents()

  await context.memoryStore.transact((memory) => {
    memory.world.clock.updated_at = FIXED_NOW_ISO
    memory.world.factions.iron_pact = {
      name: 'iron_pact',
      towns: ['alpha'],
      doctrine: 'Order through steel.',
      rivals: ['veil_church'],
      hostilityToPlayer: 22,
      stability: 74
    }
  }, { eventId: 'live-mission-session:seed-world' })

  await context.godCommandService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'live-mission-session:seed-town-alpha'
  })

  const request = createWorldMemoryRequest({
    townId: 'alpha',
    factionId: 'iron_pact',
    chronicleLimit: 4,
    historyLimit: 5
  })
  const acceptHandoff = createHandoff({
    proposalType: 'MAYOR_ACCEPT_MISSION',
    command: 'mission accept alpha sq-side-1',
    args: { missionId: 'sq-side-1' },
    snapshotHash: snapshotHashForStore(context.memoryStore),
    preconditions: [{ kind: 'mission_absent' }]
  })

  return {
    request,
    acceptHandoff
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
      reject(new Error(`Live mission session check timed out after ${timeoutMs}ms`))
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
  const logger = createSilentLogger('live_mission_session_verify')
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
    factionSummary: context.factionSummary ?? null,
    townIdentity: context.townIdentity ?? null,
    keyActors: context.keyActors ?? null
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

function pickStableMissionProjection(result) {
  return {
    request: result.request,
    preExecutionContext: pickStableWorldMemoryContext(result.preExecutionContext),
    acceptResult: pickStableExecutionResult(result.acceptResult),
    postAcceptContext: pickStableWorldMemoryContext(result.postAcceptContext),
    advanceResult: pickStableExecutionResult(result.advanceResult),
    postAdvanceContext: pickStableWorldMemoryContext(result.postAdvanceContext),
    completeResult: pickStableExecutionResult(result.completeResult),
    postCompleteContext: pickStableWorldMemoryContext(result.postCompleteContext),
    activeMissionId: result.activeMissionId,
    completedMissionStatus: result.completedMissionStatus
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
  assert(response.townIdentity)
  assert(Array.isArray(response.keyActors))
}

async function runSingleLiveMissionSession(options = {}) {
  const backend = options.backend === 'sqlite' ? 'sqlite' : 'memory'
  const timeoutMs = Number.isInteger(options.timeoutMs) ? options.timeoutMs : 15000
  const context = createSeedingContext(backend)
  const seeded = await seedMissionSessionState(context)
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
  let exitSent = false
  let stage = 'awaiting-pre-world-memory'
  let preExecutionContext = null
  let acceptResult = null
  let postAcceptContext = null
  let advanceResult = null
  let postAdvanceContext = null
  let completeResult = null
  let postCompleteContext = null
  let requestsSent = 0
  let handoffsSent = 0

  function sendWorldMemoryRequest() {
    child.stdin.write(`${JSON.stringify(seeded.request)}\n`)
    requestsSent += 1
  }

  function sendExecutionHandoff(handoff) {
    child.stdin.write(`${JSON.stringify(handoff)}\n`)
    handoffsSent += 1
  }

  function sendExit() {
    if (exitSent) return
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
        stage = 'awaiting-accept-result'
        sendExecutionHandoff(seeded.acceptHandoff)
        return
      }
      if (stage === 'awaiting-post-accept-world-memory') {
        postAcceptContext = parsed
        stage = 'awaiting-advance-result'
        sendExecutionHandoff(createHandoff({
          proposalType: 'MISSION_ADVANCE',
          command: 'mission advance alpha',
          args: { missionId: postAcceptContext.townSummary.activeMajorMissionId },
          snapshotHash: acceptResult.worldState.postExecutionSnapshotHash,
          decisionEpoch: acceptResult.worldState.postExecutionDecisionEpoch ?? acceptResult.decisionEpoch
        }))
        return
      }
      if (stage === 'awaiting-post-advance-world-memory') {
        postAdvanceContext = parsed
        stage = 'awaiting-complete-result'
        sendExecutionHandoff(createHandoff({
          proposalType: 'MISSION_COMPLETE',
          command: 'mission complete alpha',
          args: { missionId: postAdvanceContext.townSummary.activeMajorMissionId },
          snapshotHash: advanceResult.worldState.postExecutionSnapshotHash,
          decisionEpoch: advanceResult.worldState.postExecutionDecisionEpoch ?? advanceResult.decisionEpoch
        }))
        return
      }
      if (stage === 'awaiting-post-complete-world-memory') {
        postCompleteContext = parsed
        stage = 'complete'
        sendExit()
      }
      return
    }

    if (parsed.type !== 'execution-result.v1') {
      return
    }

    if (stage === 'awaiting-accept-result') {
      acceptResult = parsed
      stage = 'awaiting-post-accept-world-memory'
      sendWorldMemoryRequest()
      return
    }
    if (stage === 'awaiting-advance-result') {
      advanceResult = parsed
      stage = 'awaiting-post-advance-world-memory'
      sendWorldMemoryRequest()
      return
    }
    if (stage === 'awaiting-complete-result') {
      completeResult = parsed
      stage = 'awaiting-post-complete-world-memory'
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
  assert.equal(requestsSent, 4, 'expected four canonical world-memory requests')
  assert.equal(handoffsSent, 3, 'expected three canonical mission execution handoffs')
  assert(preExecutionContext, 'expected pre-mission world-memory-context.v1 response')
  assert(acceptResult, 'expected canonical execution-result.v1 for mission accept')
  assert(postAcceptContext, 'expected post-accept world-memory-context.v1 response')
  assert(advanceResult, 'expected canonical execution-result.v1 for mission advance')
  assert(postAdvanceContext, 'expected post-advance world-memory-context.v1 response')
  assert(completeResult, 'expected canonical execution-result.v1 for mission complete')
  assert(postCompleteContext, 'expected post-complete world-memory-context.v1 response')

  assertCanonicalWorldMemoryContext(preExecutionContext, seeded.request)
  assertCanonicalWorldMemoryContext(postAcceptContext, seeded.request)
  assertCanonicalWorldMemoryContext(postAdvanceContext, seeded.request)
  assertCanonicalWorldMemoryContext(postCompleteContext, seeded.request)

  assert.equal(isValidExecutionResult(acceptResult), true)
  assert.equal(isValidExecutionResult(advanceResult), true)
  assert.equal(isValidExecutionResult(completeResult), true)
  assert.equal(acceptResult.status, 'executed')
  assert.equal(advanceResult.status, 'executed')
  assert.equal(completeResult.status, 'executed')
  assert.deepEqual(acceptResult.authorityCommands, ['mayor talk alpha', 'mayor accept alpha'])
  assert.deepEqual(advanceResult.authorityCommands, ['mission advance alpha'])
  assert.deepEqual(completeResult.authorityCommands, ['mission complete alpha'])

  assert.equal(preExecutionContext.townSummary.activeMajorMissionId, null)
  assert.equal(preExecutionContext.townSummary.historyCount, 0)
  assert.equal(preExecutionContext.townSummary.hope, 50)
  assert.equal(preExecutionContext.townSummary.dread, 50)

  assert(postAcceptContext.townSummary.activeMajorMissionId, 'expected accepted mission id in world-memory town summary')
  assert(postAcceptContext.townSummary.historyCount > preExecutionContext.townSummary.historyCount)
  assert(postAcceptContext.recentHistory.some((entry) => entry.proposalType === 'MAYOR_ACCEPT_MISSION' && entry.status === 'executed'))
  assert.equal(postAcceptContext.townSummary.hope, preExecutionContext.townSummary.hope)
  assert.equal(postAcceptContext.townSummary.dread, preExecutionContext.townSummary.dread)

  assert.equal(postAdvanceContext.townSummary.activeMajorMissionId, postAcceptContext.townSummary.activeMajorMissionId)
  assert(postAdvanceContext.townSummary.historyCount > postAcceptContext.townSummary.historyCount)
  assert(postAdvanceContext.recentHistory.some((entry) => entry.proposalType === 'MISSION_ADVANCE' && entry.status === 'executed'))

  assert.equal(postCompleteContext.townSummary.activeMajorMissionId, null)
  assert(postCompleteContext.townSummary.historyCount > postAdvanceContext.townSummary.historyCount)
  assert(postCompleteContext.recentHistory.some((entry) => entry.proposalType === 'MISSION_COMPLETE' && entry.status === 'executed'))
  assert(postCompleteContext.townSummary.hope > preExecutionContext.townSummary.hope)
  assert(postCompleteContext.townSummary.dread < preExecutionContext.townSummary.dread)
  assert(postCompleteContext.townSummary.recentImpactCount >= postAcceptContext.townSummary.recentImpactCount)
  assert(postCompleteContext.townSummary.crierQueueDepth >= postAcceptContext.townSummary.crierQueueDepth)

  const reloaded = createReloadedExecutionStore(context)
  const acceptReceipt = reloaded.executionStore.findReceipt({
    handoffId: acceptResult.handoffId,
    idempotencyKey: acceptResult.idempotencyKey
  })
  const advanceReceipt = reloaded.executionStore.findReceipt({
    handoffId: advanceResult.handoffId,
    idempotencyKey: advanceResult.idempotencyKey
  })
  const completeReceipt = reloaded.executionStore.findReceipt({
    handoffId: completeResult.handoffId,
    idempotencyKey: completeResult.idempotencyKey
  })
  const pendingExecutions = reloaded.executionStore.listPendingExecutions()
  const completedMission = reloaded.memoryStore.getSnapshot().world.majorMissions.find((entry) => entry.id === postAcceptContext.townSummary.activeMajorMissionId)

  assert(acceptReceipt, 'expected durable receipt for mission accept')
  assert(advanceReceipt, 'expected durable receipt for mission advance')
  assert(completeReceipt, 'expected durable receipt for mission complete')
  assert.equal(pendingExecutions.length, 0, 'pending execution markers should be cleared after mission session success')
  assert(completedMission, 'expected a completed major mission after the live mission session')
  assert.equal(completedMission.status, 'completed')

  return {
    backend,
    request: seeded.request,
    acceptHandoff: seeded.acceptHandoff,
    preExecutionContext,
    acceptResult,
    postAcceptContext,
    advanceResult,
    postAdvanceContext,
    completeResult,
    postCompleteContext,
    acceptReceipt,
    advanceReceipt,
    completeReceipt,
    pendingExecutions,
    stdoutLines,
    stderrLines,
    activeMissionId: postAcceptContext.townSummary.activeMajorMissionId,
    completedMissionStatus: completedMission.status,
    responsesCapturedLiveFromSameChildProcess: true
  }
}

async function runLiveMissionSessionCheck(options = {}) {
  const firstRun = await runSingleLiveMissionSession(options)
  const secondRun = await runSingleLiveMissionSession(options)

  assert.deepEqual(
    pickStableMissionProjection(secondRun),
    pickStableMissionProjection(firstRun),
    'same seeded live mission session should yield deterministic mission retrieval/execution semantics'
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
  runLiveMissionSessionCheck
}

if (require.main === module) {
  runLiveMissionSessionCheck(parseCliOptions(process.argv.slice(2)))
    .then((result) => {
      process.stdout.write('Live mission session check passed. Captured accept, advance, complete, and retrieval from the same engine child process.\n')
      process.stdout.write(`${JSON.stringify({
        preExecutionContext: result.preExecutionContext,
        postAcceptContext: result.postAcceptContext,
        postAdvanceContext: result.postAdvanceContext,
        postCompleteContext: result.postCompleteContext
      })}\n`)
    })
    .catch((error) => {
      process.stderr.write(`${error instanceof Error ? error.stack || error.message : String(error)}\n`)
      process.exitCode = 1
    })
}
