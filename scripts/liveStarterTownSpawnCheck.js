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
const FIXED_NOW_ISO = '2026-02-26T00:00:00.000Z'

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
  actorId = 'ops',
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
      reason: 'Live starter town spawn validation.',
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
  const { dir, memoryPath, sqlitePath } = createTempPaths(`mvp-live-starter-town-spawn-${backend}-`)
  const now = fixedNowFactory()
  const logger = createSilentLogger('live_starter_spawn_seed')
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

async function seedStarterTownState(context) {
  const agents = createAgents()
  await context.memoryStore.transact((memory) => {
    memory.world.clock.updated_at = FIXED_NOW_ISO
  }, { eventId: 'live-starter-town-spawn:seed-clock' })

  await context.godCommandService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'live-starter-town-spawn:seed-town-alpha'
  })

  return {
    request: createWorldMemoryRequest({
      townId: 'alpha',
      chronicleLimit: 4,
      historyLimit: 4
    }),
    setSpawnHandoff: createHandoff({
      proposalType: 'TOWN_SET_SPAWN',
      townId: 'alpha',
      actorId: 'ops',
      command: 'town spawn set alpha overworld 8 80 -6 90 0 4 starter_hub',
      args: {
        townId: 'alpha',
        spawn: {
          dimension: 'overworld',
          x: 8,
          y: 80,
          z: -6,
          yaw: 90,
          pitch: 0,
          radius: 4,
          kind: 'starter_hub'
        }
      },
      snapshotHash: snapshotHashForStore(context.memoryStore)
    })
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
  if (!trimmed.startsWith('{')) return null
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
      reject(new Error(`Live starter town spawn check timed out after ${timeoutMs}ms`))
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
  const logger = createSilentLogger('live_starter_spawn_verify')
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
  assert(response.townIdentity)
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

function pickStableStarterSpawnProjection(result) {
  return {
    request: result.request,
    setSpawnResult: pickStableExecutionResult(result.setSpawnResult),
    assignResult: pickStableExecutionResult(result.assignResult),
    worldMemoryContext: pickStableWorldMemoryContext(result.worldMemoryContext),
    spawnResult: pickStableExecutionResult(result.spawnResult),
    playerRecord: result.playerRecord,
    townSpawn: result.townSpawn
  }
}

async function runSingleLiveStarterTownSpawn(options = {}) {
  const backend = options.backend === 'sqlite' ? 'sqlite' : 'memory'
  const timeoutMs = Number.isInteger(options.timeoutMs) ? options.timeoutMs : 15000
  const context = createSeedingContext(backend)
  const seeded = await seedStarterTownState(context)
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
  let stage = 'awaiting-set-spawn-result'
  let setSpawnResult = null
  let assignResult = null
  let worldMemoryContext = null
  let spawnResult = null
  let requestsSent = 0
  let handoffsSent = 0

  function sendExecutionHandoff(handoff) {
    child.stdin.write(`${JSON.stringify(handoff)}\n`)
    handoffsSent += 1
  }

  function sendWorldMemoryRequest() {
    child.stdin.write(`${JSON.stringify(seeded.request)}\n`)
    requestsSent += 1
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
    if (!parsed) return

    if (parsed.type === 'world-memory-context.v1') {
      if (stage === 'awaiting-world-memory') {
        worldMemoryContext = parsed
        stage = 'awaiting-spawn-result'
        sendExecutionHandoff(createHandoff({
          proposalType: 'PLAYER_GET_SPAWN',
          actorId: 'Builder01',
          townId: 'auto',
          command: 'player spawn Builder01',
          args: {
            playerId: 'Builder01'
          },
          snapshotHash: assignResult.worldState.postExecutionSnapshotHash,
          decisionEpoch: assignResult.worldState.postExecutionDecisionEpoch ?? assignResult.decisionEpoch
        }))
      }
      return
    }

    if (parsed.type !== 'execution-result.v1') {
      return
    }

    if (stage === 'awaiting-set-spawn-result') {
      setSpawnResult = parsed
      stage = 'awaiting-assign-result'
      sendExecutionHandoff(createHandoff({
        proposalType: 'PLAYER_ASSIGN_TOWN',
        actorId: 'ops',
        townId: 'alpha',
        command: 'player assign Builder01 alpha',
        args: {
          playerId: 'Builder01',
          townId: 'alpha'
        },
        snapshotHash: setSpawnResult.worldState.postExecutionSnapshotHash,
        decisionEpoch: setSpawnResult.worldState.postExecutionDecisionEpoch ?? setSpawnResult.decisionEpoch
      }))
      return
    }
    if (stage === 'awaiting-assign-result') {
      assignResult = parsed
      stage = 'awaiting-world-memory'
      sendWorldMemoryRequest()
      return
    }
    if (stage === 'awaiting-spawn-result') {
      spawnResult = parsed
      stage = 'complete'
      sendExit()
    }
  }

  child.stdout.on('data', (chunk) => {
    const text = String(chunk)
    stdoutBuffer += text
    if (!started && stdoutBuffer.includes('--- WORLD ONLINE ---')) {
      started = true
      sendExecutionHandoff(seeded.setSpawnHandoff)
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
  assert.equal(handoffsSent, 3, 'expected three canonical spawn-related execution handoffs')
  assert.equal(requestsSent, 1, 'expected one canonical world-memory request')
  assert(setSpawnResult, 'expected set-spawn execution result')
  assert(assignResult, 'expected player-assignment execution result')
  assert(worldMemoryContext, 'expected canonical world-memory response')
  assert(spawnResult, 'expected player-spawn execution result')

  assert.equal(isValidExecutionResult(setSpawnResult), true)
  assert.equal(isValidExecutionResult(assignResult), true)
  assert.equal(isValidExecutionResult(spawnResult), true)
  assert.equal(setSpawnResult.status, 'executed')
  assert.equal(assignResult.status, 'executed')
  assert.equal(spawnResult.status, 'executed')
  assert.deepEqual(setSpawnResult.authorityCommands, ['town spawn set alpha overworld 8 80 -6 90 0 4 starter_hub'])
  assert.deepEqual(assignResult.authorityCommands, ['player assign Builder01 alpha'])
  assert.deepEqual(spawnResult.authorityCommands, ['player spawn Builder01'])
  assert.equal(assignResult.townId, 'alpha')
  assert.equal(spawnResult.townId, 'alpha')
  assert.equal(spawnResult.worldState.postExecutionSnapshotHash, assignResult.worldState.postExecutionSnapshotHash)
  assert.deepEqual(spawnResult.embodiment, {
    backendHint: 'bridge',
    actions: [
      {
        type: 'teleport',
        target: {
          kind: 'player',
          id: 'Builder01'
        },
        dimension: 'overworld',
        x: 8,
        y: 80,
        z: -6,
        yaw: 90,
        pitch: 0,
        meta: {
          townId: 'alpha',
          source: 'configured',
          assigned: true,
          radius: 4,
          kind: 'starter_hub'
        }
      }
    ]
  })

  assertCanonicalWorldMemoryContext(worldMemoryContext, seeded.request)
  assert.equal(worldMemoryContext.townSummary.townId, 'alpha')
  assert.equal(worldMemoryContext.townIdentity.townId, 'alpha')
  assert(worldMemoryContext.townSummary.chronicleCount >= 1)

  const reloaded = createReloadedExecutionStore(context)
  const setSpawnReceipt = reloaded.executionStore.findReceipt({
    handoffId: setSpawnResult.handoffId,
    idempotencyKey: setSpawnResult.idempotencyKey
  })
  const assignReceipt = reloaded.executionStore.findReceipt({
    handoffId: assignResult.handoffId,
    idempotencyKey: assignResult.idempotencyKey
  })
  const spawnReceipt = reloaded.executionStore.findReceipt({
    handoffId: spawnResult.handoffId,
    idempotencyKey: spawnResult.idempotencyKey
  })
  const pendingExecutions = reloaded.executionStore.listPendingExecutions()
  const playerRecord = reloaded.memoryStore.getSnapshot().world.players.Builder01
  const townSpawn = reloaded.memoryStore.getSnapshot().world.towns.alpha.spawn

  assert(setSpawnReceipt, 'expected durable receipt for town spawn set')
  assert(assignReceipt, 'expected durable receipt for player assignment')
  assert(spawnReceipt, 'expected durable receipt for player spawn retrieval')
  assert.equal(pendingExecutions.length, 0, 'pending execution markers should be cleared after spawn session success')
  assert.deepEqual(playerRecord, {
    playerId: 'Builder01',
    townId: 'alpha',
    assignedAtDay: 1,
    spawnPolicy: 'explicit_town'
  })
  assert.deepEqual(townSpawn, {
    dimension: 'overworld',
    x: 8,
    y: 80,
    z: -6,
    yaw: 90,
    pitch: 0,
    radius: 4,
    kind: 'starter_hub'
  })

  return {
    backend,
    request: seeded.request,
    setSpawnResult,
    assignResult,
    worldMemoryContext,
    spawnResult,
    playerRecord,
    townSpawn,
    stdoutLines,
    stderrLines,
    responsesCapturedLiveFromSameChildProcess: true
  }
}

async function runLiveStarterTownSpawnCheck(options = {}) {
  const firstRun = await runSingleLiveStarterTownSpawn(options)
  const secondRun = await runSingleLiveStarterTownSpawn(options)

  assert.deepEqual(
    pickStableStarterSpawnProjection(secondRun),
    pickStableStarterSpawnProjection(firstRun),
    'same seeded live starter-town spawn session should yield deterministic spawn selection semantics'
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

if (require.main === module) {
  runLiveStarterTownSpawnCheck(parseCliOptions(process.argv.slice(2)))
    .then((result) => {
      process.stdout.write('Live starter town spawn check passed. Captured spawn assignment and teleport embodiment from the same engine child process.\n')
      process.stdout.write(`${JSON.stringify({
        setSpawnResult: pickStableExecutionResult(result.setSpawnResult),
        assignResult: pickStableExecutionResult(result.assignResult),
        worldMemoryContext: pickStableWorldMemoryContext(result.worldMemoryContext),
        spawnResult: pickStableExecutionResult(result.spawnResult),
        playerRecord: result.playerRecord,
        townSpawn: result.townSpawn
      })}\n`)
    })
    .catch((error) => {
      process.stderr.write(`${error && error.stack ? error.stack : String(error)}\n`)
      process.exitCode = 1
    })
}

module.exports = {
  runLiveStarterTownSpawnCheck
}
