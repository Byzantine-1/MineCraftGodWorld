const crypto = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')
const { execFileSync } = require('child_process')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createExecutionAdapter, isValidExecutionResult } = require('../src/executionAdapter')
const {
  createExecutionStore,
  createMemoryExecutionPersistence,
  createSqliteExecutionPersistence
} = require('../src/executionStore')
const { createGodCommandService } = require('../src/godCommands')
const { createMemoryStore } = require('../src/memory')
const { createAuthoritativeSnapshotProjection } = require('../src/worldSnapshotProjection')

function createTempPaths(prefix) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix))
  return {
    dir,
    memoryPath: path.join(dir, 'memory.json'),
    sqlitePath: path.join(dir, 'execution.sqlite3')
  }
}

function fixedNowFactory() {
  return () => Date.parse('2026-02-22T00:00:00.000Z')
}

function sqliteJson(dbPath, sql) {
  const stdout = execFileSync('sqlite3', ['-json', dbPath, sql], {
    encoding: 'utf8',
    windowsHide: true
  })
  return stdout.trim() ? JSON.parse(stdout) : []
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
      reason: 'SQLite backend test.',
      reasonTags: ['test'],
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

async function seedProject(service, memoryStore, agents) {
  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'sqlite-backend-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'project start alpha lantern_line',
    operationId: 'sqlite-backend-seed-project'
  })
  return memoryStore.getSnapshot().world.projects[0].id
}

function createStoreAndAdapter({ memoryPath, sqlitePath, backendName }) {
  const now = fixedNowFactory()
  const memoryStore = createMemoryStore({ filePath: memoryPath, now })
  const persistenceBackend = backendName === 'sqlite'
    ? createSqliteExecutionPersistence({ dbPath: sqlitePath, now })
    : createMemoryExecutionPersistence({ memoryStore })
  const executionStore = createExecutionStore({
    memoryStore,
    persistenceBackend
  })
  const godCommandService = createGodCommandService({ memoryStore })
  const executionAdapter = createExecutionAdapter({
    memoryStore,
    godCommandService,
    executionStore
  })
  return {
    executionAdapter,
    executionStore,
    godCommandService,
    memoryStore
  }
}

test('sqlite backend initializes schema and persists receipts for lookup', async () => {
  const { memoryPath, sqlitePath } = createTempPaths('mvp-sqlite-store-')
  const now = fixedNowFactory()
  const memoryStore = createMemoryStore({ filePath: memoryPath, now })
  const persistenceBackend = createSqliteExecutionPersistence({ dbPath: sqlitePath, now })
  const executionStore = createExecutionStore({
    memoryStore,
    persistenceBackend
  })

  const pending = await executionStore.stagePendingExecution({
    handoff: {
      handoffId: 'handoff_a'.padEnd(72, 'a'),
      proposalId: 'proposal_b'.padEnd(73, 'b'),
      idempotencyKey: 'proposal_b'.padEnd(73, 'b'),
      command: 'project advance alpha pr_1'
    },
    proposalType: 'PROJECT_ADVANCE',
    actorId: 'mara',
    townId: 'alpha',
    authorityCommands: ['project advance alpha pr_1'],
    beforeProjection: { snapshotHash: 'c'.repeat(64), decisionEpoch: 1 }
  })

  const fakeResult = {
    type: 'execution-result.v1',
    schemaVersion: 1,
    executionId: `result_${'d'.repeat(64)}`,
    resultId: `result_${'d'.repeat(64)}`,
    handoffId: pending.handoffId,
    proposalId: pending.proposalId,
    idempotencyKey: pending.idempotencyKey,
    snapshotHash: 'c'.repeat(64),
    decisionEpoch: 1,
    actorId: 'mara',
    townId: 'alpha',
    proposalType: 'PROJECT_ADVANCE',
    command: 'project advance alpha pr_1',
    authorityCommands: ['project advance alpha pr_1'],
    status: 'executed',
    accepted: true,
    executed: true,
    reasonCode: 'EXECUTED',
    evaluation: {
      preconditions: { evaluated: true, passed: true, failures: [] },
      staleCheck: {
        evaluated: true,
        passed: true,
        actualSnapshotHash: 'c'.repeat(64),
        actualDecisionEpoch: 1
      },
      duplicateCheck: { evaluated: true, duplicate: false, duplicateOf: null }
    },
    worldState: {
      postExecutionSnapshotHash: 'e'.repeat(64),
      postExecutionDecisionEpoch: 1
    }
  }

  await executionStore.recordResult(fakeResult)

  assert.equal(fs.existsSync(sqlitePath), true)
  assert.equal(executionStore.findReceipt({
    handoffId: fakeResult.handoffId,
    idempotencyKey: fakeResult.idempotencyKey
  }).executionId, fakeResult.executionId)

  const tables = sqliteJson(sqlitePath, `
    SELECT name
    FROM sqlite_master
    WHERE type = 'table' AND name IN (
      'execution_receipts',
      'execution_pending',
      'execution_event_ledger',
      'execution_meta',
      'world_state_snapshots',
      'world_towns',
      'world_actors',
      'world_meta',
      'world_chronicle_records'
    )
    ORDER BY name;
  `)

  assert.deepEqual(tables.map((row) => row.name), [
    'execution_event_ledger',
    'execution_meta',
    'execution_pending',
    'execution_receipts',
    'world_actors',
    'world_chronicle_records',
    'world_meta',
    'world_state_snapshots',
    'world_towns'
  ])
  assert.equal(executionStore.findPendingExecution({
    handoffId: fakeResult.handoffId,
    idempotencyKey: fakeResult.idempotencyKey
  }), null)
})

test('sqlite world-state bootstrap migrates authoritative towns/actors from memory on first init', async () => {
  const { memoryPath, sqlitePath } = createTempPaths('mvp-sqlite-world-bootstrap-')
  const now = fixedNowFactory()
  const memoryStore = createMemoryStore({ filePath: memoryPath, now })

  await memoryStore.transact((memory) => {
    memory.world.towns.alpha = {
      ...memory.world.towns.alpha,
      townId: 'alpha',
      name: 'Alpha',
      status: 'active',
      region: 'north',
      tags: ['harbor']
    }
    memory.world.towns.beta = {
      ...memory.world.towns.beta,
      townId: 'beta',
      name: 'Beta',
      status: 'distressed',
      region: 'south',
      tags: ['frontier']
    }
    memory.world.actors = {
      ...memory.world.actors,
      'alpha.mayor': {
        actorId: 'alpha.mayor',
        townId: 'alpha',
        name: 'Mayor Elara',
        role: 'mayor',
        status: 'active'
      },
      'beta.captain': {
        actorId: 'beta.captain',
        townId: 'beta',
        name: 'Captain Rook',
        role: 'captain',
        status: 'active'
      },
      'beta.warden': {
        actorId: 'beta.warden',
        townId: 'beta',
        name: 'Warden Hale',
        role: 'warden',
        status: 'active'
      }
    }
  }, { eventId: 'sqlite-world-bootstrap-seed' })

  const executionStore = createExecutionStore({
    memoryStore,
    persistenceBackend: createSqliteExecutionPersistence({ dbPath: sqlitePath, now })
  })

  const towns = executionStore.listTowns()
  const betaOfficeholders = executionStore.listOfficeholders('beta')
  const migrationMeta = executionStore.getWorldStateMigrationMeta()
  const sqliteTownRows = sqliteJson(sqlitePath, `
    SELECT town_id, name, status
    FROM world_towns
    ORDER BY town_id ASC;
  `)
  const sqliteActorRows = sqliteJson(sqlitePath, `
    SELECT actor_id, town_id, role
    FROM world_actors
    ORDER BY actor_id ASC;
  `)

  assert.equal(executionStore.worldStateBackendName, 'sqlite')
  assert.deepEqual(towns.map((town) => town.townId), ['alpha', 'beta'])
  assert.deepEqual(betaOfficeholders.map((actor) => actor.role), ['mayor', 'captain', 'warden'])
  assert.equal(migrationMeta?.value, 'imported_from_memory')
  assert.deepEqual(sqliteTownRows.map((row) => row.town_id), ['alpha', 'beta'])
  assert(sqliteActorRows.some((row) => row.actor_id === 'alpha.mayor'))
  assert(sqliteActorRows.some((row) => row.actor_id === 'beta.captain'))
  assert(sqliteActorRows.some((row) => row.actor_id === 'beta.warden'))
})

test('sqlite world-state bootstrap tolerates large snapshots without command-line overflow', async () => {
  const { memoryPath, sqlitePath } = createTempPaths('mvp-sqlite-large-bootstrap-')
  const now = fixedNowFactory()
  const memoryStore = createMemoryStore({ filePath: memoryPath, now })

  await memoryStore.transact((memory) => {
    memory.world.debugBlob = 'x'.repeat(50000)
    memory.world.chronicle = Array.from({ length: 6 }, (_, index) => ({
      id: `large-${index}`,
      type: 'town-update',
      town: 'alpha',
      at: index + 1,
      msg: `entry-${index}-` + 'y'.repeat(220)
    }))
  }, { eventId: 'sqlite-world-bootstrap-large-snapshot' })

  const executionStore = createExecutionStore({
    memoryStore,
    persistenceBackend: createSqliteExecutionPersistence({ dbPath: sqlitePath, now })
  })

  const snapshotRow = sqliteJson(sqlitePath, `
    SELECT LENGTH(payload_json) AS payload_length
    FROM world_state_snapshots
    ORDER BY snapshot_id DESC
    LIMIT 1;
  `)[0]

  assert.equal(executionStore.worldStateBackendName, 'sqlite')
  assert.equal(typeof memoryStore.getSnapshot().world.debugBlob, 'string')
  assert.equal(memoryStore.getSnapshot().world.debugBlob.length, 50000)
  assert(snapshotRow.payload_length > 50000)
})

test('sqlite world-memory sync reuses persisted chronicle projection when snapshot is unchanged', async () => {
  const { memoryPath, sqlitePath } = createTempPaths('mvp-sqlite-world-memory-cache-')
  const firstNow = () => Date.parse('2026-02-22T00:00:00.000Z')
  const secondNow = () => Date.parse('2026-02-22T01:00:00.000Z')

  const firstMemoryStore = createMemoryStore({ filePath: memoryPath, now: firstNow })
  const firstExecutionStore = createExecutionStore({
    memoryStore: firstMemoryStore,
    persistenceBackend: createSqliteExecutionPersistence({ dbPath: sqlitePath, now: firstNow })
  })

  await firstMemoryStore.transact((memory) => {
    memory.world.chronicle = [
      {
        id: 'chronicle-cache-01',
        type: 'mission',
        msg: 'Alpha posted a watch order.',
        at: 9000000000010,
        town: 'alpha',
        meta: { factionId: 'iron_pact' }
      }
    ]
  }, { eventId: 'sqlite-world-memory-cache-seed' })

  const firstRows = firstExecutionStore.listChronicleRecords({ townId: 'alpha', limit: 10 })
  const firstSyncMeta = sqliteJson(sqlitePath, `
    SELECT meta_key, meta_value
    FROM execution_meta
    WHERE meta_key IN ('world_memory.snapshot_hash', 'world_memory.decision_epoch')
    ORDER BY meta_key ASC;
  `)
  const firstTimestamp = sqliteJson(sqlitePath, `
    SELECT MAX(updated_at) AS updated_at
    FROM world_chronicle_records;
  `)[0].updated_at

  const secondMemoryStore = createMemoryStore({ filePath: memoryPath, now: secondNow })
  const secondExecutionStore = createExecutionStore({
    memoryStore: secondMemoryStore,
    persistenceBackend: createSqliteExecutionPersistence({ dbPath: sqlitePath, now: secondNow })
  })
  const secondRows = secondExecutionStore.listChronicleRecords({ townId: 'alpha', limit: 10 })
  const secondTimestamp = sqliteJson(sqlitePath, `
    SELECT MAX(updated_at) AS updated_at
    FROM world_chronicle_records;
  `)[0].updated_at

  assert.deepEqual(secondRows, firstRows)
  assert.equal(firstSyncMeta.length, 2)
  assert.equal(Number(secondTimestamp), Number(firstTimestamp))
})

test('sqlite authoritative world state preserves town spawn and player assignment across restart', async () => {
  const { memoryPath, sqlitePath } = createTempPaths('mvp-sqlite-starter-spawn-')
  const agents = createAgents()
  const first = createStoreAndAdapter({
    memoryPath,
    sqlitePath,
    backendName: 'sqlite'
  })

  await first.godCommandService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'sqlite-starter-spawn-seed-alpha'
  })
  await first.godCommandService.applyGodCommand({
    agents,
    command: 'town spawn set alpha overworld 5 81 -3 45 0 4 starter_hub',
    operationId: 'sqlite-starter-spawn-set-alpha'
  })
  await first.godCommandService.applyGodCommand({
    agents,
    command: 'player assign Builder01 alpha',
    operationId: 'sqlite-starter-spawn-assign-builder01'
  })

  const firstSnapshot = first.memoryStore.getSnapshot().world
  const townStateRow = sqliteJson(sqlitePath, `
    SELECT state_json
    FROM world_towns
    WHERE town_id = 'alpha'
    LIMIT 1;
  `)[0]
  const snapshotRow = sqliteJson(sqlitePath, `
    SELECT payload_json
    FROM world_state_snapshots
    ORDER BY snapshot_id DESC
    LIMIT 1;
  `)[0]
  const townState = JSON.parse(townStateRow.state_json)
  const worldSnapshot = JSON.parse(snapshotRow.payload_json)

  assert.deepEqual(firstSnapshot.towns.alpha.spawn, {
    dimension: 'overworld',
    x: 5,
    y: 81,
    z: -3,
    yaw: 45,
    pitch: 0,
    radius: 4,
    kind: 'starter_hub'
  })
  assert.deepEqual(firstSnapshot.players.Builder01, {
    playerId: 'Builder01',
    townId: 'alpha',
    assignedAtDay: 1,
    spawnPolicy: 'explicit_town'
  })
  assert.equal(townState.spawn.dimension, 'overworld')
  assert.equal(townState.spawn.x, 5)
  assert.equal(worldSnapshot.players.Builder01.townId, 'alpha')

  const restarted = createStoreAndAdapter({
    memoryPath,
    sqlitePath,
    backendName: 'sqlite'
  })

  assert.deepEqual(restarted.memoryStore.getSnapshot().world.towns.alpha.spawn, firstSnapshot.towns.alpha.spawn)
  assert.deepEqual(restarted.memoryStore.getSnapshot().world.players.Builder01, firstSnapshot.players.Builder01)
})

test('sqlite backend persists pending marker and recovery classification across restart', async () => {
  const { memoryPath, sqlitePath } = createTempPaths('mvp-sqlite-recovery-')
  const agents = createAgents()
  const first = createStoreAndAdapter({
    memoryPath,
    sqlitePath,
    backendName: 'sqlite'
  })
  const projectId = await seedProject(first.godCommandService, first.memoryStore, agents)
  const handoff = createHandoff({
    proposalType: 'PROJECT_ADVANCE',
    command: `project advance alpha ${projectId}`,
    args: { projectId },
    snapshotHash: snapshotHashForStore(first.memoryStore),
    preconditions: [{ kind: 'project_exists', targetId: projectId }]
  })

  const crashingAdapter = createExecutionAdapter({
    memoryStore: first.memoryStore,
    godCommandService: first.godCommandService,
    executionStore: first.executionStore,
    beforeTerminalReceiptPersist: async () => {
      throw new Error('sqlite crash window')
    }
  })

  await assert.rejects(crashingAdapter.executeHandoff({ handoff, agents }), /sqlite crash window/)
  assert.equal(first.executionStore.listPendingExecutions().length, 1)
  assert.equal(first.memoryStore.getSnapshot().world.projects.find((project) => project.id === projectId)?.stage, 2)

  const restarted = createStoreAndAdapter({
    memoryPath,
    sqlitePath,
    backendName: 'sqlite'
  })
  assert.equal(restarted.memoryStore.getSnapshot().world.projects.find((project) => project.id === projectId)?.stage, 1)
  assert.equal(restarted.executionStore.findReceipt({
    handoffId: handoff.handoffId,
    idempotencyKey: handoff.idempotencyKey
  }), null)
  const recovered = await restarted.executionAdapter.recoverInterruptedExecutions()

  assert.equal(recovered.length, 1)
  assert.equal(recovered[0].status, 'failed')
  assert.equal(recovered[0].reasonCode, 'INTERRUPTED_EXECUTION_RECOVERY')
  assert.equal(isValidExecutionResult(recovered[0]), true)
  assert.equal(restarted.executionStore.listPendingExecutions().length, 0)
  assert.equal(restarted.memoryStore.getSnapshot().world.projects.find((project) => project.id === projectId)?.stage, 1)
  assert.equal(restarted.executionStore.findReceipt({
    handoffId: handoff.handoffId,
    idempotencyKey: handoff.idempotencyKey
  }).reasonCode, 'INTERRUPTED_EXECUTION_RECOVERY')
})

test('sqlite backend preserves duplicate detection across restart', async () => {
  const { memoryPath, sqlitePath } = createTempPaths('mvp-sqlite-dup-')
  const agents = createAgents()
  const first = createStoreAndAdapter({
    memoryPath,
    sqlitePath,
    backendName: 'sqlite'
  })
  const projectId = await seedProject(first.godCommandService, first.memoryStore, agents)
  const handoff = createHandoff({
    proposalType: 'PROJECT_ADVANCE',
    command: `project advance alpha ${projectId}`,
    args: { projectId },
    snapshotHash: snapshotHashForStore(first.memoryStore),
    preconditions: [{ kind: 'project_exists', targetId: projectId }]
  })

  const executed = await first.executionAdapter.executeHandoff({ handoff, agents })

  const restarted = createStoreAndAdapter({
    memoryPath,
    sqlitePath,
    backendName: 'sqlite'
  })
  const duplicate = await restarted.executionAdapter.executeHandoff({ handoff, agents })

  assert.equal(executed.status, 'executed')
  assert.equal(duplicate.status, 'duplicate')
  assert.equal(duplicate.reasonCode, 'DUPLICATE_HANDOFF')
  assert.equal(duplicate.evaluation.duplicateCheck.duplicateOf, executed.executionId)
  assert.equal(isValidExecutionResult(duplicate), true)
})

test('execution-result behavior stays aligned between memory and sqlite backends', async () => {
  const leftPaths = createTempPaths('mvp-store-memory-')
  const rightPaths = createTempPaths('mvp-store-sqlite-')
  const agents = createAgents()

  const memoryRun = createStoreAndAdapter({
    memoryPath: leftPaths.memoryPath,
    sqlitePath: leftPaths.sqlitePath,
    backendName: 'memory'
  })
  const sqliteRun = createStoreAndAdapter({
    memoryPath: rightPaths.memoryPath,
    sqlitePath: rightPaths.sqlitePath,
    backendName: 'sqlite'
  })

  const memoryProjectId = await seedProject(memoryRun.godCommandService, memoryRun.memoryStore, agents)
  const sqliteProjectId = await seedProject(sqliteRun.godCommandService, sqliteRun.memoryStore, agents)

  const memoryHandoff = createHandoff({
    proposalType: 'PROJECT_ADVANCE',
    command: `project advance alpha ${memoryProjectId}`,
    args: { projectId: memoryProjectId },
    snapshotHash: snapshotHashForStore(memoryRun.memoryStore),
    preconditions: [{ kind: 'project_exists', targetId: memoryProjectId }]
  })
  const sqliteHandoff = createHandoff({
    proposalType: 'PROJECT_ADVANCE',
    command: `project advance alpha ${sqliteProjectId}`,
    args: { projectId: sqliteProjectId },
    snapshotHash: snapshotHashForStore(sqliteRun.memoryStore),
    preconditions: [{ kind: 'project_exists', targetId: sqliteProjectId }]
  })

  const memoryResult = await memoryRun.executionAdapter.executeHandoff({ handoff: memoryHandoff, agents })
  const sqliteResult = await sqliteRun.executionAdapter.executeHandoff({ handoff: sqliteHandoff, agents })

  assert.equal(isValidExecutionResult(memoryResult), true)
  assert.equal(isValidExecutionResult(sqliteResult), true)
  assert.equal(memoryResult.status, sqliteResult.status)
  assert.equal(memoryResult.accepted, sqliteResult.accepted)
  assert.equal(memoryResult.executed, sqliteResult.executed)
  assert.equal(memoryResult.reasonCode, sqliteResult.reasonCode)
  assert.deepEqual(memoryResult.authorityCommands, sqliteResult.authorityCommands)
  assert.equal(memoryResult.proposalType, sqliteResult.proposalType)
  assert.equal(typeof sqliteRun.executionStore.findReceipt({
    handoffId: sqliteHandoff.handoffId,
    idempotencyKey: sqliteHandoff.idempotencyKey
  })?.executionId, 'string')
})
