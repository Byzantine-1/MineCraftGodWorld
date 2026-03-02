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
    WHERE type = 'table' AND name IN ('execution_receipts', 'execution_pending', 'execution_event_ledger')
    ORDER BY name;
  `)

  assert.deepEqual(tables.map((row) => row.name), [
    'execution_event_ledger',
    'execution_pending',
    'execution_receipts'
  ])
  assert.equal(executionStore.findPendingExecution({
    handoffId: fakeResult.handoffId,
    idempotencyKey: fakeResult.idempotencyKey
  }), null)
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

  const restarted = createStoreAndAdapter({
    memoryPath,
    sqlitePath,
    backendName: 'sqlite'
  })
  const recovered = await restarted.executionAdapter.recoverInterruptedExecutions()

  assert.equal(recovered.length, 1)
  assert.equal(recovered[0].status, 'failed')
  assert.equal(recovered[0].reasonCode, 'INTERRUPTED_EXECUTION_RECOVERY')
  assert.equal(isValidExecutionResult(recovered[0]), true)
  assert.equal(restarted.executionStore.listPendingExecutions().length, 0)
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
