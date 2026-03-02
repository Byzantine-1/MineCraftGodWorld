const crypto = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')
const { execFileSync } = require('child_process')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createExecutionAdapter } = require('../src/executionAdapter')
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
  return () => Date.parse('2026-02-24T00:00:00.000Z')
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
      reason: 'World memory persistence test.',
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

function createStoreContext(backendName) {
  const { memoryPath, sqlitePath } = createTempPaths(`mvp-world-memory-${backendName}-`)
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
    backendName,
    sqlitePath,
    memoryStore,
    executionStore,
    godCommandService,
    executionAdapter
  }
}

function pickStableSummary(summary) {
  return {
    type: summary.type,
    schemaVersion: summary.schemaVersion,
    chronicleCount: summary.chronicleCount,
    historyCount: summary.historyCount,
    hope: summary.hope,
    dread: summary.dread,
    activeMajorMissionId: summary.activeMajorMissionId,
    recentImpactCount: summary.recentImpactCount,
    crierQueueDepth: summary.crierQueueDepth,
    activeProjectCount: summary.activeProjectCount,
    factions: summary.factions,
    executionCounts: summary.executionCounts,
    towns: summary.towns,
    hostilityToPlayer: summary.hostilityToPlayer,
    stability: summary.stability,
    doctrine: summary.doctrine,
    rivals: summary.rivals
  }
}

function pickStableHistoryRecords(records) {
  return records.map((record) => ({
    type: record.type,
    schemaVersion: record.schemaVersion,
    sourceType: record.sourceType,
    handoffId: record.handoffId,
    idempotencyKey: record.idempotencyKey,
    actorId: record.actorId,
    townId: record.townId,
    proposalType: record.proposalType,
    command: record.command,
    authorityCommands: record.authorityCommands,
    status: record.status,
    reasonCode: record.reasonCode,
    kind: record.kind,
    at: record.at,
    summary: record.summary
  })).sort((left, right) => (
    `${left.sourceType}:${left.kind}:${left.proposalType}:${left.status}`
      .localeCompare(`${right.sourceType}:${right.kind}:${right.proposalType}:${right.status}`)
  ))
}

async function seedWorldMemory(context) {
  const { memoryStore, godCommandService, executionAdapter, executionStore } = context
  const agents = createAgents()

  await godCommandService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'world-memory:seed-town-alpha'
  })
  await godCommandService.applyGodCommand({
    agents,
    command: 'mark add beta_hall 20 64 20 town:beta',
    operationId: 'world-memory:seed-town-beta'
  })
  await godCommandService.applyGodCommand({
    agents,
    command: 'project start alpha lantern_line',
    operationId: 'world-memory:seed-project-alpha'
  })

  await memoryStore.transact((memory) => {
    memory.world.factions.iron_pact = {
      name: 'iron_pact',
      towns: ['alpha'],
      doctrine: 'Order through steel.',
      rivals: ['veil_church'],
      hostilityToPlayer: 18,
      stability: 77
    }
    memory.world.chronicle.push(
      {
        id: 'c_alpha_01',
        type: 'mission',
        msg: 'Alpha mayor briefed a new mission.',
        at: 9000000000001,
        town: 'alpha',
        meta: { factionId: 'iron_pact', missionId: 'mm_alpha_1' }
      },
      {
        id: 'c_alpha_02',
        type: 'project',
        msg: 'Alpha raised the first lantern posts.',
        at: 9000000000002,
        town: 'alpha',
        meta: { factionId: 'iron_pact', projectId: 'pr_alpha_1' }
      },
      {
        id: 'c_beta_01',
        type: 'omens',
        msg: 'Beta heard a bad bell in the fog.',
        at: 9000000000000,
        town: 'beta',
        meta: { factionId: 'veil_church' }
      }
    )
  }, { eventId: 'world-memory:chronicle-seed' })

  const projectId = memoryStore.getSnapshot().world.projects[0].id
  const executed = await executionAdapter.executeHandoff({
    agents,
    handoff: createHandoff({
      proposalType: 'PROJECT_ADVANCE',
      command: `project advance alpha ${projectId}`,
      args: { projectId },
      snapshotHash: snapshotHashForStore(memoryStore),
      preconditions: [{ kind: 'project_exists', targetId: projectId }]
    })
  })

  const stale = await executionAdapter.executeHandoff({
    agents,
    handoff: createHandoff({
      proposalType: 'SALVAGE_PLAN',
      command: 'salvage initiate alpha scarcity',
      args: { focus: 'scarcity' },
      decisionEpoch: 2,
      snapshotHash: snapshotHashForStore(memoryStore),
      preconditions: [{ kind: 'salvage_focus_supported', expected: 'scarcity' }]
    })
  })

  executionStore.syncWorldMemory()

  return {
    executed,
    stale,
    projectId
  }
}

test('world memory chronicle queries return stable ordered records for memory backend', async () => {
  const context = createStoreContext('memory')
  await seedWorldMemory(context)

  const alphaChronicle = context.executionStore.listChronicleRecords({
    townId: 'alpha',
    factionId: 'iron_pact',
    limit: 10
  })

  assert.deepEqual(alphaChronicle.map((record) => record.recordId), [
    'chronicle:c_alpha_02',
    'chronicle:c_alpha_01'
  ])
  assert.deepEqual(alphaChronicle.map((record) => record.factionId), ['iron_pact', 'iron_pact'])
  assert.deepEqual(alphaChronicle[0].tags, ['chronicle', 'faction:iron_pact', 'town:alpha', 'type:project'])

  const searchMatches = context.executionStore.listChronicleRecords({
    townId: 'alpha',
    factionId: 'iron_pact',
    search: 'briefed',
    limit: 10
  })
  assert.deepEqual(searchMatches.map((record) => record.recordId), ['chronicle:c_alpha_01'])
})

test('world memory history and summaries are queryable through the store boundary', async () => {
  const context = createStoreContext('memory')
  const { executed, stale } = await seedWorldMemory(context)

  const alphaHistory = context.executionStore.listHistoryRecords({
    townId: 'alpha',
    limit: 10
  })

  assert.equal(alphaHistory.length, 4)
  assert.deepEqual(new Set(alphaHistory.map((record) => record.sourceType)), new Set(['execution_receipt', 'execution_event']))
  assert.deepEqual(alphaHistory
    .filter((record) => record.sourceType === 'execution_receipt')
    .map((record) => record.status)
    .sort(), ['executed', 'stale'])
  assert.equal(context.executionStore.findReceipt({
    handoffId: executed.handoffId,
    idempotencyKey: executed.idempotencyKey
  }).executionId, executed.executionId)
  assert.equal(context.executionStore.findReceipt({
    handoffId: stale.handoffId,
    idempotencyKey: stale.idempotencyKey
  }).executionId, stale.executionId)

  const townSummary = context.executionStore.getTownHistorySummary({ townId: 'alpha' })
  assert.equal(townSummary.type, 'town-history-summary.v1')
  assert.equal(townSummary.chronicleCount, context.executionStore.listChronicleRecords({ townId: 'alpha', limit: 200 }).length)
  assert.equal(townSummary.historyCount, 4)
  assert.deepEqual(townSummary.executionCounts, {
    executed: 1,
    rejected: 0,
    stale: 1,
    duplicate: 0,
    failed: 0
  })
  assert.equal(townSummary.recentChronicle[0].recordId, 'chronicle:c_alpha_02')

  const factionSummary = context.executionStore.getFactionHistorySummary({ factionId: 'iron_pact' })
  assert.equal(factionSummary.type, 'faction-history-summary.v1')
  assert.deepEqual(factionSummary.towns, ['alpha'])
  assert.equal(factionSummary.chronicleCount, townSummary.chronicleCount)
  assert.equal(factionSummary.historyCount, 4)
  assert.equal(factionSummary.doctrine, 'Order through steel.')
  assert.equal(factionSummary.recentChronicle[0].recordId, 'chronicle:c_alpha_02')
})

test('sqlite-backed world memory projection stays aligned with memory backend queries', async () => {
  const memoryContext = createStoreContext('memory')
  const sqliteContext = createStoreContext('sqlite')

  const memorySeed = await seedWorldMemory(memoryContext)
  const sqliteSeed = await seedWorldMemory(sqliteContext)

  const memoryChronicle = memoryContext.executionStore.listChronicleRecords({ factionId: 'iron_pact', limit: 10 })
  const sqliteChronicle = sqliteContext.executionStore.listChronicleRecords({ factionId: 'iron_pact', limit: 10 })
  const memoryHistory = memoryContext.executionStore.listHistoryRecords({ limit: 10 })
  const sqliteHistory = sqliteContext.executionStore.listHistoryRecords({ limit: 10 })
  const memoryTownSummary = memoryContext.executionStore.getTownHistorySummary({ townId: 'alpha' })
  const sqliteTownSummary = sqliteContext.executionStore.getTownHistorySummary({ townId: 'alpha' })
  const memoryFactionSummary = memoryContext.executionStore.getFactionHistorySummary({ factionId: 'iron_pact' })
  const sqliteFactionSummary = sqliteContext.executionStore.getFactionHistorySummary({ factionId: 'iron_pact' })

  assert.deepEqual(sqliteChronicle, memoryChronicle)
  assert.deepEqual(pickStableHistoryRecords(sqliteHistory), pickStableHistoryRecords(memoryHistory))
  assert.deepEqual(pickStableSummary(sqliteTownSummary), pickStableSummary(memoryTownSummary))
  assert.equal(sqliteTownSummary.recentChronicle[0].recordId, memoryTownSummary.recentChronicle[0].recordId)
  assert.deepEqual(pickStableSummary(sqliteFactionSummary), pickStableSummary(memoryFactionSummary))
  assert.equal(sqliteFactionSummary.recentChronicle[0].recordId, memoryFactionSummary.recentChronicle[0].recordId)
  assert.equal(sqliteContext.executionStore.findReceipt({
    handoffId: sqliteSeed.executed.handoffId,
    idempotencyKey: sqliteSeed.executed.idempotencyKey
  }).executionId, sqliteSeed.executed.executionId)
  assert.equal(sqliteContext.executionStore.findReceipt({
    handoffId: sqliteSeed.stale.handoffId,
    idempotencyKey: sqliteSeed.stale.idempotencyKey
  }).executionId, sqliteSeed.stale.executionId)

  const tables = sqliteJson(sqliteContext.sqlitePath, `
    SELECT name
    FROM sqlite_master
    WHERE type = 'table' AND name = 'world_chronicle_records';
  `)
  assert.deepEqual(tables.map((row) => row.name), ['world_chronicle_records'])

  const chronicleRows = sqliteJson(sqliteContext.sqlitePath, `
    SELECT source_id, town_id, faction_id
    FROM world_chronicle_records
    WHERE source_id LIKE 'c_%'
    ORDER BY at DESC, record_id DESC;
  `)
  assert.deepEqual(chronicleRows, [
    { source_id: 'c_alpha_02', town_id: 'alpha', faction_id: 'iron_pact' },
    { source_id: 'c_alpha_01', town_id: 'alpha', faction_id: 'iron_pact' },
    { source_id: 'c_beta_01', town_id: 'beta', faction_id: 'veil_church' }
  ])
  assert.equal(memorySeed.executed.status, 'executed')
})
