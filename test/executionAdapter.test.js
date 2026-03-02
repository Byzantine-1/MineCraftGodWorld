const crypto = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createExecutionAdapter, isValidExecutionResult, parseExecutionHandoffLine } = require('../src/executionAdapter')
const { createGodCommandService } = require('../src/godCommands')
const { createMemoryStore } = require('../src/memory')
const { createAuthoritativeSnapshotProjection } = require('../src/worldSnapshotProjection')

function createStoreContext() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-execution-adapter-'))
  const filePath = path.join(dir, 'memory.json')
  return {
    filePath,
    memoryStore: createMemoryStore({ filePath })
  }
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
      reason: 'Deterministic adapter test.',
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

function snapshotHashForStore(memoryStore) {
  return createAuthoritativeSnapshotProjection(memoryStore.recallWorld()).snapshotHash
}

test('parseExecutionHandoffLine recognizes valid execution-handoff.v1 JSON lines', () => {
  const handoff = createHandoff({
    proposalType: 'PROJECT_ADVANCE',
    command: 'project advance alpha p-1',
    args: { projectId: 'p-1' }
  })

  assert.equal(parseExecutionHandoffLine('Mara: hello'), null)
  assert.deepEqual(parseExecutionHandoffLine(JSON.stringify(handoff)), handoff)
})

test('execution adapter emits canonical executed results for direct project advancement', async () => {
  const { memoryStore } = createStoreContext()
  const service = createGodCommandService({ memoryStore })
  const executionAdapter = createExecutionAdapter({ memoryStore, godCommandService: service })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'execution-adapter-project-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'project start alpha lantern_line',
    operationId: 'execution-adapter-project-seed-start'
  })
  const projectId = memoryStore.getSnapshot().world.projects[0].id
  const handoff = createHandoff({
    proposalType: 'PROJECT_ADVANCE',
    command: `project advance alpha ${projectId}`,
    args: { projectId },
    snapshotHash: snapshotHashForStore(memoryStore),
    preconditions: [{ kind: 'project_exists', targetId: projectId }]
  })

  const result = await executionAdapter.executeHandoff({ handoff, agents })

  assert.equal(result.type, 'execution-result.v1')
  assert.equal(result.schemaVersion, 1)
  assert.equal(result.status, 'executed')
  assert.equal(result.accepted, true)
  assert.equal(result.executed, true)
  assert.equal(result.executionId, result.resultId)
  assert.deepEqual(result.authorityCommands, [`project advance alpha ${projectId}`])
  assert.equal(result.actorId, 'mara')
  assert.equal(result.townId, 'alpha')
  assert.equal(result.proposalType, 'PROJECT_ADVANCE')
  assert.equal(result.reasonCode, 'EXECUTED')
  assert.equal(isValidExecutionResult(result), true)
  assert.match(result.evaluation.staleCheck.actualSnapshotHash, /^[0-9a-f]{64}$/)
  assert.match(result.worldState.postExecutionSnapshotHash, /^[0-9a-f]{64}$/)
  assert.notEqual(result.worldState.postExecutionSnapshotHash, result.evaluation.staleCheck.actualSnapshotHash)
  assert.equal(result.worldState.postExecutionDecisionEpoch, 1)

  const advancedProject = memoryStore.getSnapshot().world.projects.find((entry) => entry.id === projectId)
  assert.equal(advancedProject.stage, 2)
})

test('execution adapter translates MAYOR_ACCEPT_MISSION into authoritative mayor talk + accept commands', async () => {
  const { memoryStore } = createStoreContext()
  const service = createGodCommandService({ memoryStore })
  const executionAdapter = createExecutionAdapter({ memoryStore, godCommandService: service })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'execution-adapter-major-seed-town'
  })
  const handoff = createHandoff({
    proposalType: 'MAYOR_ACCEPT_MISSION',
    command: 'mission accept alpha sq-side-1',
    args: { missionId: 'sq-side-1' },
    snapshotHash: snapshotHashForStore(memoryStore),
    preconditions: [{ kind: 'mission_absent' }, { kind: 'side_quest_exists', targetId: 'sq-side-1' }]
  })

  const result = await executionAdapter.executeHandoff({ handoff, agents })

  assert.equal(result.status, 'executed')
  assert.deepEqual(result.authorityCommands, ['mayor talk alpha', 'mayor accept alpha'])
  assert.equal(memoryStore.getSnapshot().world.towns.alpha.activeMajorMissionId !== null, true)
})

test('execution adapter classifies decision-epoch drift as stale before applying engine commands', async () => {
  const { memoryStore } = createStoreContext()
  const service = createGodCommandService({ memoryStore })
  const executionAdapter = createExecutionAdapter({ memoryStore, godCommandService: service })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'execution-adapter-stale-seed-town'
  })

  const before = memoryStore.getSnapshot()
  const beforeAuthoritativeHash = snapshotHashForStore(memoryStore)
  const handoff = createHandoff({
    proposalType: 'SALVAGE_PLAN',
    command: 'salvage initiate alpha scarcity',
    args: { focus: 'scarcity' },
    decisionEpoch: 2,
    snapshotHash: snapshotHashForStore(memoryStore),
    preconditions: [{ kind: 'salvage_focus_supported', expected: 'scarcity' }]
  })

  const result = await executionAdapter.executeHandoff({ handoff, agents })

  assert.equal(result.status, 'stale')
  assert.equal(result.reasonCode, 'STALE_DECISION_EPOCH')
  assert.equal(result.accepted, false)
  assert.equal(result.executed, false)
  assert.match(result.evaluation.staleCheck.actualSnapshotHash, /^[0-9a-f]{64}$/)
  assert.equal(result.worldState.postExecutionSnapshotHash, result.evaluation.staleCheck.actualSnapshotHash)
  assert.equal(snapshotHashForStore(memoryStore), beforeAuthoritativeHash)
  assert.deepEqual(memoryStore.getSnapshot().world.projects, before.world.projects)
  assert.equal(memoryStore.getSnapshot().world.execution.history.length, before.world.execution.history.length + 1)
})

test('execution adapter classifies replayed handoffs as duplicate using durable execution history', async () => {
  const { filePath, memoryStore } = createStoreContext()
  const service = createGodCommandService({ memoryStore })
  const executionAdapter = createExecutionAdapter({ memoryStore, godCommandService: service })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'execution-adapter-duplicate-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'project start alpha trench_reinforcement',
    operationId: 'execution-adapter-duplicate-seed-project'
  })
  const projectId = memoryStore.getSnapshot().world.projects[0].id
  const handoff = createHandoff({
    proposalType: 'PROJECT_ADVANCE',
    command: `project advance alpha ${projectId}`,
    args: { projectId },
    snapshotHash: snapshotHashForStore(memoryStore),
    preconditions: [{ kind: 'project_exists', targetId: projectId }]
  })

  const first = await executionAdapter.executeHandoff({ handoff, agents })
  const reloadedStore = createMemoryStore({ filePath })
  const reloadedService = createGodCommandService({ memoryStore: reloadedStore })
  const reloadedAdapter = createExecutionAdapter({
    memoryStore: reloadedStore,
    godCommandService: reloadedService
  })
  const duplicate = await reloadedAdapter.executeHandoff({ handoff, agents })

  assert.equal(first.status, 'executed')
  assert.equal(duplicate.status, 'duplicate')
  assert.equal(duplicate.reasonCode, 'DUPLICATE_HANDOFF')
  assert.equal(duplicate.evaluation.duplicateCheck.duplicate, true)
  assert.equal(duplicate.evaluation.duplicateCheck.duplicateOf, first.executionId)
  assert.match(duplicate.worldState.postExecutionSnapshotHash, /^[0-9a-f]{64}$/)
  assert.equal(reloadedStore.getSnapshot().world.execution.history.length >= 1, true)
})
