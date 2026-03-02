const crypto = require('crypto')
const fs = require('fs')
const os = require('os')
const path = require('path')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createExecutionAdapter, isValidExecutionResult } = require('../src/executionAdapter')
const { createGodCommandService } = require('../src/godCommands')
const { createMemoryStore } = require('../src/memory')
const { createAuthoritativeSnapshotProjection } = require('../src/worldSnapshotProjection')

function createStoreContext() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-execution-recovery-'))
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
      reason: 'Recovery test.',
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
    operationId: 'execution-recovery-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'project start alpha lantern_line',
    operationId: 'execution-recovery-seed-project'
  })
  return memoryStore.getSnapshot().world.projects[0].id
}

test('restart recovery classifies interrupted execution after command commit and clears pending marker', async () => {
  const { filePath, memoryStore } = createStoreContext()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()
  const projectId = await seedProject(service, memoryStore, agents)
  const handoff = createHandoff({
    proposalType: 'PROJECT_ADVANCE',
    command: `project advance alpha ${projectId}`,
    args: { projectId },
    snapshotHash: snapshotHashForStore(memoryStore),
    preconditions: [{ kind: 'project_exists', targetId: projectId }]
  })

  const crashingAdapter = createExecutionAdapter({
    memoryStore,
    godCommandService: service,
    beforeTerminalReceiptPersist: async () => {
      throw new Error('simulated crash between authority commit and terminal receipt write')
    }
  })

  await assert.rejects(
    crashingAdapter.executeHandoff({ handoff, agents }),
    /simulated crash between authority commit and terminal receipt write/
  )

  const interruptedSnapshot = memoryStore.getSnapshot()
  assert.equal(interruptedSnapshot.world.projects[0].stage, 2)
  assert.equal(interruptedSnapshot.world.execution.history.length, 0)
  assert.equal(interruptedSnapshot.world.execution.pending.length, 1)
  assert.equal(interruptedSnapshot.world.execution.pending[0].completedCommandCount, 1)

  const reloadedStore = createMemoryStore({ filePath })
  const reloadedService = createGodCommandService({ memoryStore: reloadedStore })
  const recoveryAdapter = createExecutionAdapter({
    memoryStore: reloadedStore,
    godCommandService: reloadedService
  })

  const recovered = await recoveryAdapter.recoverInterruptedExecutions()

  assert.equal(recovered.length, 1)
  assert.equal(recovered[0].status, 'failed')
  assert.equal(recovered[0].reasonCode, 'INTERRUPTED_EXECUTION_RECOVERY')
  assert.equal(recovered[0].accepted, true)
  assert.equal(recovered[0].executed, false)
  assert.equal(isValidExecutionResult(recovered[0]), true)
  assert.equal(recovered[0].evaluation.duplicateCheck.duplicate, false)
  assert.match(recovered[0].worldState.postExecutionSnapshotHash, /^[0-9a-f]{64}$/)
  assert.equal(recovered[0].evaluation.preconditions.failures[0].kind, 'interrupted_execution')

  const recoveredSnapshot = reloadedStore.getSnapshot()
  assert.equal(recoveredSnapshot.world.execution.pending.length, 0)
  assert.equal(recoveredSnapshot.world.execution.history.length, 1)
  assert.equal(recoveredSnapshot.world.execution.history[0].reasonCode, 'INTERRUPTED_EXECUTION_RECOVERY')
})

test('executeHandoff auto-recovers interrupted execution and keeps duplicate handling stable after restart', async () => {
  const { filePath, memoryStore } = createStoreContext()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()
  const projectId = await seedProject(service, memoryStore, agents)
  const handoff = createHandoff({
    proposalType: 'PROJECT_ADVANCE',
    command: `project advance alpha ${projectId}`,
    args: { projectId },
    snapshotHash: snapshotHashForStore(memoryStore),
    preconditions: [{ kind: 'project_exists', targetId: projectId }]
  })

  const crashingAdapter = createExecutionAdapter({
    memoryStore,
    godCommandService: service,
    beforeTerminalReceiptPersist: async () => {
      throw new Error('simulated crash between authority commit and terminal receipt write')
    }
  })

  await assert.rejects(
    crashingAdapter.executeHandoff({ handoff, agents }),
    /simulated crash between authority commit and terminal receipt write/
  )

  const reloadedStore = createMemoryStore({ filePath })
  const reloadedService = createGodCommandService({ memoryStore: reloadedStore })
  const restartedAdapter = createExecutionAdapter({
    memoryStore: reloadedStore,
    godCommandService: reloadedService
  })

  const duplicate = await restartedAdapter.executeHandoff({ handoff, agents })

  assert.equal(duplicate.status, 'duplicate')
  assert.equal(duplicate.reasonCode, 'DUPLICATE_HANDOFF')
  assert.equal(duplicate.evaluation.duplicateCheck.duplicate, true)
  assert.match(duplicate.worldState.postExecutionSnapshotHash, /^[0-9a-f]{64}$/)
  assert.equal(reloadedStore.getSnapshot().world.projects[0].stage, 2)
  assert.equal(reloadedStore.getSnapshot().world.execution.pending.length, 0)
  assert.equal(reloadedStore.getSnapshot().world.execution.history.length, 1)

  const recoveredReceipt = reloadedStore.getSnapshot().world.execution.history[0]
  assert.equal(recoveredReceipt.reasonCode, 'INTERRUPTED_EXECUTION_RECOVERY')
  assert.equal(duplicate.evaluation.duplicateCheck.duplicateOf, recoveredReceipt.executionId)
})
