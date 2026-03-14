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

test('execution adapter translates MISSION_ADVANCE into the authoritative mission advance command', async () => {
  const { memoryStore } = createStoreContext()
  const service = createGodCommandService({ memoryStore })
  const executionAdapter = createExecutionAdapter({ memoryStore, godCommandService: service })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'execution-adapter-mission-advance-seed-town'
  })
  await executionAdapter.executeHandoff({
    handoff: createHandoff({
      proposalType: 'MAYOR_ACCEPT_MISSION',
      command: 'mission accept alpha sq-side-1',
      args: { missionId: 'sq-side-1' },
      snapshotHash: snapshotHashForStore(memoryStore),
      preconditions: [{ kind: 'mission_absent' }]
    }),
    agents
  })

  const beforeMission = memoryStore.getSnapshot().world.majorMissions.find((entry) => entry.status === 'active' && entry.townId === 'alpha')
  assert(beforeMission)

  const handoff = createHandoff({
    proposalType: 'MISSION_ADVANCE',
    command: 'mission advance alpha',
    args: { missionId: beforeMission.id },
    snapshotHash: snapshotHashForStore(memoryStore)
  })

  const result = await executionAdapter.executeHandoff({ handoff, agents })

  assert.equal(result.status, 'executed')
  assert.equal(result.accepted, true)
  assert.equal(result.executed, true)
  assert.equal(result.reasonCode, 'EXECUTED')
  assert.deepEqual(result.authorityCommands, ['mission advance alpha'])
  assert.equal(result.proposalType, 'MISSION_ADVANCE')
  assert.equal(isValidExecutionResult(result), true)

  const afterMission = memoryStore.getSnapshot().world.majorMissions.find((entry) => entry.id === beforeMission.id)
  assert(afterMission)
  assert.equal(afterMission.phase, Number(beforeMission.phase) + 1)
  assert.equal(memoryStore.getSnapshot().world.towns.alpha.activeMajorMissionId, beforeMission.id)
})

test('execution adapter translates MISSION_COMPLETE into the authoritative mission complete command and pressure update', async () => {
  const { memoryStore } = createStoreContext()
  const service = createGodCommandService({ memoryStore })
  const executionAdapter = createExecutionAdapter({ memoryStore, godCommandService: service })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'execution-adapter-mission-complete-seed-town'
  })
  await executionAdapter.executeHandoff({
    handoff: createHandoff({
      proposalType: 'MAYOR_ACCEPT_MISSION',
      command: 'mission accept alpha sq-side-2',
      args: { missionId: 'sq-side-2' },
      snapshotHash: snapshotHashForStore(memoryStore),
      preconditions: [{ kind: 'mission_absent' }]
    }),
    agents
  })

  const activeMissionId = memoryStore.getSnapshot().world.towns.alpha.activeMajorMissionId
  assert(activeMissionId)

  const handoff = createHandoff({
    proposalType: 'MISSION_COMPLETE',
    command: 'mission complete alpha',
    args: { missionId: activeMissionId },
    snapshotHash: snapshotHashForStore(memoryStore)
  })

  const result = await executionAdapter.executeHandoff({ handoff, agents })

  assert.equal(result.status, 'executed')
  assert.equal(result.accepted, true)
  assert.equal(result.executed, true)
  assert.equal(result.reasonCode, 'EXECUTED')
  assert.deepEqual(result.authorityCommands, ['mission complete alpha'])
  assert.equal(result.proposalType, 'MISSION_COMPLETE')
  assert.equal(isValidExecutionResult(result), true)

  const snapshot = memoryStore.getSnapshot()
  const completedMission = snapshot.world.majorMissions.find((entry) => entry.id === activeMissionId)
  assert(completedMission)
  assert.equal(completedMission.status, 'completed')
  assert.equal(snapshot.world.towns.alpha.activeMajorMissionId, null)
  assert(snapshot.world.towns.alpha.majorMissionCooldownUntilDay > 0)
  assert(snapshot.world.towns.alpha.hope > 50)
  assert(snapshot.world.towns.alpha.dread < 50)
})

test('execution adapter returns a teleport embodiment hint for PLAYER_GET_SPAWN after authoritative assignment', async () => {
  const { memoryStore } = createStoreContext()
  const service = createGodCommandService({ memoryStore })
  const executionAdapter = createExecutionAdapter({ memoryStore, godCommandService: service })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'execution-adapter-spawn-seed-town'
  })

  const setSpawn = await executionAdapter.executeHandoff({
    handoff: createHandoff({
      proposalType: 'TOWN_SET_SPAWN',
      actorId: 'ops',
      townId: 'alpha',
      command: 'town spawn set alpha overworld 4 81 -2 90 0 3 starter_hub',
      args: {
        townId: 'alpha',
        spawn: {
          dimension: 'overworld',
          x: 4,
          y: 81,
          z: -2,
          yaw: 90,
          pitch: 0,
          radius: 3,
          kind: 'starter_hub'
        }
      },
      snapshotHash: snapshotHashForStore(memoryStore)
    }),
    agents
  })

  const assignTown = await executionAdapter.executeHandoff({
    handoff: createHandoff({
      proposalType: 'PLAYER_ASSIGN_TOWN',
      actorId: 'ops',
      townId: 'alpha',
      command: 'player assign Builder01 alpha',
      args: {
        playerId: 'Builder01',
        townId: 'alpha'
      },
      snapshotHash: snapshotHashForStore(memoryStore)
    }),
    agents
  })

  const getSpawn = await executionAdapter.executeHandoff({
    handoff: createHandoff({
      proposalType: 'PLAYER_GET_SPAWN',
      actorId: 'Builder01',
      townId: 'auto',
      command: 'player spawn Builder01',
      args: {
        playerId: 'Builder01'
      },
      snapshotHash: snapshotHashForStore(memoryStore)
    }),
    agents
  })

  assert.equal(setSpawn.status, 'executed')
  assert.equal(assignTown.status, 'executed')
  assert.equal(getSpawn.status, 'executed')
  assert.equal(getSpawn.townId, 'alpha')
  assert.equal(isValidExecutionResult(getSpawn), true)
  assert.deepEqual(memoryStore.getSnapshot().world.players.Builder01, {
    playerId: 'Builder01',
    townId: 'alpha',
    assignedAtDay: 1,
    spawnPolicy: 'explicit_town'
  })
  assert.deepEqual(memoryStore.getSnapshot().world.towns.alpha.spawn, {
    dimension: 'overworld',
    x: 4,
    y: 81,
    z: -2,
    yaw: 90,
    pitch: 0,
    radius: 3,
    kind: 'starter_hub'
  })
  assert.equal(getSpawn.embodiment.backendHint, 'bridge')
  assert.equal(getSpawn.embodiment.actions[0].type, 'teleport')
  assert.deepEqual(getSpawn.embodiment.actions[0].target, {
    kind: 'player',
    id: 'Builder01'
  })
  assert.equal(getSpawn.embodiment.actions[0].dimension, 'overworld')
  assert.equal(getSpawn.embodiment.actions[0].x, 4)
  assert.equal(getSpawn.embodiment.actions[0].y, 81)
  assert.equal(getSpawn.embodiment.actions[0].z, -2)
  assert.equal(getSpawn.embodiment.actions[0].yaw, 90)
  assert.equal(getSpawn.embodiment.actions[0].meta.townId, 'alpha')
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
