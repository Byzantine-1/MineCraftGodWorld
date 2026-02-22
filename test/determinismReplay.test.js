const fs = require('fs')
const os = require('os')
const path = require('path')
const crypto = require('crypto')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createMemoryStore } = require('../src/memory')
const { createGodCommandService } = require('../src/godCommands')

function createTempMemoryPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-determinism-'))
  return path.join(dir, 'memory.json')
}

function createAgents() {
  return [
    { name: 'Mara', faction: 'Pilgrims', applyGodCommand: () => {} },
    { name: 'Eli', faction: 'Pilgrims', applyGodCommand: () => {} }
  ]
}

function createDeterministicNow(stepMs = 19) {
  let ts = 1_700_000_000_000
  return () => {
    ts += stepMs
    return ts
  }
}

function stableSortValue(value) {
  if (Array.isArray(value)) return value.map(stableSortValue)
  if (!value || typeof value !== 'object') return value
  const out = {}
  for (const key of Object.keys(value).sort()) out[key] = stableSortValue(value[key])
  return out
}

function normalizeVolatileFields(value) {
  if (Array.isArray(value)) return value.map(normalizeVolatileFields)
  if (!value || typeof value !== 'object') return value
  const out = {}
  for (const [key, child] of Object.entries(value)) {
    if (key === 'updated_at') continue
    out[key] = normalizeVolatileFields(child)
  }
  return out
}

function hashSnapshot(snapshot) {
  const stable = stableSortValue(normalizeVolatileFields(snapshot))
  return crypto.createHash('sha256').update(JSON.stringify(stable)).digest('hex')
}

function parseOutputValue(lines, regex) {
  for (const line of lines || []) {
    const match = String(line).match(regex)
    if (match) return match[1]
  }
  return ''
}

async function runDeterministicScenario() {
  const filePath = createTempMemoryPath()
  const now = createDeterministicNow()
  const memoryStore = createMemoryStore({ filePath, now })
  const service = createGodCommandService({ memoryStore, now })
  const agents = createAgents()

  /**
   * @param {string} command
   * @param {string} operationId
   */
  async function apply(command, operationId) {
    const result = await service.applyGodCommand({ agents, command, operationId })
    assert.equal(result.applied, true, `expected applied command: ${command}`)
    return result
  }

  await apply('mark add alpha_hall 0 64 0 town:alpha', 'op-01')
  await apply('market add bazaar alpha_hall', 'op-02')
  await apply('mint Mara 25', 'op-03')
  await apply('mint Eli 25', 'op-04')

  const offerAdd = await apply('offer add bazaar Mara sell 2 5', 'op-05')
  const offerId = parseOutputValue(offerAdd.outputLines, /id=([^\s]+)/i)
  assert.ok(offerId)

  await apply(`trade bazaar ${offerId} Eli 1`, 'op-06')
  await apply('event seed 777', 'op-07')
  await apply('event draw alpha', 'op-08')

  const decisionId = memoryStore.getSnapshot().world.decisions[0]?.id
  assert.ok(decisionId)
  const decisionShow = await apply(`decision show ${decisionId}`, 'op-09')
  const optionKey = parseOutputValue(decisionShow.outputLines, /GOD DECISION OPTION:\s*key=([^\s]+)/i)
  assert.ok(optionKey)
  await apply(`decision choose ${decisionId} ${optionKey}`, 'op-10')

  await apply('rumor spawn alpha supernatural 2 mist_shapes 2', 'op-11')
  const rumorId = memoryStore.getSnapshot().world.rumors[0]?.id
  assert.ok(rumorId)
  await apply(`rumor quest ${rumorId}`, 'op-12')
  const sideQuestId = memoryStore.getSnapshot().world.quests.find((entry) => entry.rumor_id === rumorId)?.id
  assert.ok(sideQuestId)
  await apply(`quest accept Mara ${sideQuestId}`, 'op-13')
  await apply(`quest visit ${sideQuestId}`, 'op-14')

  const replayA = await service.applyGodCommand({
    agents,
    command: 'mint Mara 25',
    operationId: 'op-03'
  })
  const replayB = await service.applyGodCommand({
    agents,
    command: `trade bazaar ${offerId} Eli 1`,
    operationId: 'op-06'
  })
  const replayC = await service.applyGodCommand({
    agents,
    command: `quest visit ${sideQuestId}`,
    operationId: 'op-14'
  })
  assert.equal(replayA.applied, false)
  assert.equal(replayB.applied, false)
  assert.equal(replayC.applied, false)

  const snapshot = memoryStore.getSnapshot()
  return {
    snapshot,
    hash: hashSnapshot(snapshot)
  }
}

test('deterministic replay scenario yields identical final snapshot hash across isolated runs', async () => {
  const runA = await runDeterministicScenario()
  const runB = await runDeterministicScenario()

  assert.equal(runA.hash, runB.hash)
  assert.deepEqual(normalizeVolatileFields(runA.snapshot), normalizeVolatileFields(runB.snapshot))
})
