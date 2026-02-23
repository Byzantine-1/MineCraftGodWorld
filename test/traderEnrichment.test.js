const fs = require('fs')
const os = require('os')
const path = require('path')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createMemoryStore } = require('../src/memory')
const { createGodCommandService } = require('../src/godCommands')

function createStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-trader-enrichment-'))
  const filePath = path.join(dir, 'memory.json')
  return createMemoryStore({ filePath })
}

function createAgents() {
  return [
    { name: 'Mara', faction: 'Pilgrims', applyGodCommand: () => {} },
    { name: 'Eli', faction: 'Pilgrims', applyGodCommand: () => {} }
  ]
}

/**
 * @param {string} text
 */
function stableHashNumber(text) {
  let hash = 0
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) - hash) + text.charCodeAt(i)
    hash |= 0
  }
  return Math.abs(hash)
}

test('market pulse stays deterministic/read-only and is actionable across day+town', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'pulse-enrich-seed-alpha'
  })
  await service.applyGodCommand({
    agents,
    command: 'mark add beta_hall 100 64 0 town:beta',
    operationId: 'pulse-enrich-seed-beta'
  })
  await service.applyGodCommand({
    agents,
    command: 'threat set alpha 88',
    operationId: 'pulse-enrich-threat-alpha'
  })
  await service.applyGodCommand({
    agents,
    command: 'threat set beta 10',
    operationId: 'pulse-enrich-threat-beta'
  })

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const before = memoryStore.getSnapshot()
  const alphaA = await service.applyGodCommand({
    agents,
    command: 'market pulse alpha',
    operationId: 'pulse-enrich-alpha-a'
  })
  const alphaB = await service.applyGodCommand({
    agents,
    command: 'market pulse alpha',
    operationId: 'pulse-enrich-alpha-b'
  })
  const betaA = await service.applyGodCommand({
    agents,
    command: 'market pulse beta',
    operationId: 'pulse-enrich-beta-a'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(alphaA.applied, true)
  assert.equal(alphaB.applied, true)
  assert.equal(betaA.applied, true)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
  assert.deepEqual(alphaA.outputLines, alphaB.outputLines)
  assert.ok(alphaA.outputLines.some(line => line.includes('reason=Bring ')))
  assert.ok(alphaA.outputLines.some(line => line.includes('reason=Hold ')))
  assert.notDeepEqual(alphaA.outputLines, betaA.outputLines)

  await service.applyGodCommand({
    agents,
    command: 'clock advance 2',
    operationId: 'pulse-enrich-day-advance'
  })
  const alphaDay2 = await service.applyGodCommand({
    agents,
    command: 'market pulse alpha',
    operationId: 'pulse-enrich-alpha-day2'
  })
  assert.equal(alphaDay2.applied, true)
  assert.notDeepEqual(alphaA.outputLines, alphaDay2.outputLines)
})

test('daily contracts remain bounded/replay-safe and show richer template variety', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'contract-enrich-seed-alpha'
  })
  await service.applyGodCommand({
    agents,
    command: 'mark add beta_hall 100 64 0 town:beta',
    operationId: 'contract-enrich-seed-beta'
  })
  await service.applyGodCommand({
    agents,
    command: 'mark add gamma_hall 200 64 0 town:gamma',
    operationId: 'contract-enrich-seed-gamma'
  })
  await service.applyGodCommand({
    agents,
    command: 'market add alpha_market alpha_hall',
    operationId: 'contract-enrich-market-alpha'
  })
  await service.applyGodCommand({
    agents,
    command: 'market add beta_market beta_hall',
    operationId: 'contract-enrich-market-beta'
  })

  const day2 = await service.applyGodCommand({
    agents,
    command: 'clock advance 2',
    operationId: 'contract-enrich-day2'
  })
  assert.equal(day2.applied, true)
  const snapshotDay2 = memoryStore.getSnapshot()
  const day2Replay = await service.applyGodCommand({
    agents,
    command: 'clock advance 2',
    operationId: 'contract-enrich-day2'
  })
  assert.equal(day2Replay.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), snapshotDay2)

  for (let day = 3; day <= 6; day += 1) {
    const result = await service.applyGodCommand({
      agents,
      command: 'clock advance 2',
      operationId: `contract-enrich-day${day}`
    })
    assert.equal(result.applied, true)
  }

  const snapshot = memoryStore.getSnapshot()
  const contracts = snapshot.world.quests.filter((quest) => quest?.meta?.contract === true)
  assert.ok(contracts.length >= 8)
  assert.ok(contracts.some(quest => quest.type === 'trade_n'))
  assert.ok(contracts.some(quest => quest.type === 'visit_town'))
  const uniqueTitles = new Set(contracts.map(quest => String(quest.title || '')))
  assert.ok(uniqueTitles.size >= 4)

  const byTownDay = new Map()
  for (const quest of contracts) {
    const town = String(quest.town || '').toLowerCase()
    const day = Number(quest.meta?.contract_day || 0)
    const key = `${town}:${day}`
    byTownDay.set(key, Number(byTownDay.get(key) || 0) + 1)
    assert.ok(Number.isInteger(quest.reward))
    assert.ok(quest.reward >= 1 && quest.reward <= 12)
  }
  for (const value of byTownDay.values()) {
    assert.ok(value >= 1 && value <= 2)
  }
})

test('event-driven rumor leads remain replay-safe and directional templates render outward cues', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'lead-enrich-seed-alpha'
  })
  await service.applyGodCommand({
    agents,
    command: 'event seed 777',
    operationId: 'lead-enrich-seed-event'
  })

  let drawIdx = 0
  while (memoryStore.getSnapshot().world.rumors.length === 0 && drawIdx < 10) {
    drawIdx += 1
    const operationId = `lead-enrich-draw-${drawIdx}`
    const draw = await service.applyGodCommand({
      agents,
      command: 'event draw alpha',
      operationId
    })
    assert.equal(draw.applied, true)
  }

  const withRumor = memoryStore.getSnapshot()
  assert.ok(withRumor.world.rumors.length >= 1)

  const directionalSpawn = await service.applyGodCommand({
    agents,
    command: 'rumor spawn alpha grounded 2 old_well_east 2',
    operationId: 'lead-enrich-directional-spawn'
  })
  assert.equal(directionalSpawn.applied, true)

  const rumorTexts = memoryStore.getSnapshot().world.rumors
    .map(rumor => String(rumor.text || '').toLowerCase())
  const directional = rumorTexts.some(text => /(east|west|north|south|ridge|well|bridge|birch|gate)/i.test(text))
  assert.equal(directional, true)

  const replayBaseline = memoryStore.getSnapshot()
  const replay = await service.applyGodCommand({
    agents,
    command: 'event draw alpha',
    operationId: `lead-enrich-draw-${drawIdx}`
  })
  assert.equal(replay.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), replayBaseline)
})

test('town board keeps Trader-first ordering with read-only behavior', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'board-enrich-seed-alpha'
  })
  await service.applyGodCommand({
    agents,
    command: 'market add bazaar alpha_hall',
    operationId: 'board-enrich-seed-market'
  })
  await service.applyGodCommand({
    agents,
    command: 'event seed 456',
    operationId: 'board-enrich-seed-event'
  })
  await service.applyGodCommand({
    agents,
    command: 'clock advance 2',
    operationId: 'board-enrich-seed-day2'
  })

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const before = memoryStore.getSnapshot()
  const board = await service.applyGodCommand({
    agents,
    command: 'town board alpha 10',
    operationId: 'board-enrich-readonly'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(board.applied, true)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD TRADER TIP:')))
  assert.equal(board.outputLines.some(line => line.includes('OPEN DECISION')), false)

  const pulseIdx = board.outputLines.findIndex(line => line.includes('GOD TOWN BOARD MARKET PULSE HOT:'))
  const contractsIdx = board.outputLines.findIndex(line => line.includes('GOD TOWN BOARD CONTRACTS AVAILABLE:'))
  const rumorIdx = board.outputLines.findIndex(line => line.includes('GOD TOWN BOARD RUMOR LEADS:'))
  const clockIdx = board.outputLines.findIndex(line => line.includes('GOD TOWN BOARD CLOCK:'))
  assert.ok(pulseIdx >= 0)
  assert.ok(contractsIdx > pulseIdx)
  assert.ok(rumorIdx > contractsIdx)
  assert.ok(clockIdx > rumorIdx)
})

test('night caravan trouble warning is deterministic and replay-safe', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'nightwarn-seed-alpha'
  })
  await service.applyGodCommand({
    agents,
    command: 'threat set alpha 95',
    operationId: 'nightwarn-seed-threat'
  })

  let targetDay = 1
  for (let day = 1; day <= 20; day += 1) {
    const roll = stableHashNumber(`alpha:${day}:dawn:caravan_trouble`) % 100
    if (roll < 60) {
      targetDay = day
      break
    }
  }
  await memoryStore.transact((memory) => {
    memory.world.clock = {
      day: targetDay,
      phase: 'day',
      season: 'dawn',
      updated_at: '2026-02-23T00:00:00.000Z'
    }
  }, { eventId: 'nightwarn-seed-clock' })

  const first = await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'nightwarn-advance-a'
  })
  assert.equal(first.applied, true)
  const afterFirst = memoryStore.getSnapshot()
  const warningLines = afterFirst.world.news
    .filter(entry => String(entry.msg || '').includes('Night report: caravan trouble'))
  assert.ok(warningLines.length >= 1)

  const replay = await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'nightwarn-advance-a'
  })
  assert.equal(replay.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), afterFirst)
})
