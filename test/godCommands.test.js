const fs = require('fs')
const os = require('os')
const path = require('path')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createMemoryStore } = require('../src/memory')
const { createGodCommandService } = require('../src/godCommands')

function createStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-god-commands-'))
  const filePath = path.join(dir, 'memory.json')
  return createMemoryStore({ filePath })
}

function createAgents() {
  return [
    { name: 'Mara', faction: 'Pilgrims', applyGodCommand: () => {} },
    { name: 'Eli', faction: 'Pilgrims', applyGodCommand: () => {} }
  ]
}

test('god mastery commands mutate durable state via transaction + eventId', async () => {
  const memoryStore = createStore()
  const seenEventIds = []
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (mutator, opts = {}) => {
    seenEventIds.push(opts.eventId || null)
    return originalTransact(mutator, opts)
  }

  const spoken = []
  const service = createGodCommandService({
    memoryStore,
    runtimeSay: ({ agent, message }) => {
      spoken.push(`${agent.name}:${message}`)
    }
  })

  const agents = createAgents()

  const leaderResult = await service.applyGodCommand({
    agents,
    command: 'leader set Mara',
    operationId: 'op-leader'
  })
  assert.equal(leaderResult.applied, true)

  const freezeResult = await service.applyGodCommand({
    agents,
    command: 'freeze Mara',
    operationId: 'op-freeze'
  })
  assert.equal(freezeResult.applied, true)

  const intentResult = await service.applyGodCommand({
    agents,
    command: 'intent set Mara follow Eli',
    operationId: 'op-intent'
  })
  assert.equal(intentResult.applied, true)

  const txCountBeforeSay = seenEventIds.length
  const sayResult = await service.applyGodCommand({
    agents,
    command: 'say Mara Hold this line',
    operationId: 'op-say'
  })
  assert.equal(sayResult.applied, true)
  assert.equal(seenEventIds.length, txCountBeforeSay)

  assert.ok(seenEventIds.length >= 3)
  for (const eventId of seenEventIds) {
    assert.equal(typeof eventId, 'string')
    assert.ok(eventId.length > 0)
  }
  assert.ok(seenEventIds.some(id => id.includes('op-leader')))
  assert.ok(seenEventIds.some(id => id.includes('op-freeze')))
  assert.ok(seenEventIds.some(id => id.includes('op-intent')))

  const mara = memoryStore.recallAgent('Mara')
  assert.equal(mara.profile.world_intent.is_leader, true)
  assert.equal(mara.profile.world_intent.frozen, true)
  assert.equal(mara.profile.world_intent.intent, 'follow')
  assert.equal(mara.profile.world_intent.intent_target, 'Eli')
  assert.deepEqual(spoken, ['Mara:Hold this line'])
})

test('inspect commands are read-only and do not execute transactions', async () => {
  const memoryStore = createStore()
  await memoryStore.transact((memory) => {
    memory.agents.Mara = {
      short: [],
      long: [],
      summary: '',
      archive: [],
      recentUtterances: [],
      lastProcessedTime: 0,
      profile: {
        trust: 4,
        mood: 'calm',
        world_intent: {
          intent: 'wander',
          intent_target: null,
          intent_set_at: 123,
          last_action: 'scheduled:wander',
          last_action_at: 124,
          budgets: { minute_bucket: 1, events_in_min: 2 },
          manual_override: false,
          frozen: false,
          is_leader: true
        }
      }
    }
  }, { eventId: 'seed-inspect' })

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()
  const before = memoryStore.getSnapshot()

  const inspectAgent = await service.applyGodCommand({
    agents,
    command: 'inspect Mara',
    operationId: 'inspect-op-agent'
  })
  const inspectWorld = await service.applyGodCommand({
    agents,
    command: 'inspect world',
    operationId: 'inspect-op-world'
  })

  const after = memoryStore.getSnapshot()
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
  assert.equal(inspectAgent.applied, true)
  assert.equal(inspectWorld.applied, true)
  assert.ok(inspectAgent.outputLines.some(line => line.includes('GOD INSPECT AGENT')))
  assert.ok(inspectWorld.outputLines.some(line => line.includes('GOD INSPECT WORLD LOOP')))
})

test('marker add/list/remove is idempotent with duplicate operationId', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  const addA = await service.applyGodCommand({
    agents,
    command: 'mark add spawn 10 64 20 base',
    operationId: 'mark-op-add'
  })
  const addB = await service.applyGodCommand({
    agents,
    command: 'mark add spawn 10 64 20 base',
    operationId: 'mark-op-add'
  })
  assert.equal(addA.applied, true)
  assert.equal(addB.applied, false)

  const listed = await service.applyGodCommand({
    agents,
    command: 'mark list',
    operationId: 'mark-op-list'
  })
  assert.equal(listed.applied, true)
  assert.ok(listed.outputLines.some(line => line.includes('count=1')))

  const removeA = await service.applyGodCommand({
    agents,
    command: 'mark remove spawn',
    operationId: 'mark-op-remove'
  })
  const removeB = await service.applyGodCommand({
    agents,
    command: 'mark remove spawn',
    operationId: 'mark-op-remove'
  })
  assert.equal(removeA.applied, true)
  assert.equal(removeB.applied, false)

  const listedAfter = await service.applyGodCommand({
    agents,
    command: 'mark list',
    operationId: 'mark-op-list-2'
  })
  assert.ok(listedAfter.outputLines.some(line => line.includes('(none)')))

  const snapshot = memoryStore.getSnapshot()
  assert.ok(Array.isArray(snapshot.world.markers))
  assert.equal(snapshot.world.markers.length, 0)
})

test('marker duplicate-name policy is overwrite with new eventId', async () => {
  let clock = 0
  const memoryStore = createStore()
  const service = createGodCommandService({
    memoryStore,
    now: () => {
      clock += 1000
      return clock
    }
  })
  const agents = createAgents()

  const first = await service.applyGodCommand({
    agents,
    command: 'mark add alpha 1 2 3 first',
    operationId: 'mark-overwrite-1'
  })
  const second = await service.applyGodCommand({
    agents,
    command: 'mark add alpha 9 8 7 second',
    operationId: 'mark-overwrite-2'
  })
  const snapshotAfterSecond = memoryStore.getSnapshot()
  const replaySecondEvent = await service.applyGodCommand({
    agents,
    command: 'mark add alpha 4 5 6 replay-ignored',
    operationId: 'mark-overwrite-2'
  })
  const snapshotAfterReplay = memoryStore.getSnapshot()

  assert.equal(first.applied, true)
  assert.equal(second.applied, true)
  assert.equal(replaySecondEvent.applied, false)
  assert.deepEqual(snapshotAfterReplay, snapshotAfterSecond)

  const markers = snapshotAfterSecond.world.markers
  assert.equal(markers.length, 1)
  assert.equal(markers[0].name, 'alpha')
  assert.equal(markers[0].x, 9)
  assert.equal(markers[0].y, 8)
  assert.equal(markers[0].z, 7)
  assert.equal(markers[0].tag, 'second')
  assert.equal(markers[0].created_at, 2000)
})

test('marker command accepts decimals and rejects non-finite coordinates without mutation', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  const decimal = await service.applyGodCommand({
    agents,
    command: 'mark add decimal 1.5 64.25 -10.75 slope',
    operationId: 'mark-decimal'
  })
  assert.equal(decimal.applied, true)

  const marker = memoryStore.getSnapshot().world.markers.find(item => item.name === 'decimal')
  assert.equal(marker.x, 1.5)
  assert.equal(marker.y, 64.25)
  assert.equal(marker.z, -10.75)

  async function assertInvalidNoMutation(command, operationId) {
    const before = memoryStore.getSnapshot()
    await assert.rejects(async () => {
      await service.applyGodCommand({ agents, command, operationId })
    })
    const after = memoryStore.getSnapshot()
    assert.deepEqual(after, before)
  }

  await assertInvalidNoMutation('mark add beta NaN 64 10', 'mark-bad-1')
  await assertInvalidNoMutation('mark add beta Infinity 64 10', 'mark-bad-2')
  await assertInvalidNoMutation('mark add beta 1e309 64 10', 'mark-bad-3')
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('job set and clear are idempotent with duplicate eventId', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add home 10 64 10 base',
    operationId: 'job-home-marker'
  })

  const setA = await service.applyGodCommand({
    agents,
    command: 'job set Mara scout home',
    operationId: 'job-set-op'
  })
  const setB = await service.applyGodCommand({
    agents,
    command: 'job set Mara scout home',
    operationId: 'job-set-op'
  })
  assert.equal(setA.applied, true)
  assert.equal(setB.applied, false)

  const maraAfterSet = memoryStore.recallAgent('Mara')
  assert.equal(maraAfterSet.profile.job.role, 'scout')
  assert.equal(maraAfterSet.profile.job.home_marker, 'home')

  const clearA = await service.applyGodCommand({
    agents,
    command: 'job clear Mara',
    operationId: 'job-clear-op'
  })
  const clearB = await service.applyGodCommand({
    agents,
    command: 'job clear Mara',
    operationId: 'job-clear-op'
  })
  assert.equal(clearA.applied, true)
  assert.equal(clearB.applied, false)

  const maraAfterClear = memoryStore.recallAgent('Mara')
  assert.equal(Object.prototype.hasOwnProperty.call(maraAfterClear.profile, 'job'), false)
})

test('job validation failures are clean no-ops (unknown agent/role/marker)', async () => {
  const memoryStore = createStore()
  const agents = createAgents()
  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()

  const unknownAgent = await service.applyGodCommand({
    agents,
    command: 'job set Ghost scout',
    operationId: 'job-invalid-agent'
  })
  const invalidRole = await service.applyGodCommand({
    agents,
    command: 'job set Mara wizard',
    operationId: 'job-invalid-role'
  })
  const missingMarker = await service.applyGodCommand({
    agents,
    command: 'job set Mara scout nowhere',
    operationId: 'job-invalid-marker'
  })

  assert.equal(unknownAgent.applied, false)
  assert.equal(invalidRole.applied, false)
  assert.equal(missingMarker.applied, false)
  const after = memoryStore.getSnapshot()
  assert.deepEqual(after, before)
  assert.equal(txCalls, 0)
})

test('roster command is read-only', async () => {
  const memoryStore = createStore()
  const seedService = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await seedService.applyGodCommand({
    agents,
    command: 'job set Mara guard',
    operationId: 'roster-seed'
  })

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()
  const roster = await service.applyGodCommand({
    agents,
    command: 'roster',
    operationId: 'roster-readonly'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(roster.applied, true)
  assert.ok(roster.outputLines.some(line => line.includes('GOD ROSTER')))
  const maraRow = roster.outputLines.find(line => line.startsWith('GOD ROSTER ENTRY: Mara '))
  const eliRow = roster.outputLines.find(line => line.startsWith('GOD ROSTER ENTRY: Eli '))
  assert.ok(maraRow)
  assert.ok(eliRow)
  assert.match(maraRow, /role=guard/)
  assert.match(maraRow, /home_marker=-/)
  assert.match(maraRow, /assigned_at=\d{4}-\d{2}-\d{2}T/)
  assert.match(eliRow, /role=none/)
  assert.match(eliRow, /home_marker=-/)
  assert.match(eliRow, /assigned_at=-/)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
})

test('economy mint is transactional and idempotent', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  const mintA = await service.applyGodCommand({
    agents,
    command: 'mint Mara 10',
    operationId: 'eco-mint-a'
  })
  const mintAReplay = await service.applyGodCommand({
    agents,
    command: 'mint Mara 10',
    operationId: 'eco-mint-a'
  })
  const mintB = await service.applyGodCommand({
    agents,
    command: 'mint Mara 10',
    operationId: 'eco-mint-b'
  })

  assert.equal(mintA.applied, true)
  assert.equal(mintAReplay.applied, false)
  assert.equal(mintB.applied, true)

  const snapshot = memoryStore.getSnapshot()
  assert.equal(snapshot.world.economy.currency, 'emerald')
  assert.equal(snapshot.world.economy.ledger.Mara, 20)
  assert.equal(snapshot.world.economy.minted_total, 20)
})

test('economy transfer is transactional and idempotent', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mint Mara 10',
    operationId: 'eco-seed-transfer'
  })

  const transferA = await service.applyGodCommand({
    agents,
    command: 'transfer Mara Eli 7',
    operationId: 'eco-transfer-a'
  })
  const transferReplay = await service.applyGodCommand({
    agents,
    command: 'transfer Mara Eli 7',
    operationId: 'eco-transfer-a'
  })

  assert.equal(transferA.applied, true)
  assert.equal(transferReplay.applied, false)

  const snapshot = memoryStore.getSnapshot()
  assert.equal(snapshot.world.economy.ledger.Mara, 3)
  assert.equal(snapshot.world.economy.ledger.Eli, 7)
})

test('economy validation failures are clean no-ops', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mint Mara 10',
    operationId: 'eco-seed-validation'
  })

  async function assertValidationNoMutation(command, operationId) {
    const before = memoryStore.getSnapshot()
    const result = await service.applyGodCommand({ agents, command, operationId })
    const after = memoryStore.getSnapshot()
    assert.equal(result.applied, false)
    assert.deepEqual(after, before)
    assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
  }

  await assertValidationNoMutation('mint Ghost 10', 'eco-invalid-1')
  await assertValidationNoMutation('mint Mara NaN', 'eco-invalid-2')
  await assertValidationNoMutation('mint Mara Infinity', 'eco-invalid-3')
  await assertValidationNoMutation('mint Mara -5', 'eco-invalid-4')
  await assertValidationNoMutation('transfer Mara Eli 999', 'eco-invalid-5')
  await assertValidationNoMutation('transfer Ghost Eli 1', 'eco-invalid-6')
})

test('economy read-only commands do not mutate state', async () => {
  const memoryStore = createStore()
  const seedService = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await seedService.applyGodCommand({
    agents,
    command: 'mint Mara 10',
    operationId: 'eco-readonly-seed-1'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'mint Eli 4',
    operationId: 'eco-readonly-seed-2'
  })

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()

  const balance = await service.applyGodCommand({
    agents,
    command: 'balance Mara',
    operationId: 'eco-readonly-balance'
  })
  const overview = await service.applyGodCommand({
    agents,
    command: 'economy',
    operationId: 'eco-readonly-economy'
  })

  const after = memoryStore.getSnapshot()
  assert.equal(balance.applied, true)
  assert.equal(overview.applied, true)
  assert.ok(balance.outputLines.some(line => line.includes('GOD BALANCE: Mara balance=10 currency=emerald')))
  assert.ok(overview.outputLines.some(line => line.includes('GOD ECONOMY: currency=emerald')))
  assert.ok(overview.outputLines.some(line => line.includes('GOD ECONOMY TOP: rank=1 agent=Mara balance=10')))
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
})

test('economy amount policy is integers only (decimal mint is no-op)', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  const before = memoryStore.getSnapshot()
  const result = await service.applyGodCommand({
    agents,
    command: 'mint Mara 1.5',
    operationId: 'eco-decimal-reject'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(result.applied, false)
  assert.deepEqual(after, before)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('market add/remove are transactional and idempotent', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add hub 10 64 10 origin',
    operationId: 'market-seed-marker'
  })

  const addA = await service.applyGodCommand({
    agents,
    command: 'market add bazaar hub',
    operationId: 'market-add-a'
  })
  const addReplay = await service.applyGodCommand({
    agents,
    command: 'market add bazaar hub',
    operationId: 'market-add-a'
  })

  assert.equal(addA.applied, true)
  assert.equal(addReplay.applied, false)
  assert.equal(memoryStore.getSnapshot().world.markets.length, 1)

  const removeA = await service.applyGodCommand({
    agents,
    command: 'market remove bazaar',
    operationId: 'market-remove-a'
  })
  const removeReplay = await service.applyGodCommand({
    agents,
    command: 'market remove bazaar',
    operationId: 'market-remove-a'
  })

  assert.equal(removeA.applied, true)
  assert.equal(removeReplay.applied, false)
  assert.equal(memoryStore.getSnapshot().world.markets.length, 0)
})

test('offer add/cancel are transactional and idempotent', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'market add plaza',
    operationId: 'offer-seed-market'
  })

  const addA = await service.applyGodCommand({
    agents,
    command: 'offer add plaza Mara sell 5 3',
    operationId: 'offer-add-a'
  })
  const addReplay = await service.applyGodCommand({
    agents,
    command: 'offer add plaza Mara sell 5 3',
    operationId: 'offer-add-a'
  })
  assert.equal(addA.applied, true)
  assert.equal(addReplay.applied, false)

  const snapshotAfterAdd = memoryStore.getSnapshot()
  const marketAfterAdd = snapshotAfterAdd.world.markets.find(item => item.name === 'plaza')
  assert.ok(marketAfterAdd)
  assert.equal(marketAfterAdd.offers.length, 1)
  const offerId = marketAfterAdd.offers[0].offer_id
  assert.equal(marketAfterAdd.offers[0].active, true)

  const cancelA = await service.applyGodCommand({
    agents,
    command: `offer cancel plaza ${offerId}`,
    operationId: 'offer-cancel-a'
  })
  const cancelReplay = await service.applyGodCommand({
    agents,
    command: `offer cancel plaza ${offerId}`,
    operationId: 'offer-cancel-a'
  })
  assert.equal(cancelA.applied, true)
  assert.equal(cancelReplay.applied, false)

  const snapshotAfterCancel = memoryStore.getSnapshot()
  const marketAfterCancel = snapshotAfterCancel.world.markets.find(item => item.name === 'plaza')
  assert.equal(marketAfterCancel.offers[0].active, false)
})

test('trade is transactional and idempotent (no double-transfer on replay)', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'market add plaza',
    operationId: 'trade-seed-market'
  })
  await service.applyGodCommand({
    agents,
    command: 'offer add plaza Mara sell 10 2',
    operationId: 'trade-seed-offer'
  })
  await service.applyGodCommand({
    agents,
    command: 'mint Eli 20',
    operationId: 'trade-seed-balance'
  })

  const offerId = memoryStore.getSnapshot().world.markets[0].offers[0].offer_id
  const tradeA = await service.applyGodCommand({
    agents,
    command: `trade plaza ${offerId} Eli 3`,
    operationId: 'trade-op-a'
  })
  const tradeReplay = await service.applyGodCommand({
    agents,
    command: `trade plaza ${offerId} Eli 3`,
    operationId: 'trade-op-a'
  })

  assert.equal(tradeA.applied, true)
  assert.equal(tradeReplay.applied, false)

  const snapshot = memoryStore.getSnapshot()
  assert.equal(snapshot.world.economy.ledger.Eli, 14)
  assert.equal(snapshot.world.economy.ledger.Mara, 6)
  assert.equal(snapshot.world.markets[0].offers[0].amount, 7)
  assert.equal(snapshot.world.markets[0].offers[0].active, true)
})

test('market validation failures are clean no-ops', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'market add plaza',
    operationId: 'market-invalid-seed-market'
  })
  await service.applyGodCommand({
    agents,
    command: 'offer add plaza Mara sell 2 5',
    operationId: 'market-invalid-seed-offer-expensive'
  })
  await service.applyGodCommand({
    agents,
    command: 'offer add plaza Mara sell 2 1',
    operationId: 'market-invalid-seed-offer-cheap'
  })
  await service.applyGodCommand({
    agents,
    command: 'mint Eli 4',
    operationId: 'market-invalid-seed-balance'
  })

  const offers = memoryStore.getSnapshot().world.markets[0].offers
  const expensiveOfferId = offers.find(item => item.price === 5).offer_id
  const cheapOfferId = offers.find(item => item.price === 1).offer_id

  async function assertValidationNoMutation(command, operationId) {
    const before = memoryStore.getSnapshot()
    const result = await service.applyGodCommand({ agents, command, operationId })
    const after = memoryStore.getSnapshot()
    assert.equal(result.applied, false)
    assert.deepEqual(after, before)
    assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
  }

  await assertValidationNoMutation('offer add nowhere Mara sell 1 1', 'market-invalid-1')
  await assertValidationNoMutation('offer add plaza Mara sell 1.5 1', 'market-invalid-2')
  await assertValidationNoMutation('offer add plaza Mara sell 1 1.5', 'market-invalid-3')
  await assertValidationNoMutation(`trade plaza ${expensiveOfferId} Eli 1`, 'market-invalid-4')
  await assertValidationNoMutation(`trade plaza ${cheapOfferId} Eli 3`, 'market-invalid-5')
  await assertValidationNoMutation('offer add plaza Ghost sell 1 1', 'market-invalid-6')
})

test('market list and offer list are read-only', async () => {
  const memoryStore = createStore()
  const seedService = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await seedService.applyGodCommand({
    agents,
    command: 'market add plaza',
    operationId: 'market-readonly-seed-market'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'offer add plaza Mara buy 4 2',
    operationId: 'market-readonly-seed-offer'
  })

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()
  const marketList = await service.applyGodCommand({
    agents,
    command: 'market list',
    operationId: 'market-readonly-list'
  })
  const offerList = await service.applyGodCommand({
    agents,
    command: 'offer list plaza',
    operationId: 'market-readonly-offer-list'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(marketList.applied, true)
  assert.equal(offerList.applied, true)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
})

test('town list and town board are read-only', async () => {
  const memoryStore = createStore()
  const seedService = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await seedService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 10 64 10 town:alpha',
    operationId: 'town-seed-marker'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'job set Mara guard alpha_hall',
    operationId: 'town-seed-job'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'market add alpha_market alpha_hall',
    operationId: 'town-seed-market'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'mint Mara 9',
    operationId: 'town-seed-balance'
  })

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()
  const townList = await service.applyGodCommand({
    agents,
    command: 'town list',
    operationId: 'town-readonly-list'
  })
  const townBoard = await service.applyGodCommand({
    agents,
    command: 'town board alpha 10',
    operationId: 'town-readonly-board'
  })
  const unknownTown = await service.applyGodCommand({
    agents,
    command: 'town board nowhere',
    operationId: 'town-readonly-missing'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(townList.applied, true)
  assert.equal(townBoard.applied, true)
  assert.equal(unknownTown.applied, false)
  assert.ok(townList.outputLines.some(line => line.includes('GOD TOWN: name=alpha') && line.includes('tag=town:alpha')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD: town=alpha')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD MARKETS:')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD OFFERS:')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD MOOD: town=alpha')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD EVENTS:')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD AGENT: name=Mara') && line.includes('role=guard') && line.includes('home_marker=alpha_hall') && line.includes('balance=9')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD AGENT: name=Eli') && line.includes('role=none') && line.includes('home_marker=-') && line.includes('assigned_at=-') && line.includes('balance=0')))
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
})

test('chronicle add is transactional and idempotent', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'chronicle-seed-town'
  })

  const addA = await service.applyGodCommand({
    agents,
    command: 'chronicle add note harvest-ready town=alpha',
    operationId: 'chronicle-add-a'
  })
  const addAReplay = await service.applyGodCommand({
    agents,
    command: 'chronicle add note harvest-ready town=alpha',
    operationId: 'chronicle-add-a'
  })
  const addB = await service.applyGodCommand({
    agents,
    command: 'chronicle add note harvest-ready town=alpha',
    operationId: 'chronicle-add-b'
  })

  assert.equal(addA.applied, true)
  assert.equal(addAReplay.applied, false)
  assert.equal(addB.applied, true)

  const snapshot = memoryStore.getSnapshot()
  const notes = snapshot.world.chronicle.filter(entry => entry.type === 'note' && entry.msg === 'harvest-ready')
  assert.equal(notes.length, 2)
  assert.equal(notes[0].town, 'alpha')
  assert.equal(notes[1].town, 'alpha')
})

test('chronicle append policy keeps the latest 200 entries', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  for (let idx = 0; idx < 205; idx += 1) {
    const result = await service.applyGodCommand({
      agents,
      command: `chronicle add test msg-${idx}`,
      operationId: `chronicle-cap-${idx}`
    })
    assert.equal(result.applied, true)
  }

  const snapshot = memoryStore.getSnapshot()
  assert.equal(snapshot.world.chronicle.length, 200)
  assert.equal(snapshot.world.chronicle[0].msg, 'msg-5')
  assert.equal(snapshot.world.chronicle[snapshot.world.chronicle.length - 1].msg, 'msg-204')
})

test('auto-chronicle and auto-news hooks append on marker/job/trade and remain idempotent', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 5 64 5 town:alpha',
    operationId: 'auto-chronicle-marker'
  })
  await service.applyGodCommand({
    agents,
    command: 'job set Mara scout alpha_hall',
    operationId: 'auto-chronicle-job'
  })
  await service.applyGodCommand({
    agents,
    command: 'market add alpha_market alpha_hall',
    operationId: 'auto-chronicle-market'
  })
  await service.applyGodCommand({
    agents,
    command: 'offer add alpha_market Mara sell 5 2',
    operationId: 'auto-chronicle-offer'
  })
  await service.applyGodCommand({
    agents,
    command: 'mint Eli 20',
    operationId: 'auto-chronicle-mint'
  })

  const offerId = memoryStore.getSnapshot().world.markets[0].offers[0].offer_id
  const tradeA = await service.applyGodCommand({
    agents,
    command: `trade alpha_market ${offerId} Eli 2`,
    operationId: 'auto-chronicle-trade'
  })
  assert.equal(tradeA.applied, true)

  const snapshotAfterTrade = memoryStore.getSnapshot()
  const types = snapshotAfterTrade.world.chronicle.map(entry => entry.type)
  const topics = snapshotAfterTrade.world.news.map(entry => entry.topic)
  assert.ok(types.includes('marker_add'))
  assert.ok(types.includes('job_set'))
  assert.ok(types.includes('trade'))
  assert.ok(topics.includes('marker'))
  assert.ok(topics.includes('job'))
  assert.ok(topics.includes('trade'))

  const chronicleLenBeforeReplay = snapshotAfterTrade.world.chronicle.length
  const newsLenBeforeReplay = snapshotAfterTrade.world.news.length
  const tradeReplay = await service.applyGodCommand({
    agents,
    command: `trade alpha_market ${offerId} Eli 2`,
    operationId: 'auto-chronicle-trade'
  })
  const snapshotAfterReplay = memoryStore.getSnapshot()
  assert.equal(tradeReplay.applied, false)
  assert.equal(snapshotAfterReplay.world.chronicle.length, chronicleLenBeforeReplay)
  assert.equal(snapshotAfterReplay.world.news.length, newsLenBeforeReplay)
})

test('news tail is read-only and news writes are transactional/idempotent', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 5 64 5 settlement:alpha',
    operationId: 'news-seed-marker'
  })
  const marketAddA = await service.applyGodCommand({
    agents,
    command: 'market add alpha_market alpha_hall',
    operationId: 'news-market-add'
  })
  const marketAddReplay = await service.applyGodCommand({
    agents,
    command: 'market add alpha_market alpha_hall',
    operationId: 'news-market-add'
  })
  assert.equal(marketAddA.applied, true)
  assert.equal(marketAddReplay.applied, false)

  await service.applyGodCommand({
    agents,
    command: 'offer add alpha_market Mara sell 2 3',
    operationId: 'news-seed-offer'
  })
  await service.applyGodCommand({
    agents,
    command: 'mint Eli 20',
    operationId: 'news-seed-mint'
  })
  const offerId = memoryStore.getSnapshot().world.markets[0].offers[0].offer_id
  const tradeA = await service.applyGodCommand({
    agents,
    command: `trade alpha_market ${offerId} Eli 1`,
    operationId: 'news-trade'
  })
  const tradeReplay = await service.applyGodCommand({
    agents,
    command: `trade alpha_market ${offerId} Eli 1`,
    operationId: 'news-trade'
  })
  const tradeB = await service.applyGodCommand({
    agents,
    command: `trade alpha_market ${offerId} Eli 1`,
    operationId: 'news-trade-b'
  })
  assert.equal(tradeA.applied, true)
  assert.equal(tradeReplay.applied, false)
  assert.equal(tradeB.applied, true)

  const newsSnapshot = memoryStore.getSnapshot().world.news
  const marketItems = newsSnapshot.filter(item => item.topic === 'market')
  const tradeItems = newsSnapshot.filter(item => item.topic === 'trade')
  assert.equal(marketItems.length, 1)
  assert.equal(tradeItems.length, 2)

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const readonlyService = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()
  const newsTail = await readonlyService.applyGodCommand({
    agents,
    command: 'news tail 5',
    operationId: 'news-tail-readonly'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(newsTail.applied, true)
  assert.ok(newsTail.outputLines.some(line => line.includes('GOD NEWS TAIL')))
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
})

test('quest offer is transactional and idempotent', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'quest-offer-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'market add bazaar alpha_hall',
    operationId: 'quest-offer-seed-market'
  })

  const offerA = await service.applyGodCommand({
    agents,
    command: 'quest offer alpha trade_n 2 bazaar 7',
    operationId: 'quest-offer-a'
  })
  const offerAReplay = await service.applyGodCommand({
    agents,
    command: 'quest offer alpha trade_n 2 bazaar 7',
    operationId: 'quest-offer-a'
  })
  const offerB = await service.applyGodCommand({
    agents,
    command: 'quest offer alpha trade_n 2 bazaar 7',
    operationId: 'quest-offer-b'
  })

  assert.equal(offerA.applied, true)
  assert.equal(offerAReplay.applied, false)
  assert.equal(offerB.applied, true)

  const snapshot = memoryStore.getSnapshot()
  const quests = snapshot.world.quests
  assert.equal(quests.length, 2)
  assert.notEqual(quests[0].id, quests[1].id)
  assert.equal(quests[0].type, 'trade_n')
  assert.equal(quests[1].type, 'trade_n')
  assert.equal(quests[0].state, 'offered')
  assert.equal(quests[1].state, 'offered')
  assert.equal(quests[0].objective.n, 2)
  assert.equal(quests[1].objective.n, 2)
  assert.equal(quests[0].objective.market, 'bazaar')
  assert.equal(quests[1].objective.market, 'bazaar')
  assert.equal(quests[0].reward, 7)
  assert.equal(quests[1].reward, 7)

  const chronicleOffers = snapshot.world.chronicle.filter(entry => entry.type === 'quest_offer')
  const newsOffers = snapshot.world.news.filter(entry => entry.topic === 'quest' && entry.msg.includes('offered'))
  assert.equal(chronicleOffers.length, 2)
  assert.equal(newsOffers.length, 2)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('quest accept is transactional and idempotent and invalid accepts are no-ops', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'quest-accept-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'quest offer alpha visit_town alpha 6',
    operationId: 'quest-accept-offer'
  })
  const questId = memoryStore.getSnapshot().world.quests[0].id

  const acceptA = await service.applyGodCommand({
    agents,
    command: `quest accept Mara ${questId}`,
    operationId: 'quest-accept-a'
  })
  const acceptReplay = await service.applyGodCommand({
    agents,
    command: `quest accept Mara ${questId}`,
    operationId: 'quest-accept-a'
  })
  const afterAccept = memoryStore.getSnapshot()

  const invalidUnknownAgent = await service.applyGodCommand({
    agents,
    command: `quest accept Ghost ${questId}`,
    operationId: 'quest-accept-invalid-agent'
  })
  const afterUnknownAgent = memoryStore.getSnapshot()

  const invalidAlreadyAccepted = await service.applyGodCommand({
    agents,
    command: `quest accept Eli ${questId}`,
    operationId: 'quest-accept-invalid-state'
  })
  const afterInvalidState = memoryStore.getSnapshot()

  assert.equal(acceptA.applied, true)
  assert.equal(acceptReplay.applied, false)
  assert.equal(invalidUnknownAgent.applied, false)
  assert.equal(invalidAlreadyAccepted.applied, false)

  const acceptedQuest = afterAccept.world.quests.find(item => item.id === questId)
  assert.equal(acceptedQuest.state, 'accepted')
  assert.equal(acceptedQuest.owner, 'Mara')
  assert.equal(typeof acceptedQuest.accepted_at, 'string')

  assert.deepEqual(afterUnknownAgent, afterAccept)
  assert.deepEqual(afterInvalidState, afterAccept)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('trade_n progress is buyer-owned and increments exactly once under replay', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 5 64 5 town:alpha',
    operationId: 'quest-trade-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'market add bazaar alpha_hall',
    operationId: 'quest-trade-seed-market'
  })
  await service.applyGodCommand({
    agents,
    command: 'mint Mara 30',
    operationId: 'quest-trade-seed-mara-balance'
  })

  await service.applyGodCommand({
    agents,
    command: 'quest offer alpha trade_n 2 bazaar 5',
    operationId: 'quest-trade-offer-buyer'
  })
  const buyerQuestId = memoryStore.getSnapshot().world.quests[0].id
  await service.applyGodCommand({
    agents,
    command: `quest accept Mara ${buyerQuestId}`,
    operationId: 'quest-trade-accept-buyer'
  })

  await service.applyGodCommand({
    agents,
    command: 'quest offer alpha trade_n 2 bazaar 9',
    operationId: 'quest-trade-offer-seller'
  })
  const sellerQuestId = memoryStore.getSnapshot().world.quests[1].id
  await service.applyGodCommand({
    agents,
    command: `quest accept Eli ${sellerQuestId}`,
    operationId: 'quest-trade-accept-seller'
  })

  await service.applyGodCommand({
    agents,
    command: 'offer add bazaar Eli sell 5 2',
    operationId: 'quest-trade-seed-offer'
  })
  const offerId = memoryStore.getSnapshot().world.markets[0].offers[0].offer_id

  const tradeA = await service.applyGodCommand({
    agents,
    command: `trade bazaar ${offerId} Mara 1`,
    operationId: 'quest-trade-a'
  })
  const tradeAReplay = await service.applyGodCommand({
    agents,
    command: `trade bazaar ${offerId} Mara 1`,
    operationId: 'quest-trade-a'
  })
  const snapshotAfterReplayA = memoryStore.getSnapshot()
  const buyerAfterA = snapshotAfterReplayA.world.quests.find(item => item.id === buyerQuestId)
  const sellerAfterA = snapshotAfterReplayA.world.quests.find(item => item.id === sellerQuestId)
  assert.equal(tradeA.applied, true)
  assert.equal(tradeAReplay.applied, false)
  assert.equal(buyerAfterA.progress.done, 1)
  assert.equal(buyerAfterA.state, 'in_progress')
  assert.equal(sellerAfterA.progress.done, 0)
  assert.equal(sellerAfterA.state, 'accepted')

  const tradeB = await service.applyGodCommand({
    agents,
    command: `trade bazaar ${offerId} Mara 1`,
    operationId: 'quest-trade-b'
  })
  const tradeBReplay = await service.applyGodCommand({
    agents,
    command: `trade bazaar ${offerId} Mara 1`,
    operationId: 'quest-trade-b'
  })
  assert.equal(tradeB.applied, true)
  assert.equal(tradeBReplay.applied, false)

  const snapshot = memoryStore.getSnapshot()
  const buyerQuest = snapshot.world.quests.find(item => item.id === buyerQuestId)
  const sellerQuest = snapshot.world.quests.find(item => item.id === sellerQuestId)
  assert.equal(buyerQuest.progress.done, 2)
  assert.equal(buyerQuest.state, 'completed')
  assert.equal(sellerQuest.progress.done, 0)
  assert.equal(sellerQuest.state, 'accepted')
  assert.equal(snapshot.world.economy.ledger.Mara, 31)

  const buyerCompletions = snapshot.world.chronicle.filter(entry => entry.type === 'quest_complete' && entry.meta?.quest_id === buyerQuestId)
  assert.equal(buyerCompletions.length, 1)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('quest complete pays reward exactly once under replay', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'quest-complete-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'quest offer alpha trade_n 1 4',
    operationId: 'quest-complete-offer'
  })
  const questId = memoryStore.getSnapshot().world.quests[0].id
  await service.applyGodCommand({
    agents,
    command: `quest accept Mara ${questId}`,
    operationId: 'quest-complete-accept'
  })

  await memoryStore.transact((memory) => {
    const idx = memory.world.quests.findIndex(item => item.id === questId)
    memory.world.quests[idx].state = 'in_progress'
    memory.world.quests[idx].progress = { done: 1 }
  }, { eventId: 'quest-complete-seed-progress' })

  const completeA = await service.applyGodCommand({
    agents,
    command: `quest complete ${questId}`,
    operationId: 'quest-complete-a'
  })
  const completeReplay = await service.applyGodCommand({
    agents,
    command: `quest complete ${questId}`,
    operationId: 'quest-complete-a'
  })

  assert.equal(completeA.applied, true)
  assert.equal(completeReplay.applied, false)

  const snapshot = memoryStore.getSnapshot()
  const quest = snapshot.world.quests.find(item => item.id === questId)
  assert.equal(quest.state, 'completed')
  assert.equal(snapshot.world.economy.ledger.Mara, 4)

  const completions = snapshot.world.news.filter(entry => entry.topic === 'quest' && entry.meta?.quest_id === questId && entry.msg.includes('completed'))
  assert.equal(completions.length, 1)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('visit_town quest visit command completes once and rewards once', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'quest-visit-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'quest offer alpha visit_town alpha 6',
    operationId: 'quest-visit-offer'
  })
  const questId = memoryStore.getSnapshot().world.quests[0].id
  await service.applyGodCommand({
    agents,
    command: `quest accept Mara ${questId}`,
    operationId: 'quest-visit-accept'
  })

  const visitA = await service.applyGodCommand({
    agents,
    command: `quest visit ${questId}`,
    operationId: 'quest-visit-a'
  })
  const visitReplay = await service.applyGodCommand({
    agents,
    command: `quest visit ${questId}`,
    operationId: 'quest-visit-a'
  })

  assert.equal(visitA.applied, true)
  assert.equal(visitReplay.applied, false)

  const snapshot = memoryStore.getSnapshot()
  const quest = snapshot.world.quests.find(item => item.id === questId)
  assert.equal(quest.state, 'completed')
  assert.equal(quest.progress.visited, true)
  assert.equal(snapshot.world.economy.ledger.Mara, 6)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('quest list/show and town board remain read-only and expose quest sections', async () => {
  const memoryStore = createStore()
  const seedService = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await seedService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 10 64 10 town:alpha',
    operationId: 'quest-readonly-seed-town'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'quest offer alpha visit_town alpha 3',
    operationId: 'quest-readonly-offer'
  })
  const questId = memoryStore.getSnapshot().world.quests[0].id

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()
  const questList = await service.applyGodCommand({
    agents,
    command: 'quest list alpha',
    operationId: 'quest-readonly-list'
  })
  const questShow = await service.applyGodCommand({
    agents,
    command: `quest show ${questId}`,
    operationId: 'quest-readonly-show'
  })
  const townBoard = await service.applyGodCommand({
    agents,
    command: 'town board alpha 10',
    operationId: 'quest-readonly-town-board'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(questList.applied, true)
  assert.equal(questShow.applied, true)
  assert.equal(townBoard.applied, true)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
  assert.ok(questList.outputLines.some(line => line.includes(`id=${questId}`)))
  assert.ok(questShow.outputLines.some(line => line.includes('GOD QUEST OBJECTIVE:')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD QUESTS AVAILABLE:')))
  assert.ok(townBoard.outputLines.some(line => line.includes(`GOD TOWN BOARD QUEST AVAILABLE: id=${questId}`)))
})

test('clock/threat/faction/rep read-only commands do not mutate and town board includes story sections', async () => {
  const memoryStore = createStore()
  const seedService = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await seedService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 10 64 10 town:alpha',
    operationId: 'story-readonly-seed-town'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'faction set alpha iron_pact',
    operationId: 'story-readonly-seed-faction'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'rep add Mara iron_pact 2',
    operationId: 'story-readonly-seed-rep'
  })

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()

  const clock = await service.applyGodCommand({
    agents,
    command: 'clock',
    operationId: 'story-readonly-clock'
  })
  const threat = await service.applyGodCommand({
    agents,
    command: 'threat alpha',
    operationId: 'story-readonly-threat'
  })
  const factionList = await service.applyGodCommand({
    agents,
    command: 'faction list',
    operationId: 'story-readonly-faction-list'
  })
  const rep = await service.applyGodCommand({
    agents,
    command: 'rep Mara',
    operationId: 'story-readonly-rep'
  })
  const townBoard = await service.applyGodCommand({
    agents,
    command: 'town board alpha 10',
    operationId: 'story-readonly-town-board'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(clock.applied, true)
  assert.equal(threat.applied, true)
  assert.equal(factionList.applied, true)
  assert.equal(rep.applied, true)
  assert.equal(townBoard.applied, true)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
  assert.ok(clock.outputLines.some(line => line.includes('GOD CLOCK:')))
  assert.ok(threat.outputLines.some(line => line.includes('GOD THREAT: town=alpha')))
  assert.ok(factionList.outputLines.some(line => line.includes('GOD FACTION: name=iron_pact')))
  assert.ok(rep.outputLines.some(line => line.includes('GOD REP: agent=Mara')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD CLOCK:')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD THREAT: town=alpha')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD MOOD: town=alpha')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD FACTION: town=alpha')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD AGENT: name=Mara') && line.includes('rep_faction=') && line.includes('rep=')))
})

test('clock advance is transactional and idempotent with deterministic threat updates', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'clock-advance-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'event seed 1',
    operationId: 'clock-advance-seed-events'
  })

  const advanceA = await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'clock-advance-a'
  })
  const snapshotAfterA = memoryStore.getSnapshot()

  const advanceReplay = await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'clock-advance-a'
  })
  const snapshotAfterReplay = memoryStore.getSnapshot()

  const advanceB = await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'clock-advance-b'
  })
  const snapshotAfterB = memoryStore.getSnapshot()

  assert.equal(advanceA.applied, true)
  assert.equal(advanceReplay.applied, false)
  assert.equal(advanceB.applied, true)

  assert.equal(snapshotAfterA.world.clock.day, 1)
  assert.equal(snapshotAfterA.world.clock.phase, 'night')
  assert.equal(snapshotAfterA.world.threat.byTown.alpha, 5)
  assert.equal(snapshotAfterA.world.moods.byTown.alpha.fear, 3)

  assert.deepEqual(snapshotAfterReplay, snapshotAfterA)

  assert.equal(snapshotAfterB.world.clock.day, 2)
  assert.equal(snapshotAfterB.world.clock.phase, 'day')
  assert.equal(snapshotAfterB.world.threat.byTown.alpha, 2)
  assert.equal(snapshotAfterB.world.moods.byTown.alpha.fear, 1)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('clock season command is transactional, idempotent, and validates input', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  const seasonA = await service.applyGodCommand({
    agents,
    command: 'clock season long_night',
    operationId: 'clock-season-a'
  })
  const seasonReplay = await service.applyGodCommand({
    agents,
    command: 'clock season long_night',
    operationId: 'clock-season-a'
  })
  assert.equal(seasonA.applied, true)
  assert.equal(seasonReplay.applied, false)

  const afterSeason = memoryStore.getSnapshot()
  assert.equal(afterSeason.world.clock.season, 'long_night')
  assert.ok(afterSeason.world.chronicle.some(entry => entry.type === 'clock' && entry.msg.includes('Season shifts to long_night')))
  assert.ok(afterSeason.world.news.some(entry => entry.topic === 'world' && entry.msg.includes('Season shifts to long_night')))

  const beforeInvalid = memoryStore.getSnapshot()
  const invalid = await service.applyGodCommand({
    agents,
    command: 'clock season monsoon',
    operationId: 'clock-season-invalid'
  })
  const afterInvalid = memoryStore.getSnapshot()
  assert.equal(invalid.applied, false)
  assert.deepEqual(afterInvalid, beforeInvalid)
})

test('threat set clamps values and rejects non-finite inputs with no mutation', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'threat-set-seed-town'
  })

  const setClamp = await service.applyGodCommand({
    agents,
    command: 'threat set alpha -1',
    operationId: 'threat-set-clamp'
  })
  assert.equal(setClamp.applied, true)
  const snapshotAfterClamp = memoryStore.getSnapshot()
  assert.equal(snapshotAfterClamp.world.threat.byTown.alpha, 0)
  assert.ok(snapshotAfterClamp.world.chronicle.some(entry => entry.type === 'threat' && entry.msg.includes('Threat set to 0')))
  assert.ok(snapshotAfterClamp.world.news.some(entry => entry.topic === 'world' && entry.msg.includes('Threat set to 0')))

  async function assertValidationNoMutation(command, operationId) {
    const before = memoryStore.getSnapshot()
    const result = await service.applyGodCommand({ agents, command, operationId })
    const after = memoryStore.getSnapshot()
    assert.equal(result.applied, false)
    assert.deepEqual(after, before)
  }

  await assertValidationNoMutation('threat set alpha NaN', 'threat-set-invalid-nan')
  await assertValidationNoMutation('threat set alpha Infinity', 'threat-set-invalid-inf')
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('faction set validates unknown town/faction and applies valid assignment', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'faction-set-seed-town'
  })

  const before = memoryStore.getSnapshot()
  const unknownTown = await service.applyGodCommand({
    agents,
    command: 'faction set nowhere iron_pact',
    operationId: 'faction-set-invalid-town'
  })
  const afterUnknownTown = memoryStore.getSnapshot()
  const unknownFaction = await service.applyGodCommand({
    agents,
    command: 'faction set alpha guild',
    operationId: 'faction-set-invalid-faction'
  })
  const afterUnknownFaction = memoryStore.getSnapshot()

  assert.equal(unknownTown.applied, false)
  assert.equal(unknownFaction.applied, false)
  assert.deepEqual(afterUnknownTown, before)
  assert.deepEqual(afterUnknownFaction, before)

  const valid = await service.applyGodCommand({
    agents,
    command: 'faction set alpha veil_church',
    operationId: 'faction-set-valid'
  })
  assert.equal(valid.applied, true)
  const snapshot = memoryStore.getSnapshot()
  assert.ok(snapshot.world.factions.veil_church.towns.includes('alpha'))
  assert.equal(snapshot.world.factions.iron_pact.towns.includes('alpha'), false)
  assert.ok(snapshot.world.chronicle.some(entry => entry.type === 'faction' && entry.msg.includes('veil_church')))
  assert.ok(snapshot.world.news.some(entry => entry.topic === 'faction' && entry.msg.includes('veil_church')))
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('rep add enforces integer policy, validates agent/faction, and is idempotent', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  const addA = await service.applyGodCommand({
    agents,
    command: 'rep add Mara iron_pact 2',
    operationId: 'rep-add-a'
  })
  const addReplay = await service.applyGodCommand({
    agents,
    command: 'rep add Mara iron_pact 2',
    operationId: 'rep-add-a'
  })
  assert.equal(addA.applied, true)
  assert.equal(addReplay.applied, false)

  const afterAdd = memoryStore.getSnapshot()
  assert.equal(afterAdd.agents.Mara.profile.rep.iron_pact, 2)
  assert.ok(afterAdd.world.chronicle.some(entry => entry.type === 'rep' && entry.msg.includes('Mara gains favor')))
  assert.ok(afterAdd.world.news.some(entry => entry.topic === 'faction' && entry.msg.includes('Mara gains favor')))

  async function assertValidationNoMutation(command, operationId) {
    const before = memoryStore.getSnapshot()
    const result = await service.applyGodCommand({ agents, command, operationId })
    const after = memoryStore.getSnapshot()
    assert.equal(result.applied, false)
    assert.deepEqual(after, before)
  }

  await assertValidationNoMutation('rep add Mara iron_pact 1.5', 'rep-add-invalid-decimal')
  await assertValidationNoMutation('rep add Ghost iron_pact 1', 'rep-add-invalid-agent')
  await assertValidationNoMutation('rep add Mara guild 1', 'rep-add-invalid-faction')
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('clock advance narration hooks are replay-safe', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'clock-news-seed-town'
  })

  const before = memoryStore.getSnapshot()
  const beforeChronicle = before.world.chronicle.length
  const beforeNews = before.world.news.length

  const advanceA = await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'clock-news-a'
  })
  assert.equal(advanceA.applied, true)
  const afterAdvance = memoryStore.getSnapshot()
  assert.ok(afterAdvance.world.chronicle.length > beforeChronicle)
  assert.ok(afterAdvance.world.news.length > beforeNews)
  assert.ok(afterAdvance.world.chronicle.some(entry => entry.type === 'clock'))
  assert.ok(afterAdvance.world.chronicle.some(entry => entry.type === 'threat'))
  assert.ok(afterAdvance.world.news.some(entry => entry.topic === 'world' && entry.msg.includes('Lanterns flare')))

  const replay = await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'clock-news-a'
  })
  assert.equal(replay.applied, false)
  const afterReplay = memoryStore.getSnapshot()
  assert.equal(afterReplay.world.chronicle.length, afterAdvance.world.chronicle.length)
  assert.equal(afterReplay.world.news.length, afterAdvance.world.news.length)
})

test('mood commands are read-only and town board includes mood section', async () => {
  const memoryStore = createStore()
  const seedService = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await seedService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'mood-readonly-seed-town'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'threat set alpha 12',
    operationId: 'mood-readonly-seed-threat'
  })

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()
  const moodList = await service.applyGodCommand({
    agents,
    command: 'mood list',
    operationId: 'mood-readonly-list'
  })
  const moodShow = await service.applyGodCommand({
    agents,
    command: 'mood alpha',
    operationId: 'mood-readonly-show'
  })
  const townBoard = await service.applyGodCommand({
    agents,
    command: 'town board alpha 10',
    operationId: 'mood-readonly-board'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(moodList.applied, true)
  assert.equal(moodShow.applied, true)
  assert.equal(townBoard.applied, true)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
  assert.ok(moodList.outputLines.some(line => line.includes('GOD MOOD TOWN: town=alpha')))
  assert.ok(moodShow.outputLines.some(line => line.includes('GOD MOOD: town=alpha')))
  assert.ok(townBoard.outputLines.some(line => line.includes('GOD TOWN BOARD MOOD: town=alpha')))
})

test('trade updates town mood exactly once under replay', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'mood-trade-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'market add bazaar alpha_hall',
    operationId: 'mood-trade-seed-market'
  })
  await service.applyGodCommand({
    agents,
    command: 'offer add bazaar Mara sell 4 2',
    operationId: 'mood-trade-seed-offer'
  })
  await service.applyGodCommand({
    agents,
    command: 'mint Eli 20',
    operationId: 'mood-trade-seed-balance'
  })

  const offerId = memoryStore.getSnapshot().world.markets[0].offers[0].offer_id
  const tradeA = await service.applyGodCommand({
    agents,
    command: `trade bazaar ${offerId} Eli 1`,
    operationId: 'mood-trade-a'
  })
  const afterA = memoryStore.getSnapshot()
  const replayA = await service.applyGodCommand({
    agents,
    command: `trade bazaar ${offerId} Eli 1`,
    operationId: 'mood-trade-a'
  })
  const afterReplay = memoryStore.getSnapshot()

  assert.equal(tradeA.applied, true)
  assert.equal(replayA.applied, false)
  assert.equal(afterA.world.moods.byTown.alpha.prosperity, 1)
  assert.equal(afterA.world.moods.byTown.alpha.unrest, 0)
  assert.deepEqual(afterReplay, afterA)

  const tradeB = await service.applyGodCommand({
    agents,
    command: `trade bazaar ${offerId} Eli 1`,
    operationId: 'mood-trade-b'
  })
  const afterB = memoryStore.getSnapshot()
  assert.equal(tradeB.applied, true)
  assert.equal(afterB.world.moods.byTown.alpha.prosperity, 2)
  assert.equal(afterB.world.moods.byTown.alpha.unrest, 0)
})

test('quest completion updates town mood exactly once under replay', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'mood-quest-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'quest offer alpha trade_n 1 0',
    operationId: 'mood-quest-offer'
  })
  const questId = memoryStore.getSnapshot().world.quests[0].id
  await service.applyGodCommand({
    agents,
    command: `quest accept Mara ${questId}`,
    operationId: 'mood-quest-accept'
  })
  await memoryStore.transact((memory) => {
    const idx = memory.world.quests.findIndex(item => item.id === questId)
    memory.world.quests[idx].state = 'in_progress'
    memory.world.quests[idx].progress = { done: 1 }
  }, { eventId: 'mood-quest-seed-progress' })

  const completeA = await service.applyGodCommand({
    agents,
    command: `quest complete ${questId}`,
    operationId: 'mood-quest-complete-a'
  })
  const afterA = memoryStore.getSnapshot()
  const replayA = await service.applyGodCommand({
    agents,
    command: `quest complete ${questId}`,
    operationId: 'mood-quest-complete-a'
  })
  const afterReplay = memoryStore.getSnapshot()

  assert.equal(completeA.applied, true)
  assert.equal(replayA.applied, false)
  assert.equal(afterA.world.moods.byTown.alpha.prosperity, 2)
  assert.equal(afterA.world.moods.byTown.alpha.fear, 0)
  assert.equal(afterA.world.moods.byTown.alpha.unrest, 0)
  assert.deepEqual(afterReplay, afterA)
})

test('mood narration triggers on threshold/label change and is replay-safe', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'mood-news-seed-town'
  })
  await memoryStore.transact((memory) => {
    memory.world.moods = {
      byTown: {
        alpha: { fear: 0, unrest: 24, prosperity: 0 }
      }
    }
  }, { eventId: 'mood-news-seed-levels' })
  await service.applyGodCommand({
    agents,
    command: 'quest offer alpha visit_town alpha 0',
    operationId: 'mood-news-offer'
  })
  const questId = memoryStore.getSnapshot().world.quests[0].id

  const before = memoryStore.getSnapshot()
  const beforeMoodChronicle = before.world.chronicle.filter(entry => entry.type === 'mood').length
  const beforeMoodNews = before.world.news.filter(entry => entry.msg.includes('Mood:')).length

  const cancelA = await service.applyGodCommand({
    agents,
    command: `quest cancel ${questId}`,
    operationId: 'mood-news-cancel-a'
  })
  assert.equal(cancelA.applied, true)
  const afterA = memoryStore.getSnapshot()
  const afterMoodChronicle = afterA.world.chronicle.filter(entry => entry.type === 'mood').length
  const afterMoodNews = afterA.world.news.filter(entry => entry.msg.includes('Mood:')).length
  assert.ok(afterMoodChronicle > beforeMoodChronicle)
  assert.ok(afterMoodNews > beforeMoodNews)

  const replay = await service.applyGodCommand({
    agents,
    command: `quest cancel ${questId}`,
    operationId: 'mood-news-cancel-a'
  })
  assert.equal(replay.applied, false)
  const afterReplay = memoryStore.getSnapshot()
  assert.equal(
    afterReplay.world.chronicle.filter(entry => entry.type === 'mood').length,
    afterMoodChronicle
  )
  assert.equal(
    afterReplay.world.news.filter(entry => entry.msg.includes('Mood:')).length,
    afterMoodNews
  )
})

test('event read-only commands do not mutate state', async () => {
  const memoryStore = createStore()
  const seedService = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await seedService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'event-readonly-seed-town'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'event draw alpha',
    operationId: 'event-readonly-seed-draw'
  })

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()
  const eventList = await service.applyGodCommand({
    agents,
    command: 'event list',
    operationId: 'event-readonly-list'
  })
  const eventAlias = await service.applyGodCommand({
    agents,
    command: 'event',
    operationId: 'event-readonly-alias'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(eventList.applied, true)
  assert.equal(eventAlias.applied, true)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
})

test('event seed/draw/clear are transactional and idempotent', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'event-seed-town'
  })

  const seedA = await service.applyGodCommand({
    agents,
    command: 'event seed 999',
    operationId: 'event-seed-a'
  })
  const seedReplay = await service.applyGodCommand({
    agents,
    command: 'event seed 999',
    operationId: 'event-seed-a'
  })
  assert.equal(seedA.applied, true)
  assert.equal(seedReplay.applied, false)

  const drawA = await service.applyGodCommand({
    agents,
    command: 'event draw alpha',
    operationId: 'event-draw-a'
  })
  const drawReplay = await service.applyGodCommand({
    agents,
    command: 'event draw alpha',
    operationId: 'event-draw-a'
  })
  assert.equal(drawA.applied, true)
  assert.equal(drawReplay.applied, false)
  const afterDraw = memoryStore.getSnapshot()
  assert.equal(afterDraw.world.events.seed, 999)
  assert.equal(afterDraw.world.events.active.length, 1)

  const eventId = afterDraw.world.events.active[0].id
  const clearA = await service.applyGodCommand({
    agents,
    command: `event clear ${eventId}`,
    operationId: 'event-clear-a'
  })
  const clearReplay = await service.applyGodCommand({
    agents,
    command: `event clear ${eventId}`,
    operationId: 'event-clear-a'
  })
  assert.equal(clearA.applied, true)
  assert.equal(clearReplay.applied, false)
  assert.equal(memoryStore.getSnapshot().world.events.active.length, 0)
})

test('clock advance auto-draws exactly one event per nightfall and replay is safe', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'event-auto-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'event seed 2026',
    operationId: 'event-auto-seed'
  })

  const nightA = await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'event-auto-a'
  })
  assert.equal(nightA.applied, true)
  const afterNightA = memoryStore.getSnapshot()
  assert.equal(afterNightA.world.events.index, 1)
  assert.equal(afterNightA.world.events.active.length, 1)
  const firstEventId = afterNightA.world.events.active[0].id

  const replayNightA = await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'event-auto-a'
  })
  assert.equal(replayNightA.applied, false)
  const afterReplay = memoryStore.getSnapshot()
  assert.equal(afterReplay.world.events.index, 1)
  assert.equal(afterReplay.world.events.active.length, 1)

  await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'event-auto-day'
  })
  const afterDay = memoryStore.getSnapshot()
  assert.equal(afterDay.world.clock.phase, 'day')
  assert.equal(afterDay.world.events.index, 1)

  await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'event-auto-b'
  })
  const afterNightB = memoryStore.getSnapshot()
  assert.equal(afterNightB.world.clock.phase, 'night')
  assert.equal(afterNightB.world.events.index, 2)
  assert.equal(afterNightB.world.events.active.length, 1)
  assert.notEqual(afterNightB.world.events.active[0].id, firstEventId)
})

test('event draw applies deterministic mood deltas from the drawn card', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'event-mood-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'event draw alpha',
    operationId: 'event-mood-draw'
  })
  const snapshot = memoryStore.getSnapshot()
  const event = snapshot.world.events.active[0]
  const mood = snapshot.world.moods.byTown.alpha
  const mods = event.mods || {}
  const clamp = value => Math.max(0, Math.min(100, value))
  assert.equal(mood.fear, clamp(Number(mods.fear || 0)))
  assert.equal(mood.unrest, clamp(Number(mods.unrest || 0)))
  assert.equal(mood.prosperity, clamp(Number(mods.prosperity || 0)))
})

test('quest offer reward bonus applies under active event and not without event', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'event-reward-seed-town'
  })
  await memoryStore.transact((memory) => {
    memory.world.clock = { day: 3, phase: 'day', season: 'dawn', updated_at: '2026-02-22T00:00:00.000Z' }
    memory.world.events = {
      seed: 1337,
      index: 0,
      active: [
        {
          id: 'e-shortage',
          type: 'shortage',
          town: 'alpha',
          starts_day: 3,
          ends_day: 3,
          mods: { unrest: 2, trade_reward_bonus: 2 }
        }
      ]
    }
  }, { eventId: 'event-reward-seed-state' })

  await service.applyGodCommand({
    agents,
    command: 'quest offer alpha trade_n 1',
    operationId: 'event-reward-offer-bonus'
  })
  const withBonus = memoryStore.getSnapshot().world.quests[0]
  assert.equal(withBonus.reward, 10)

  const controlStore = createStore()
  const controlService = createGodCommandService({ memoryStore: controlStore })
  await controlService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'event-reward-control-town'
  })
  await controlService.applyGodCommand({
    agents,
    command: 'quest offer alpha trade_n 1',
    operationId: 'event-reward-control-offer'
  })
  const withoutBonus = controlStore.getSnapshot().world.quests[0]
  assert.equal(withoutBonus.reward, 8)
})

test('quest completion rep bonus applies under omen/patrol and is replay-safe', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'event-rep-seed-town'
  })
  await memoryStore.transact((memory) => {
    memory.world.clock = { day: 2, phase: 'day', season: 'dawn', updated_at: '2026-02-22T00:00:00.000Z' }
    memory.world.events = {
      seed: 1337,
      index: 0,
      active: [
        {
          id: 'e-omen',
          type: 'omen',
          town: 'alpha',
          starts_day: 2,
          ends_day: 2,
          mods: { fear: 2, veil_church_rep_bonus: 1 }
        },
        {
          id: 'e-patrol',
          type: 'patrol',
          town: 'alpha',
          starts_day: 2,
          ends_day: 2,
          mods: { fear: -1, iron_pact_rep_bonus: 1 }
        }
      ]
    }
  }, { eventId: 'event-rep-seed-events' })

  await service.applyGodCommand({
    agents,
    command: 'quest offer alpha visit_town alpha 0',
    operationId: 'event-rep-offer'
  })
  const questId = memoryStore.getSnapshot().world.quests[0].id
  await service.applyGodCommand({
    agents,
    command: `quest accept Mara ${questId}`,
    operationId: 'event-rep-accept'
  })

  const visitA = await service.applyGodCommand({
    agents,
    command: `quest visit ${questId}`,
    operationId: 'event-rep-visit-a'
  })
  assert.equal(visitA.applied, true)
  const afterA = memoryStore.getSnapshot()
  assert.equal(afterA.agents.Mara.profile.rep.iron_pact, 1)
  assert.equal(afterA.agents.Mara.profile.rep.veil_church, 1)

  const replay = await service.applyGodCommand({
    agents,
    command: `quest visit ${questId}`,
    operationId: 'event-rep-visit-a'
  })
  assert.equal(replay.applied, false)
  const afterReplay = memoryStore.getSnapshot()
  assert.equal(afterReplay.agents.Mara.profile.rep.iron_pact, 1)
  assert.equal(afterReplay.agents.Mara.profile.rep.veil_church, 1)
})

test('job runtime side effects execute only after durable commit', async () => {
  const trace = []
  const state = {
    agents: {
      Mara: {
        short: [],
        long: [],
        summary: '',
        archive: [],
        recentUtterances: [],
        lastProcessedTime: 0,
        profile: {}
      }
    },
    factions: {},
    world: {
      warActive: false,
      rules: { allowLethalPolitics: true },
      player: { name: 'Player', alive: true, legitimacy: 50 },
      factions: {},
      markers: [],
      archive: [],
      processedEventIds: []
    }
  }
  const seen = new Set()
  const memoryStore = {
    getSnapshot: () => JSON.parse(JSON.stringify(state)),
    validateMemoryIntegrity: () => ({ ok: true, issues: [] }),
    getRuntimeMetrics: () => ({
      eventsProcessed: 0,
      duplicateEventsSkipped: 0,
      lockRetries: 0,
      lockTimeouts: 0,
      transactionsCommitted: 0,
      transactionsAborted: 0,
      openAiRequests: 0,
      openAiTimeouts: 0
    }),
    transact: async (mutator, opts = {}) => {
      if (seen.has(opts.eventId)) return { skipped: true, result: null }
      trace.push('tx_start')
      const result = await mutator(state)
      seen.add(opts.eventId)
      trace.push('tx_commit')
      return { skipped: false, result }
    }
  }

  const service = createGodCommandService({
    memoryStore,
    runtimeJob: () => {
      trace.push('runtime_job')
    }
  })

  const result = await service.applyGodCommand({
    agents: createAgents(),
    command: 'job set Mara builder',
    operationId: 'job-boundary'
  })

  assert.equal(result.applied, true)
  assert.deepEqual(trace, ['tx_start', 'tx_commit', 'runtime_job'])
})

test('marker runtime side effects execute only after durable commit', async () => {
  const trace = []
  const state = {
    agents: {},
    factions: {},
    world: {
      warActive: false,
      rules: { allowLethalPolitics: true },
      player: { name: 'Player', alive: true, legitimacy: 50 },
      factions: {},
      markers: [],
      archive: [],
      processedEventIds: []
    }
  }
  const seen = new Set()
  const memoryStore = {
    getSnapshot: () => JSON.parse(JSON.stringify(state)),
    validateMemoryIntegrity: () => ({ ok: true, issues: [] }),
    getRuntimeMetrics: () => ({
      eventsProcessed: 0,
      duplicateEventsSkipped: 0,
      lockRetries: 0,
      lockTimeouts: 0,
      transactionsCommitted: 0,
      transactionsAborted: 0,
      openAiRequests: 0,
      openAiTimeouts: 0
    }),
    transact: async (mutator, opts = {}) => {
      if (seen.has(opts.eventId)) return { skipped: true, result: null }
      trace.push('tx_start')
      const result = await mutator(state)
      seen.add(opts.eventId)
      trace.push('tx_commit')
      return { skipped: false, result }
    }
  }

  const service = createGodCommandService({
    memoryStore,
    runtimeMark: () => {
      trace.push('runtime_mark')
    }
  })

  const result = await service.applyGodCommand({
    agents: createAgents(),
    command: 'mark add camp 1 2 3 alpha',
    operationId: 'mark-boundary'
  })

  assert.equal(result.applied, true)
  assert.deepEqual(trace, ['tx_start', 'tx_commit', 'runtime_mark'])
})

test('rumor list/show are read-only and do not mutate state', async () => {
  const memoryStore = createStore()
  const seedService = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await seedService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'rumor-readonly-seed-town'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'rumor spawn alpha grounded 2 missing_goods 2',
    operationId: 'rumor-readonly-seed-rumor'
  })
  const rumorId = memoryStore.getSnapshot().world.rumors[0].id

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()
  const listResult = await service.applyGodCommand({
    agents,
    command: 'rumor list alpha 5',
    operationId: 'rumor-readonly-list'
  })
  const showResult = await service.applyGodCommand({
    agents,
    command: `rumor show ${rumorId}`,
    operationId: 'rumor-readonly-show'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(listResult.applied, true)
  assert.equal(showResult.applied, true)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
  assert.ok(listResult.outputLines.some(line => line.includes(`id=${rumorId}`)))
  assert.ok(showResult.outputLines.some(line => line.includes(`GOD RUMOR SHOW: id=${rumorId}`)))
})

test('rumor spawn/resolve/clear are transactional and replay-safe', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'rumor-tx-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'quest offer alpha visit_town alpha 1',
    operationId: 'rumor-tx-seed-quest'
  })
  const questId = memoryStore.getSnapshot().world.quests[0].id

  const spawnA = await service.applyGodCommand({
    agents,
    command: 'rumor spawn alpha supernatural 2 mist_shapes 2',
    operationId: 'rumor-tx-spawn-a'
  })
  assert.equal(spawnA.applied, true)
  const afterSpawn = memoryStore.getSnapshot()
  assert.equal(afterSpawn.world.rumors.length, 1)
  const rumorId = afterSpawn.world.rumors[0].id

  const spawnReplay = await service.applyGodCommand({
    agents,
    command: 'rumor spawn alpha supernatural 2 mist_shapes 2',
    operationId: 'rumor-tx-spawn-a'
  })
  assert.equal(spawnReplay.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), afterSpawn)

  const resolveA = await service.applyGodCommand({
    agents,
    command: `rumor resolve ${rumorId} ${questId}`,
    operationId: 'rumor-tx-resolve-a'
  })
  assert.equal(resolveA.applied, true)
  const afterResolve = memoryStore.getSnapshot()
  assert.equal(afterResolve.world.rumors[0].resolved_by_quest_id, questId)

  const resolveReplay = await service.applyGodCommand({
    agents,
    command: `rumor resolve ${rumorId} ${questId}`,
    operationId: 'rumor-tx-resolve-a'
  })
  assert.equal(resolveReplay.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), afterResolve)

  const clearA = await service.applyGodCommand({
    agents,
    command: `rumor clear ${rumorId}`,
    operationId: 'rumor-tx-clear-a'
  })
  assert.equal(clearA.applied, true)
  const afterClear = memoryStore.getSnapshot()
  assert.equal(afterClear.world.rumors.length, 0)

  const clearReplay = await service.applyGodCommand({
    agents,
    command: `rumor clear ${rumorId}`,
    operationId: 'rumor-tx-clear-a'
  })
  assert.equal(clearReplay.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), afterClear)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('rumor expiry occurs inside clock advance and replay is safe', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'rumor-expire-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'rumor spawn alpha supernatural 2 mist_shapes 0',
    operationId: 'rumor-expire-seed-rumor'
  })
  assert.equal(memoryStore.getSnapshot().world.rumors.length, 1)

  const advanceA = await service.applyGodCommand({
    agents,
    command: 'clock advance 2',
    operationId: 'rumor-expire-advance-a'
  })
  assert.equal(advanceA.applied, true)
  const afterAdvance = memoryStore.getSnapshot()
  assert.equal(afterAdvance.world.clock.day, 2)
  assert.equal(afterAdvance.world.rumors.length, 0)

  const advanceReplay = await service.applyGodCommand({
    agents,
    command: 'clock advance 2',
    operationId: 'rumor-expire-advance-a'
  })
  assert.equal(advanceReplay.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), afterAdvance)
})

test('rumor quest creation is transactional/idempotent and binds rumor_id', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'rumor-quest-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'rumor spawn alpha political 2 levy_accusations 2',
    operationId: 'rumor-quest-seed-rumor'
  })
  const rumorId = memoryStore.getSnapshot().world.rumors[0].id

  const createA = await service.applyGodCommand({
    agents,
    command: `rumor quest ${rumorId}`,
    operationId: 'rumor-quest-create-a'
  })
  assert.equal(createA.applied, true)
  const afterCreate = memoryStore.getSnapshot()
  const sideQuests = afterCreate.world.quests.filter(q => q.type === 'rumor_task' && q.rumor_id === rumorId)
  assert.equal(sideQuests.length, 1)
  assert.equal(sideQuests[0].meta.side, true)

  const createReplay = await service.applyGodCommand({
    agents,
    command: `rumor quest ${rumorId}`,
    operationId: 'rumor-quest-create-a'
  })
  assert.equal(createReplay.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), afterCreate)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('town board shows Trader sections without mutation', async () => {
  const memoryStore = createStore()
  const seedService = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await seedService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'board-side-seed-town'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'quest offer alpha visit_town alpha 2',
    operationId: 'board-side-seed-main-quest'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'rumor spawn alpha supernatural 2 mist_shapes 2',
    operationId: 'board-side-seed-rumor'
  })
  const rumorId = memoryStore.getSnapshot().world.rumors[0].id
  await seedService.applyGodCommand({
    agents,
    command: `rumor quest ${rumorId}`,
    operationId: 'board-side-seed-side-quest'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'event draw alpha',
    operationId: 'board-side-seed-event'
  })

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()
  const board = await service.applyGodCommand({
    agents,
    command: 'town board alpha 10',
    operationId: 'board-side-readonly'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(board.applied, true)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD QUESTS MAIN AVAILABLE:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD MARKET PULSE HOT:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD ROUTE RISK:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD CONTRACTS AVAILABLE:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD RUMOR LEADS:')))
  assert.equal(board.outputLines.some(line => line.includes('GOD TOWN BOARD DECISION OPEN:')), false)
})

test('major mission lifecycle enforces single active mission per town and shows on town board', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'major-seed-town'
  })

  const talk = await service.applyGodCommand({
    agents,
    command: 'mayor talk alpha',
    operationId: 'major-talk-a'
  })
  const accept = await service.applyGodCommand({
    agents,
    command: 'mayor accept alpha',
    operationId: 'major-accept-a'
  })
  const secondAccept = await service.applyGodCommand({
    agents,
    command: 'mayor accept alpha',
    operationId: 'major-accept-b'
  })
  const board = await service.applyGodCommand({
    agents,
    command: 'town board alpha 10',
    operationId: 'major-board-a'
  })

  assert.equal(talk.applied, true)
  assert.equal(accept.applied, true)
  assert.equal(secondAccept.applied, false)
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD MAJOR MISSION ACTIVE:')))

  const snapshot = memoryStore.getSnapshot()
  const active = snapshot.world.majorMissions.filter(
    mission => mission.townId === 'alpha' && mission.status === 'active'
  )
  assert.equal(active.length, 1)
  assert.equal(snapshot.world.towns.alpha.activeMajorMissionId, active[0].id)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('major mission accept/advance/complete are transactional and idempotent on replay', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'major-idem-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'mayor talk alpha',
    operationId: 'major-idem-talk'
  })

  const acceptA = await service.applyGodCommand({
    agents,
    command: 'mayor accept alpha',
    operationId: 'major-idem-accept'
  })
  const acceptReplay = await service.applyGodCommand({
    agents,
    command: 'mayor accept alpha',
    operationId: 'major-idem-accept'
  })
  const advanceA = await service.applyGodCommand({
    agents,
    command: 'mission advance alpha',
    operationId: 'major-idem-advance'
  })
  const advanceReplay = await service.applyGodCommand({
    agents,
    command: 'mission advance alpha',
    operationId: 'major-idem-advance'
  })
  const completeA = await service.applyGodCommand({
    agents,
    command: 'mission complete alpha',
    operationId: 'major-idem-complete'
  })
  const completeReplay = await service.applyGodCommand({
    agents,
    command: 'mission complete alpha',
    operationId: 'major-idem-complete'
  })

  assert.equal(acceptA.applied, true)
  assert.equal(acceptReplay.applied, false)
  assert.equal(advanceA.applied, true)
  assert.equal(advanceReplay.applied, false)
  assert.equal(completeA.applied, true)
  assert.equal(completeReplay.applied, false)

  const snapshot = memoryStore.getSnapshot()
  const mission = snapshot.world.majorMissions.find(item => item.id === snapshot.world.towns.alpha.activeMajorMissionId)
  assert.equal(mission, undefined)
  const completed = snapshot.world.majorMissions.find(item => item.status === 'completed' && item.townId === 'alpha')
  assert.ok(completed)
  assert.equal(snapshot.world.towns.alpha.activeMajorMissionId, null)
  assert.equal(snapshot.world.towns.alpha.majorMissionCooldownUntilDay, 3)
  assert.equal(snapshot.world.towns.alpha.hope, 57)
  assert.equal(snapshot.world.towns.alpha.dread, 46)
  assert.equal(snapshot.world.towns.alpha.recentImpacts.filter(item => item.type === 'mission_complete').length, 1)

  const crierTypes = snapshot.world.towns.alpha.crierQueue.map(item => item.type)
  assert.equal(crierTypes.filter(type => type === 'mission_available').length, 1)
  assert.equal(crierTypes.filter(type => type === 'mission_accepted').length, 1)
  assert.equal(crierTypes.filter(type => type === 'mission_phase').length, 1)
  assert.equal(crierTypes.filter(type => type === 'mission_win').length, 1)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('major mission fail is transactional and idempotent on replay', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'major-fail-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'mayor talk alpha',
    operationId: 'major-fail-talk'
  })
  await service.applyGodCommand({
    agents,
    command: 'mayor accept alpha',
    operationId: 'major-fail-accept'
  })

  const failA = await service.applyGodCommand({
    agents,
    command: 'mission fail alpha route collapsed',
    operationId: 'major-fail-a'
  })
  const failReplay = await service.applyGodCommand({
    agents,
    command: 'mission fail alpha route collapsed',
    operationId: 'major-fail-a'
  })

  assert.equal(failA.applied, true)
  assert.equal(failReplay.applied, false)

  const snapshot = memoryStore.getSnapshot()
  const failed = snapshot.world.majorMissions.find(item => item.status === 'failed' && item.townId === 'alpha')
  assert.ok(failed)
  assert.equal(snapshot.world.towns.alpha.activeMajorMissionId, null)
  assert.equal(snapshot.world.towns.alpha.majorMissionCooldownUntilDay, 3)
  assert.equal(snapshot.world.towns.alpha.hope, 46)
  assert.equal(snapshot.world.towns.alpha.dread, 57)
  assert.equal(snapshot.world.towns.alpha.crierQueue.filter(item => item.type === 'mission_fail').length, 1)
  assert.equal(snapshot.world.towns.alpha.recentImpacts.filter(item => item.type === 'mission_fail').length, 1)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('major mission crier queue remains bounded', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'major-crier-seed-town'
  })

  await memoryStore.transact((memory) => {
    memory.world.towns = memory.world.towns || {}
    memory.world.towns.alpha = memory.world.towns.alpha || {
      activeMajorMissionId: null,
      majorMissionCooldownUntilDay: 0,
      crierQueue: []
    }
    memory.world.towns.alpha.crierQueue = Array.from({ length: 40 }).map((_, idx) => ({
      id: `seed-${idx}`,
      day: 1,
      type: 'seed',
      message: `seed-${idx}`
    }))
  }, { eventId: 'major-crier-seed-queue' })

  const before = memoryStore.getSnapshot().world.towns.alpha.crierQueue
  assert.equal(before.length, 40)

  await service.applyGodCommand({
    agents,
    command: 'mayor talk alpha',
    operationId: 'major-crier-talk'
  })

  const queue = memoryStore.getSnapshot().world.towns.alpha.crierQueue
  assert.equal(queue.length, 40)
  assert.equal(queue[0].id, 'seed-1')
  assert.equal(queue[queue.length - 1].type, 'mission_available')
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('nether tick is deterministic for same seed/state and propagates crier entries to all towns', async () => {
  const createDeterministicService = () => {
    const memoryStore = createStore()
    let t = 0
    const service = createGodCommandService({
      memoryStore,
      now: () => {
        t += 1000
        return t
      }
    })
    return { memoryStore, service }
  }
  const left = createDeterministicService()
  const right = createDeterministicService()
  const agents = createAgents()

  const seedAndTick = async (service) => {
    await service.applyGodCommand({
      agents,
      command: 'mark add alpha_hall 0 64 0 town:alpha',
      operationId: 'nether-seed-alpha'
    })
    await service.applyGodCommand({
      agents,
      command: 'mark add beta_gate 100 64 0 town:beta',
      operationId: 'nether-seed-beta'
    })
    await service.applyGodCommand({
      agents,
      command: 'event seed 4242',
      operationId: 'nether-seed-events'
    })
    const tick = await service.applyGodCommand({
      agents,
      command: 'nether tick 8',
      operationId: 'nether-tick-a'
    })
    assert.equal(tick.applied, true)
  }

  await seedAndTick(left.service)
  await seedAndTick(right.service)

  const leftSnapshot = left.memoryStore.getSnapshot()
  const rightSnapshot = right.memoryStore.getSnapshot()
  assert.deepEqual(leftSnapshot.world.nether, rightSnapshot.world.nether)
  assert.equal(leftSnapshot.world.towns.alpha.hope, rightSnapshot.world.towns.alpha.hope)
  assert.equal(leftSnapshot.world.towns.alpha.dread, rightSnapshot.world.towns.alpha.dread)
  assert.equal(leftSnapshot.world.towns.beta.hope, rightSnapshot.world.towns.beta.hope)
  assert.equal(leftSnapshot.world.towns.beta.dread, rightSnapshot.world.towns.beta.dread)
  assert.ok(leftSnapshot.world.nether.eventLedger.length > 0)
  assert.ok(leftSnapshot.world.towns.alpha.crierQueue.some(entry => entry.type === 'nether_event'))
  assert.ok(leftSnapshot.world.towns.beta.crierQueue.some(entry => entry.type === 'nether_event'))
  assert.ok(leftSnapshot.world.towns.alpha.recentImpacts.some(entry => entry.type === 'nether_event'))
  assert.ok(leftSnapshot.world.towns.beta.recentImpacts.some(entry => entry.type === 'nether_event'))
  assert.equal(left.memoryStore.validateMemoryIntegrity().ok, true)
  assert.equal(right.memoryStore.validateMemoryIntegrity().ok, true)
})

test('nether tick is transactional/idempotent on replay and bounded under burst days', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'nether-bound-seed-alpha'
  })
  await service.applyGodCommand({
    agents,
    command: 'mark add beta_gate 100 64 0 town:beta',
    operationId: 'nether-bound-seed-beta'
  })
  await service.applyGodCommand({
    agents,
    command: 'event seed 1234',
    operationId: 'nether-bound-seed-events'
  })

  const tickA = await service.applyGodCommand({
    agents,
    command: 'nether tick 20',
    operationId: 'nether-bound-a'
  })
  assert.equal(tickA.applied, true)
  const afterA = memoryStore.getSnapshot()

  const tickReplay = await service.applyGodCommand({
    agents,
    command: 'nether tick 20',
    operationId: 'nether-bound-a'
  })
  assert.equal(tickReplay.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), afterA)

  const burst = await service.applyGodCommand({
    agents,
    command: 'nether tick 240',
    operationId: 'nether-bound-burst'
  })
  assert.equal(burst.applied, true)

  const snapshot = memoryStore.getSnapshot()
  assert.ok(snapshot.world.nether.eventLedger.length <= 120)
  assert.ok(snapshot.world.news.length <= 200)
  assert.ok(snapshot.world.chronicle.length <= 200)
  assert.ok(snapshot.world.towns.alpha.crierQueue.length <= 40)
  assert.ok(snapshot.world.towns.beta.crierQueue.length <= 40)
  assert.ok(snapshot.world.towns.alpha.recentImpacts.length <= 30)
  assert.ok(snapshot.world.towns.beta.recentImpacts.length <= 30)
  assert.ok(snapshot.world.towns.alpha.crierQueue.some(entry => entry.type === 'nether_event'))
  assert.ok(snapshot.world.towns.beta.crierQueue.some(entry => entry.type === 'nether_event'))
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('townsfolk talk creates deterministic side quests with dedupe and major mission linkage', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'townsfolk-seed-alpha'
  })
  await service.applyGodCommand({
    agents,
    command: 'mark add beta_gate 100 64 0 town:beta',
    operationId: 'townsfolk-seed-beta'
  })
  await service.applyGodCommand({
    agents,
    command: 'mayor talk alpha',
    operationId: 'townsfolk-major-talk'
  })
  await service.applyGodCommand({
    agents,
    command: 'mayor accept alpha',
    operationId: 'townsfolk-major-accept'
  })

  const talkCreated = await service.applyGodCommand({
    agents,
    command: 'townsfolk talk alpha Gate Warden',
    operationId: 'townsfolk-talk-a'
  })
  const talkReplay = await service.applyGodCommand({
    agents,
    command: 'townsfolk talk alpha Gate Warden',
    operationId: 'townsfolk-talk-a'
  })
  const talkDedupe = await service.applyGodCommand({
    agents,
    command: 'townsfolk talk alpha Gate Warden',
    operationId: 'townsfolk-talk-b'
  })
  assert.equal(talkCreated.applied, true)
  assert.equal(talkReplay.applied, false)
  assert.equal(talkDedupe.applied, true)
  assert.ok(talkDedupe.outputLines.some(line => line.includes('status=existing')))

  const snapshot = memoryStore.getSnapshot()
  const townsfolkQuests = snapshot.world.quests.filter((quest) => (
    quest.origin === 'townsfolk'
    && quest.townId === 'alpha'
    && quest.npcKey === 'gate_warden'
  ))
  assert.equal(townsfolkQuests.length, 1)
  assert.equal(townsfolkQuests[0].supportsMajorMissionId, snapshot.world.towns.alpha.activeMajorMissionId)

  const board = await service.applyGodCommand({
    agents,
    command: 'town board alpha 10',
    operationId: 'townsfolk-board'
  })
  assert.equal(board.applied, true)
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD WAR FRONTLINE STATUS:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD WAR HOPE DREAD:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD WAR RATIONS STRAIN:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD WAR WHAT CHANGED TODAY:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD WAR ORDERS OF THE DAY:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD MAJOR MISSION')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD SIDE QUESTS TOP:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD NETHER PULSE:')))

  const list = await service.applyGodCommand({
    agents,
    command: 'quest list alpha',
    operationId: 'townsfolk-quest-list'
  })
  assert.equal(list.applied, true)
  assert.ok(list.outputLines.some(line => line.includes('origin=townsfolk')))
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('townsfolk quest completion/failure updates hope-dread once and is replay-safe', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'townsfolk-meter-seed-alpha'
  })
  await service.applyGodCommand({
    agents,
    command: 'mark add beta_gate 100 64 0 town:beta',
    operationId: 'townsfolk-meter-seed-beta'
  })

  await service.applyGodCommand({
    agents,
    command: 'townsfolk talk alpha miller',
    operationId: 'townsfolk-meter-talk-a'
  })
  const firstQuest = memoryStore.getSnapshot().world.quests.find(quest => quest.origin === 'townsfolk' && quest.npcKey === 'miller')
  assert.ok(firstQuest)

  const cancelA = await service.applyGodCommand({
    agents,
    command: `quest cancel ${firstQuest.id}`,
    operationId: 'townsfolk-meter-cancel-a'
  })
  const cancelReplay = await service.applyGodCommand({
    agents,
    command: `quest cancel ${firstQuest.id}`,
    operationId: 'townsfolk-meter-cancel-a'
  })
  assert.equal(cancelA.applied, true)
  assert.equal(cancelReplay.applied, false)

  await service.applyGodCommand({
    agents,
    command: 'townsfolk talk alpha scout',
    operationId: 'townsfolk-meter-talk-b'
  })
  const secondQuest = memoryStore.getSnapshot().world.quests.find(quest => quest.origin === 'townsfolk' && quest.npcKey === 'scout')
  assert.ok(secondQuest)

  const accept = await service.applyGodCommand({
    agents,
    command: `quest accept Mara ${secondQuest.id}`,
    operationId: 'townsfolk-meter-accept'
  })
  assert.equal(accept.applied, true)

  await memoryStore.transact((memory) => {
    const target = memory.world.quests.find(entry => entry.id === secondQuest.id)
    if (!target) return
    if (target.type === 'trade_n') {
      target.progress = { done: Number(target.objective?.n || 1) }
    } else {
      target.progress = { visited: true }
    }
  }, { eventId: 'townsfolk-meter-satisfy-progress' })

  const completeA = await service.applyGodCommand({
    agents,
    command: `quest complete ${secondQuest.id}`,
    operationId: 'townsfolk-meter-complete-a'
  })
  const completeReplay = await service.applyGodCommand({
    agents,
    command: `quest complete ${secondQuest.id}`,
    operationId: 'townsfolk-meter-complete-a'
  })
  assert.equal(completeA.applied, true)
  assert.equal(completeReplay.applied, false)

  const snapshot = memoryStore.getSnapshot()
  assert.equal(snapshot.world.towns.alpha.hope, 51)
  assert.equal(snapshot.world.towns.alpha.dread, 52)
  assert.equal(snapshot.world.towns.alpha.recentImpacts.filter(item => item.type === 'townsfolk_fail').length, 1)
  assert.equal(snapshot.world.towns.alpha.recentImpacts.filter(item => item.type === 'townsfolk_complete').length, 1)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('project lifecycle is deterministic and replay-safe', async () => {
  function createScenario() {
    const memoryStore = createStore()
    const service = createGodCommandService({ memoryStore })
    const agents = createAgents()
    return { memoryStore, service, agents }
  }
  async function runScenario(state) {
    await state.service.applyGodCommand({
      agents: state.agents,
      command: 'mark add alpha_hall 0 64 0 town:alpha',
      operationId: 'project-seed-alpha'
    })
    await state.service.applyGodCommand({
      agents: state.agents,
      command: 'mark add beta_gate 100 64 0 town:beta',
      operationId: 'project-seed-beta'
    })
    await state.service.applyGodCommand({
      agents: state.agents,
      command: 'threat set alpha 20',
      operationId: 'project-seed-threat'
    })
    await state.service.applyGodCommand({
      agents: state.agents,
      command: 'mayor talk alpha',
      operationId: 'project-seed-mayor-talk'
    })
    await state.service.applyGodCommand({
      agents: state.agents,
      command: 'mayor accept alpha',
      operationId: 'project-seed-mayor-accept'
    })

    const startA = await state.service.applyGodCommand({
      agents: state.agents,
      command: 'project start alpha trench_reinforcement',
      operationId: 'project-start-a'
    })
    const startReplay = await state.service.applyGodCommand({
      agents: state.agents,
      command: 'project start alpha trench_reinforcement',
      operationId: 'project-start-a'
    })
    const startDedupe = await state.service.applyGodCommand({
      agents: state.agents,
      command: 'project start alpha trench_reinforcement',
      operationId: 'project-start-b'
    })
    assert.equal(startA.applied, true)
    assert.equal(startReplay.applied, false)
    assert.equal(startDedupe.applied, true)
    assert.ok(startDedupe.outputLines.some(line => line.includes('status=existing')))

    const firstProject = state.memoryStore.getSnapshot().world.projects.find(project => project.type === 'trench_reinforcement' && project.townId === 'alpha')
    assert.ok(firstProject)

    const advanceA = await state.service.applyGodCommand({
      agents: state.agents,
      command: `project advance alpha ${firstProject.id}`,
      operationId: 'project-advance-a'
    })
    const advanceReplay = await state.service.applyGodCommand({
      agents: state.agents,
      command: `project advance alpha ${firstProject.id}`,
      operationId: 'project-advance-a'
    })
    assert.equal(advanceA.applied, true)
    assert.equal(advanceReplay.applied, false)

    const completeA = await state.service.applyGodCommand({
      agents: state.agents,
      command: `project complete alpha ${firstProject.id}`,
      operationId: 'project-complete-a'
    })
    const completeReplay = await state.service.applyGodCommand({
      agents: state.agents,
      command: `project complete alpha ${firstProject.id}`,
      operationId: 'project-complete-a'
    })
    assert.equal(completeA.applied, true)
    assert.equal(completeReplay.applied, false)

    const startB = await state.service.applyGodCommand({
      agents: state.agents,
      command: 'project start alpha watchtower_line',
      operationId: 'project-start-c'
    })
    assert.equal(startB.applied, true)
    const secondProject = state.memoryStore.getSnapshot().world.projects.find(project => project.type === 'watchtower_line' && project.townId === 'alpha')
    assert.ok(secondProject)

    const failA = await state.service.applyGodCommand({
      agents: state.agents,
      command: `project fail alpha ${secondProject.id} route collapsed`,
      operationId: 'project-fail-a'
    })
    const failReplay = await state.service.applyGodCommand({
      agents: state.agents,
      command: `project fail alpha ${secondProject.id} route collapsed`,
      operationId: 'project-fail-a'
    })
    assert.equal(failA.applied, true)
    assert.equal(failReplay.applied, false)
  }

  const left = createScenario()
  const right = createScenario()
  await runScenario(left)
  await runScenario(right)

  const leftSnapshot = left.memoryStore.getSnapshot()
  const rightSnapshot = right.memoryStore.getSnapshot()
  assert.deepEqual(leftSnapshot.world.projects, rightSnapshot.world.projects)
  assert.equal(leftSnapshot.world.towns.alpha.hope, rightSnapshot.world.towns.alpha.hope)
  assert.equal(leftSnapshot.world.towns.alpha.dread, rightSnapshot.world.towns.alpha.dread)
  assert.equal(leftSnapshot.world.threat.byTown.alpha, 18)
  assert.equal(leftSnapshot.world.towns.alpha.hope, 50)
  assert.equal(leftSnapshot.world.towns.alpha.dread, 52)
  assert.equal(leftSnapshot.world.projects.filter(project => project.status === 'completed').length, 1)
  assert.equal(leftSnapshot.world.projects.filter(project => project.status === 'failed').length, 1)
  assert.equal(leftSnapshot.world.towns.alpha.recentImpacts.filter(item => item.type === 'project_complete').length, 1)
  assert.equal(leftSnapshot.world.towns.alpha.recentImpacts.filter(item => item.type === 'project_fail').length, 1)
  assert.ok(leftSnapshot.world.towns.alpha.crierQueue.some(item => item.type === 'project_start'))
  assert.ok(leftSnapshot.world.towns.alpha.crierQueue.some(item => item.type === 'project_phase'))
  assert.ok(leftSnapshot.world.towns.alpha.crierQueue.some(item => item.type === 'project_complete'))
  assert.ok(leftSnapshot.world.towns.alpha.crierQueue.some(item => item.type === 'project_fail'))
  assert.equal(left.memoryStore.validateMemoryIntegrity().ok, true)
  assert.equal(right.memoryStore.validateMemoryIntegrity().ok, true)
})

test('salvage plan/resolve/fail are deterministic and replay-safe', async () => {
  function createScenario() {
    const memoryStore = createStore()
    const service = createGodCommandService({ memoryStore })
    const agents = createAgents()
    return { memoryStore, service, agents }
  }
  async function runScenario(state) {
    await state.service.applyGodCommand({
      agents: state.agents,
      command: 'mark add alpha_hall 0 64 0 town:alpha',
      operationId: 'salvage-seed-alpha'
    })
    await state.service.applyGodCommand({
      agents: state.agents,
      command: 'mark add beta_gate 100 64 0 town:beta',
      operationId: 'salvage-seed-beta'
    })
    await state.service.applyGodCommand({
      agents: state.agents,
      command: 'project start alpha lantern_line',
      operationId: 'salvage-seed-project'
    })

    const planA = await state.service.applyGodCommand({
      agents: state.agents,
      command: 'salvage plan alpha no_mans_land_scrap',
      operationId: 'salvage-plan-a'
    })
    const planReplay = await state.service.applyGodCommand({
      agents: state.agents,
      command: 'salvage plan alpha no_mans_land_scrap',
      operationId: 'salvage-plan-a'
    })
    const planDedupe = await state.service.applyGodCommand({
      agents: state.agents,
      command: 'salvage plan alpha no_mans_land_scrap',
      operationId: 'salvage-plan-b'
    })
    assert.equal(planA.applied, true)
    assert.equal(planReplay.applied, false)
    assert.equal(planDedupe.applied, true)
    assert.ok(planDedupe.outputLines.some(line => line.includes('status=existing')))

    const firstRun = state.memoryStore.getSnapshot().world.salvageRuns.find(run => run.targetKey === 'no_mans_land_scrap' && run.townId === 'alpha')
    assert.ok(firstRun)
    assert.ok(firstRun.supportsProjectId)

    const resolveA = await state.service.applyGodCommand({
      agents: state.agents,
      command: `salvage resolve alpha ${firstRun.id} secure`,
      operationId: 'salvage-resolve-a'
    })
    const resolveReplay = await state.service.applyGodCommand({
      agents: state.agents,
      command: `salvage resolve alpha ${firstRun.id} secure`,
      operationId: 'salvage-resolve-a'
    })
    assert.equal(resolveA.applied, true)
    assert.equal(resolveReplay.applied, false)

    const planB = await state.service.applyGodCommand({
      agents: state.agents,
      command: 'salvage plan alpha collapsed_tunnel_tools',
      operationId: 'salvage-plan-c'
    })
    assert.equal(planB.applied, true)
    const secondRun = state.memoryStore.getSnapshot().world.salvageRuns.find(run => run.targetKey === 'collapsed_tunnel_tools' && run.townId === 'alpha')
    assert.ok(secondRun)

    const failA = await state.service.applyGodCommand({
      agents: state.agents,
      command: `salvage fail alpha ${secondRun.id} patrol broke formation`,
      operationId: 'salvage-fail-a'
    })
    const failReplay = await state.service.applyGodCommand({
      agents: state.agents,
      command: `salvage fail alpha ${secondRun.id} patrol broke formation`,
      operationId: 'salvage-fail-a'
    })
    assert.equal(failA.applied, true)
    assert.equal(failReplay.applied, false)
  }

  const left = createScenario()
  const right = createScenario()
  await runScenario(left)
  await runScenario(right)

  const leftSnapshot = left.memoryStore.getSnapshot()
  const rightSnapshot = right.memoryStore.getSnapshot()
  assert.deepEqual(leftSnapshot.world.salvageRuns, rightSnapshot.world.salvageRuns)
  assert.equal(leftSnapshot.world.towns.alpha.hope, rightSnapshot.world.towns.alpha.hope)
  assert.equal(leftSnapshot.world.towns.alpha.dread, rightSnapshot.world.towns.alpha.dread)
  assert.ok(leftSnapshot.world.salvageRuns.some(run => run.status === 'resolved'))
  assert.ok(leftSnapshot.world.salvageRuns.some(run => run.status === 'failed'))
  assert.equal(leftSnapshot.world.towns.alpha.recentImpacts.filter(item => item.type === 'salvage_plan').length, 2)
  assert.equal(leftSnapshot.world.towns.alpha.recentImpacts.filter(item => item.type === 'salvage_resolve').length, 1)
  assert.equal(leftSnapshot.world.towns.alpha.recentImpacts.filter(item => item.type === 'salvage_fail').length, 1)
  assert.ok(leftSnapshot.world.towns.alpha.crierQueue.some(item => item.type === 'salvage_plan'))
  assert.ok(leftSnapshot.world.towns.alpha.crierQueue.some(item => item.type === 'salvage_resolve'))
  assert.ok(leftSnapshot.world.towns.alpha.crierQueue.some(item => item.type === 'salvage_fail'))
  assert.equal(left.memoryStore.validateMemoryIntegrity().ok, true)
  assert.equal(right.memoryStore.validateMemoryIntegrity().ok, true)
})

test('town board includes project and salvage sections', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'board-proj-seed-alpha'
  })
  await service.applyGodCommand({
    agents,
    command: 'project start alpha ration_depot',
    operationId: 'board-proj-start'
  })
  await service.applyGodCommand({
    agents,
    command: 'salvage plan alpha ruined_hamlet_supplies',
    operationId: 'board-salvage-plan'
  })

  const board = await service.applyGodCommand({
    agents,
    command: 'town board alpha 10',
    operationId: 'board-project-salvage'
  })
  assert.equal(board.applied, true)
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD PROJECTS:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD PROJECT:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD SALVAGE:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD WAR FRONTLINE STATUS:')))
  assert.ok(board.outputLines.some(line => line.includes('GOD TOWN BOARD SIDE QUESTS TOP:')))
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('project and salvage histories remain bounded during command mutations', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'bound-project-seed-alpha'
  })

  await memoryStore.transact((memory) => {
    memory.world.projects = Array.from({ length: 140 }).map((_, idx) => ({
      id: `pr_bound_${idx}`,
      townId: 'alpha',
      type: idx % 2 === 0 ? 'watchtower_line' : 'ration_depot',
      status: idx % 3 === 0 ? 'active' : 'completed',
      stage: 1,
      requirements: { labor: 2 },
      effects: { hopeDelta: 1 },
      startedAtDay: 1,
      updatedAtDay: 1 + idx
    }))
    memory.world.salvageRuns = Array.from({ length: 150 }).map((_, idx) => ({
      id: `sr_bound_${idx}`,
      townId: 'alpha',
      targetKey: idx % 2 === 0 ? 'no_mans_land_scrap' : 'collapsed_tunnel_tools',
      status: idx % 3 === 0 ? 'planned' : 'resolved',
      plannedAtDay: 1,
      resolvedAtDay: idx % 3 === 0 ? 0 : (1 + idx),
      result: { supplies: 2, hopeDelta: 1, dreadDelta: -1 }
    }))
  }, { eventId: 'bound-project-salvage-seed' })

  const start = await service.applyGodCommand({
    agents,
    command: 'project start alpha trench_reinforcement',
    operationId: 'bound-project-start'
  })
  const plan = await service.applyGodCommand({
    agents,
    command: 'salvage plan alpha no_mans_land_scrap',
    operationId: 'bound-salvage-plan'
  })
  assert.equal(start.applied, true)
  assert.equal(plan.applied, true)

  const snapshot = memoryStore.getSnapshot()
  assert.ok(snapshot.world.projects.length <= 120)
  assert.ok(snapshot.world.salvageRuns.length <= 120)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('decision list/show are read-only and do not mutate state', async () => {
  const memoryStore = createStore()
  const seedService = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await seedService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'decision-readonly-seed-town'
  })
  await memoryStore.transact((memory) => {
    memory.world.decisions = [
      {
        id: 'd_readonly_alpha',
        town: 'alpha',
        event_id: 'e_readonly_alpha',
        event_type: 'shortage',
        prompt: 'Legacy decision from an old save.',
        options: [
          { key: 'ration', label: 'Ration', effects: { mood: { unrest: 1 } } },
          { key: 'import', label: 'Import', effects: { mood: { prosperity: 1 } } }
        ],
        state: 'open',
        starts_day: 1,
        expires_day: 10,
        created_at: 1000
      }
    ]
  }, { eventId: 'decision-readonly-seed-state' })
  const decisionId = memoryStore.getSnapshot().world.decisions[0].id

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()
  const list = await service.applyGodCommand({
    agents,
    command: 'decision list alpha',
    operationId: 'decision-readonly-list'
  })
  const show = await service.applyGodCommand({
    agents,
    command: `decision show ${decisionId}`,
    operationId: 'decision-readonly-show'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(list.applied, true)
  assert.equal(show.applied, true)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
  assert.ok(list.outputLines.some(line => line.includes('GOD DECISION DEPRECATED:')))
  assert.ok(list.outputLines.some(line => line.includes(`id=${decisionId}`)))
  assert.ok(show.outputLines.some(line => line.includes(`GOD DECISION SHOW: id=${decisionId}`)))
})

test('nightfall does not auto-create decisions and replay is safe', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'decision-auto-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'event seed 777',
    operationId: 'decision-auto-seed-event'
  })
  await memoryStore.transact((memory) => {
    memory.world.decisions = [
      {
        id: 'd_legacy_alpha',
        town: 'alpha',
        event_id: 'e_legacy_alpha',
        event_type: 'festival',
        prompt: 'Legacy saved decision.',
        options: [
          { key: 'hold_feast', label: 'Hold Feast', effects: { mood: { prosperity: 1 } } },
          { key: 'fund_music', label: 'Fund Music', effects: { mood: { fear: -1 } } }
        ],
        state: 'open',
        starts_day: 1,
        expires_day: 99,
        created_at: 2000
      }
    ]
  }, { eventId: 'decision-auto-seed-legacy' })
  const beforeDecisions = memoryStore.getSnapshot().world.decisions

  const advanceA = await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'decision-auto-night-a'
  })
  assert.equal(advanceA.applied, true)
  const afterAdvanceA = memoryStore.getSnapshot()
  assert.equal(afterAdvanceA.world.clock.phase, 'night')
  assert.equal(afterAdvanceA.world.events.active.length, 1)
  assert.deepEqual(afterAdvanceA.world.decisions, beforeDecisions)

  const replay = await service.applyGodCommand({
    agents,
    command: 'clock advance 1',
    operationId: 'decision-auto-night-a'
  })
  assert.equal(replay.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), afterAdvanceA)
})

test('market pulse commands are deterministic and read-only', async () => {
  const memoryStore = createStore()
  const seedService = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await seedService.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'pulse-seed-town'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'threat set alpha 63',
    operationId: 'pulse-seed-threat'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'clock season long_night',
    operationId: 'pulse-seed-season'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'event seed 777',
    operationId: 'pulse-seed-event'
  })
  await seedService.applyGodCommand({
    agents,
    command: 'event draw alpha',
    operationId: 'pulse-seed-event-draw'
  })

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const service = createGodCommandService({ memoryStore })
  const before = memoryStore.getSnapshot()

  const pulseTownA = await service.applyGodCommand({
    agents,
    command: 'market pulse alpha',
    operationId: 'pulse-town-a'
  })
  const pulseTownB = await service.applyGodCommand({
    agents,
    command: 'market pulse alpha',
    operationId: 'pulse-town-b'
  })
  const pulseWorld = await service.applyGodCommand({
    agents,
    command: 'market pulse world',
    operationId: 'pulse-world-a'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(pulseTownA.applied, true)
  assert.equal(pulseTownB.applied, true)
  assert.equal(pulseWorld.applied, true)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
  assert.deepEqual(pulseTownA.outputLines, pulseTownB.outputLines)
  assert.ok(pulseTownA.outputLines.some(line => line.includes('GOD MARKET PULSE HOT:')))
  assert.ok(pulseTownA.outputLines.some(line => line.includes('GOD MARKET PULSE COLD:')))
  assert.ok(pulseTownA.outputLines.some(line => line.includes('GOD MARKET PULSE RISK:')))
  assert.ok(pulseWorld.outputLines.some(line => line.includes('GOD MARKET PULSE WORLD:')))
  assert.ok(pulseWorld.outputLines.some(line => line.includes('GOD MARKET PULSE TOWN: town=alpha')))
})

test('daily contract generation is bounded per town and replay-safe', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'contract-gen-seed-alpha'
  })
  await service.applyGodCommand({
    agents,
    command: 'mark add beta_hall 100 64 0 town:beta',
    operationId: 'contract-gen-seed-beta'
  })

  const advance = await service.applyGodCommand({
    agents,
    command: 'clock advance 2',
    operationId: 'contract-gen-day2'
  })
  assert.equal(advance.applied, true)

  const afterAdvance = memoryStore.getSnapshot()
  const day2Contracts = afterAdvance.world.quests
    .filter(quest => quest?.meta?.contract === true && Number(quest.meta?.contract_day || 0) === 2)
  assert.ok(day2Contracts.length >= 2)
  assert.ok(day2Contracts.length <= 4)

  const byTown = new Map()
  for (const contract of day2Contracts) {
    const town = String(contract.town || '').toLowerCase()
    if (!town) continue
    byTown.set(town, Number(byTown.get(town) || 0) + 1)
    assert.equal(contract.state, 'offered')
  }
  assert.ok(byTown.has('alpha'))
  assert.ok(byTown.has('beta'))
  assert.ok(Number(byTown.get('alpha')) >= 1 && Number(byTown.get('alpha')) <= 2)
  assert.ok(Number(byTown.get('beta')) >= 1 && Number(byTown.get('beta')) <= 2)

  const replay = await service.applyGodCommand({
    agents,
    command: 'clock advance 2',
    operationId: 'contract-gen-day2'
  })
  assert.equal(replay.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), afterAdvance)
})

test('contract accept/complete delegates to quest flow and rewards once', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'contract-flow-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'market add bazaar alpha_hall',
    operationId: 'contract-flow-seed-market'
  })
  await service.applyGodCommand({
    agents,
    command: 'mint Mara 50',
    operationId: 'contract-flow-seed-mint-mara'
  })
  await service.applyGodCommand({
    agents,
    command: 'mint Eli 50',
    operationId: 'contract-flow-seed-mint-eli'
  })

  const offerAdd = await service.applyGodCommand({
    agents,
    command: 'offer add bazaar Eli sell 5 1',
    operationId: 'contract-flow-seed-offer'
  })
  assert.equal(offerAdd.applied, true)
  const offerAddedLine = offerAdd.outputLines.find(line => line.includes('GOD OFFER ADDED:')) || ''
  const offerMatch = offerAddedLine.match(/id=([^\s]+)/i)
  assert.ok(offerMatch)
  const offerId = offerMatch[1]

  await service.applyGodCommand({
    agents,
    command: 'clock advance 2',
    operationId: 'contract-flow-day2'
  })

  const offeredContracts = memoryStore.getSnapshot().world.quests
    .filter(quest => quest?.meta?.contract === true && quest.state === 'offered' && quest.type === 'trade_n')
  assert.ok(offeredContracts.length >= 1)
  const contract = offeredContracts[0]
  const contractId = contract.id
  const tradeTarget = Math.max(1, Number(contract.objective?.n || 1))

  const acceptA = await service.applyGodCommand({
    agents,
    command: `contract accept Mara ${contractId}`,
    operationId: 'contract-flow-accept-a'
  })
  assert.equal(acceptA.applied, true)
  assert.ok(acceptA.outputLines.some(line => line.includes(`id=${contractId}`)))

  const afterAccept = memoryStore.getSnapshot()
  const accepted = afterAccept.world.quests.find(quest => quest.id === contractId)
  assert.equal(accepted.owner, 'Mara')
  assert.ok(accepted.state === 'accepted' || accepted.state === 'in_progress')

  const maraBeforeTrades = Number(afterAccept.world.economy.ledger.Mara || 0)

  for (let idx = 0; idx < tradeTarget; idx += 1) {
    const tradeResult = await service.applyGodCommand({
      agents,
      command: `trade bazaar ${offerId} Mara 1`,
      operationId: `contract-flow-trade-${idx + 1}`
    })
    assert.equal(tradeResult.applied, true)
  }

  const tradeReplay = await service.applyGodCommand({
    agents,
    command: `trade bazaar ${offerId} Mara 1`,
    operationId: `contract-flow-trade-${tradeTarget}`
  })
  assert.equal(tradeReplay.applied, false)

  const afterTrades = memoryStore.getSnapshot()
  const completed = afterTrades.world.quests.find(quest => quest.id === contractId)
  assert.equal(completed.state, 'completed')
  const maraAfterTrades = Number(afterTrades.world.economy.ledger.Mara || 0)
  const expectedAfterTrades = maraBeforeTrades - tradeTarget + Number(completed.reward || 0)
  assert.equal(maraAfterTrades, expectedAfterTrades)

  const completeA = await service.applyGodCommand({
    agents,
    command: `contract complete ${contractId}`,
    operationId: 'contract-flow-complete-a'
  })
  assert.equal(completeA.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), afterTrades)

  const completeAgain = await service.applyGodCommand({
    agents,
    command: `contract complete ${contractId}`,
    operationId: 'contract-flow-complete-b'
  })
  assert.equal(completeAgain.applied, false)
  const afterCompleteAgain = memoryStore.getSnapshot()
  assert.equal(Number(afterCompleteAgain.world.economy.ledger.Mara || 0), maraAfterTrades)
})

test('decision choose applies effects once and replay is safe', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'decision-choose-seed-town'
  })
  await memoryStore.transact((memory) => {
    memory.world.clock = { day: 3, phase: 'day', season: 'dawn', updated_at: '2026-02-22T00:00:00.000Z' }
    memory.world.threat = { byTown: { alpha: 10 } }
    memory.world.moods = { byTown: { alpha: { fear: 0, unrest: 0, prosperity: 0 } } }
    memory.world.decisions = [
      {
        id: 'd_choose_alpha',
        town: 'alpha',
        event_id: 'e_choose_alpha',
        event_type: 'shortage',
        prompt: 'Storehouses are thinning. What policy will the mayor choose?',
        options: [
          { key: 'ration', label: 'Ration Bread', effects: { mood: { unrest: 1, prosperity: -1 }, threat_delta: -1 } },
          { key: 'free_market', label: 'Free Market', effects: { mood: { prosperity: 2, unrest: 1 }, threat_delta: 1 } }
        ],
        state: 'open',
        starts_day: 3,
        expires_day: 3,
        created_at: 1000
      }
    ]
  }, { eventId: 'decision-choose-seed-state' })

  const chooseA = await service.applyGodCommand({
    agents,
    command: 'decision choose d_choose_alpha ration',
    operationId: 'decision-choose-a'
  })
  assert.equal(chooseA.applied, true)
  const afterChoose = memoryStore.getSnapshot()
  assert.equal(afterChoose.world.decisions[0].state, 'chosen')
  assert.equal(afterChoose.world.decisions[0].chosen_key, 'ration')
  assert.equal(afterChoose.world.threat.byTown.alpha, 9)
  assert.equal(afterChoose.world.moods.byTown.alpha.unrest, 1)
  assert.equal(afterChoose.world.moods.byTown.alpha.prosperity, 0)

  const chooseReplay = await service.applyGodCommand({
    agents,
    command: 'decision choose d_choose_alpha ration',
    operationId: 'decision-choose-a'
  })
  assert.equal(chooseReplay.applied, false)
  assert.deepEqual(memoryStore.getSnapshot(), afterChoose)
})

test('decision choose can spawn rumor and replay does not duplicate it', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'decision-rumor-seed-town'
  })
  await memoryStore.transact((memory) => {
    memory.world.clock = { day: 4, phase: 'night', season: 'dawn', updated_at: '2026-02-22T00:00:00.000Z' }
    memory.world.decisions = [
      {
        id: 'd_rumor_alpha',
        town: 'alpha',
        event_id: 'e_rumor_alpha',
        event_type: 'fog',
        prompt: 'Fog blankets the roads tonight. Which order stands?',
        options: [
          { key: 'send_scouts', label: 'Send Scouts', effects: { mood: { fear: 1 }, rumor_spawn: { kind: 'supernatural', severity: 2, templateKey: 'mist_shapes', expiresInDays: 1 } } },
          { key: 'stay_inside', label: 'Curfew Bells', effects: { mood: { fear: -1 }, threat_delta: -1 } }
        ],
        state: 'open',
        starts_day: 4,
        expires_day: 4,
        created_at: 2000
      }
    ]
  }, { eventId: 'decision-rumor-seed-state' })

  const chooseA = await service.applyGodCommand({
    agents,
    command: 'decision choose d_rumor_alpha send_scouts',
    operationId: 'decision-rumor-choose-a'
  })
  assert.equal(chooseA.applied, true)
  const afterChoose = memoryStore.getSnapshot()
  assert.equal(afterChoose.world.decisions[0].state, 'chosen')
  assert.equal(afterChoose.world.rumors.length, 1)
  const rumorId = afterChoose.world.rumors[0].id

  const chooseReplay = await service.applyGodCommand({
    agents,
    command: 'decision choose d_rumor_alpha send_scouts',
    operationId: 'decision-rumor-choose-a'
  })
  assert.equal(chooseReplay.applied, false)
  const afterReplay = memoryStore.getSnapshot()
  assert.equal(afterReplay.world.rumors.length, 1)
  assert.equal(afterReplay.world.rumors[0].id, rumorId)
})

test('trait/title read-only commands do not mutate state', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  let txCalls = 0
  const originalTransact = memoryStore.transact.bind(memoryStore)
  memoryStore.transact = async (...args) => {
    txCalls += 1
    return originalTransact(...args)
  }

  const before = memoryStore.getSnapshot()
  const traitShow = await service.applyGodCommand({
    agents,
    command: 'trait Mara',
    operationId: 'trait-readonly-show'
  })
  const titleShow = await service.applyGodCommand({
    agents,
    command: 'title Mara',
    operationId: 'title-readonly-show'
  })
  const after = memoryStore.getSnapshot()

  assert.equal(traitShow.applied, true)
  assert.equal(titleShow.applied, true)
  assert.equal(txCalls, 0)
  assert.deepEqual(after, before)
  assert.ok(traitShow.outputLines.some(line => line.includes('GOD TRAIT: agent=Mara')))
  assert.ok(titleShow.outputLines.some(line => line.includes('GOD TITLE: agent=Mara')))
})

test('trait set and title grant/revoke are transactional and idempotent', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  const traitA = await service.applyGodCommand({
    agents,
    command: 'trait set Mara courage 3',
    operationId: 'trait-set-a'
  })
  const traitReplay = await service.applyGodCommand({
    agents,
    command: 'trait set Mara courage 3',
    operationId: 'trait-set-a'
  })
  assert.equal(traitA.applied, true)
  assert.equal(traitReplay.applied, false)

  const grantA = await service.applyGodCommand({
    agents,
    command: 'title grant Mara Night Watch',
    operationId: 'title-grant-a'
  })
  const grantReplay = await service.applyGodCommand({
    agents,
    command: 'title grant Mara Night Watch',
    operationId: 'title-grant-a'
  })
  assert.equal(grantA.applied, true)
  assert.equal(grantReplay.applied, false)

  const revokeA = await service.applyGodCommand({
    agents,
    command: 'title revoke Mara Night Watch',
    operationId: 'title-revoke-a'
  })
  const revokeReplay = await service.applyGodCommand({
    agents,
    command: 'title revoke Mara Night Watch',
    operationId: 'title-revoke-a'
  })
  assert.equal(revokeA.applied, true)
  assert.equal(revokeReplay.applied, false)

  const snapshot = memoryStore.getSnapshot()
  assert.equal(snapshot.agents.Mara.profile.traits.courage, 3)
  assert.equal(snapshot.agents.Mara.profile.titles.includes('Night Watch'), false)
  assert.equal(memoryStore.validateMemoryIntegrity().ok, true)
})

test('rep threshold title award is replay-safe and emits title news once', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  const repA = await service.applyGodCommand({
    agents,
    command: 'rep add Mara iron_pact 5',
    operationId: 'rep-title-a'
  })
  const repReplay = await service.applyGodCommand({
    agents,
    command: 'rep add Mara iron_pact 5',
    operationId: 'rep-title-a'
  })
  assert.equal(repA.applied, true)
  assert.equal(repReplay.applied, false)

  const afterFirst = memoryStore.getSnapshot()
  const titles = afterFirst.agents.Mara.profile.titles
  assert.equal(titles.filter(item => item === 'Pact Friend').length, 1)
  const titleNewsAfterFirst = afterFirst.world.news
    .filter(entry => entry.topic === 'title' && entry.meta?.title === 'Pact Friend')
  assert.equal(titleNewsAfterFirst.length, 1)

  const repB = await service.applyGodCommand({
    agents,
    command: 'rep add Mara iron_pact 1',
    operationId: 'rep-title-b'
  })
  assert.equal(repB.applied, true)
  const afterSecond = memoryStore.getSnapshot()
  assert.equal(afterSecond.agents.Mara.profile.titles.filter(item => item === 'Pact Friend').length, 1)
  const titleNewsAfterSecond = afterSecond.world.news
    .filter(entry => entry.topic === 'title' && entry.meta?.title === 'Pact Friend')
  assert.equal(titleNewsAfterSecond.length, 1)
})

test('rumor side-quest completion awards Wanderer once and is replay-safe', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  await service.applyGodCommand({
    agents,
    command: 'mark add alpha_hall 0 64 0 town:alpha',
    operationId: 'wanderer-seed-town'
  })
  await service.applyGodCommand({
    agents,
    command: 'rumor spawn alpha supernatural 2 mist_shapes 2',
    operationId: 'wanderer-seed-rumor'
  })
  const rumorId = memoryStore.getSnapshot().world.rumors[0].id
  await service.applyGodCommand({
    agents,
    command: `rumor quest ${rumorId}`,
    operationId: 'wanderer-seed-quest'
  })
  const questId = memoryStore.getSnapshot().world.quests.find(q => q.type === 'rumor_task').id
  await service.applyGodCommand({
    agents,
    command: `quest accept Mara ${questId}`,
    operationId: 'wanderer-seed-accept'
  })
  await service.applyGodCommand({
    agents,
    command: 'trait set Mara courage 1',
    operationId: 'wanderer-seed-trait'
  })
  await memoryStore.transact((memory) => {
    memory.agents.Mara.profile.rumors_completed = 2
  }, { eventId: 'wanderer-seed-progress' })

  const visitA = await service.applyGodCommand({
    agents,
    command: `quest visit ${questId}`,
    operationId: 'wanderer-visit-a'
  })
  assert.equal(visitA.applied, true)
  const afterVisit = memoryStore.getSnapshot()
  assert.equal(afterVisit.agents.Mara.profile.rumors_completed, 3)
  assert.equal(afterVisit.agents.Mara.profile.titles.filter(item => item === 'Wanderer').length, 1)

  const visitReplay = await service.applyGodCommand({
    agents,
    command: `quest visit ${questId}`,
    operationId: 'wanderer-visit-a'
  })
  assert.equal(visitReplay.applied, false)
  const afterReplay = memoryStore.getSnapshot()
  assert.equal(afterReplay.agents.Mara.profile.rumors_completed, 3)
  assert.equal(afterReplay.agents.Mara.profile.titles.filter(item => item === 'Wanderer').length, 1)
  const wandererNews = afterReplay.world.news
    .filter(entry => entry.topic === 'title' && entry.meta?.title === 'Wanderer')
  assert.equal(wandererNews.length, 1)
})
