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

  const agents = [
    { name: 'Mara', faction: 'Pilgrims', applyGodCommand: () => {} },
    { name: 'Eli', faction: 'Pilgrims', applyGodCommand: () => {} }
  ]

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
