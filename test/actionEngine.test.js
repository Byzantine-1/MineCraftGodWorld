const fs = require('fs')
const os = require('os')
const path = require('path')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createMemoryStore } = require('../src/memory')
const { createActionEngine } = require('../src/actionEngine')

function createStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-action-'))
  const filePath = path.join(dir, 'memory.json')
  return createMemoryStore({ filePath })
}

test('action engine applies rumor action deterministically', async () => {
  const memoryStore = createStore()
  const actionEngine = createActionEngine({ memoryStore })
  const agent = { name: 'Mara', faction: 'Pilgrims' }

  const outcomes = await actionEngine.applyProposedActions({
    agent,
    proposedActions: [{ type: 'spread_rumor', reason: 'test' }],
    operationId: 'turn-1'
  })
  assert.equal(outcomes.length, 1)
  assert.equal(outcomes[0].accepted, true)

  const world = memoryStore.recallWorld()
  assert.equal(world.player.legitimacy, 48)
  assert.equal(world.factions.Pilgrims.hostilityToPlayer, 13)
})

test('action engine ignores duplicate operation id', async () => {
  const memoryStore = createStore()
  const actionEngine = createActionEngine({ memoryStore })
  const agent = { name: 'Mara', faction: 'Pilgrims' }

  await actionEngine.applyProposedActions({
    agent,
    proposedActions: [{ type: 'recruit', reason: 'first' }],
    operationId: 'turn-dup'
  })
  const second = await actionEngine.applyProposedActions({
    agent,
    proposedActions: [{ type: 'recruit', reason: 'retry' }],
    operationId: 'turn-dup'
  })

  assert.equal(second[0].accepted, false)
  assert.match(second[0].reason, /Duplicate operation/i)
  const world = memoryStore.recallWorld()
  assert.equal(world.factions.Pilgrims.stability, 71)
})
