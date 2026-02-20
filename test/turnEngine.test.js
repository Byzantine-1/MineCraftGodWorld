const fs = require('fs')
const os = require('os')
const path = require('path')
const test = require('node:test')
const assert = require('node:assert/strict')

const Agent = require('../src/agent')
const { createMemoryStore } = require('../src/memory')
const { createTurnEngine } = require('../src/turnEngine')

function createStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-turn-'))
  const filePath = path.join(dir, 'memory.json')
  return createMemoryStore({ filePath })
}

test('turn engine does not double-mutate agent state for duplicate operationId', async () => {
  const memoryStore = createStore()
  const actionEngine = {
    applyProposedActions: async () => []
  }
  const turnEngine = createTurnEngine({ memoryStore, actionEngine })
  const agent = new Agent({ name: 'Mara', role: 'Scout', faction: 'Pilgrims' })

  const fallbackTurn = {
    say: 'Speak.',
    tone: 'wary',
    trust_delta: 0,
    memory_writes: [],
    proposed_actions: [{ type: 'none', target: 'none', confidence: 0, reason: 'fallback' }]
  }
  const rawTurn = {
    say: 'Trust me.',
    tone: 'joyful',
    trust_delta: 2,
    memory_writes: [],
    proposed_actions: []
  }

  const first = await turnEngine.applyTurn({
    agent,
    rawTurn,
    fallbackTurn,
    operationId: 'turn-idem-1'
  })
  const trustAfterFirst = agent.trust

  const second = await turnEngine.applyTurn({
    agent,
    rawTurn,
    fallbackTurn,
    operationId: 'turn-idem-1'
  })

  assert.equal(first.skipped, false)
  assert.equal(second.skipped, true)
  assert.equal(agent.trust, trustAfterFirst)
  assert.equal(agent.mood, 'happy')

  const persisted = memoryStore.recallAgent(agent.name)
  assert.equal(persisted.profile.trust, trustAfterFirst)
  assert.equal(persisted.profile.mood, 'happy')
  assert.equal(persisted.profile.flags.rebellious, false)
})
