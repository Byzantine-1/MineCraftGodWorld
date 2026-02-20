const test = require('node:test')
const assert = require('node:assert/strict')

const Agent = require('../src/agent')

test('applyGodCommand toggles war/peace and trust boundaries', () => {
  const agent = new Agent({ name: 'Mara', role: 'Scout', faction: 'Pilgrims' })

  agent.applyGodCommand('declare_war')
  assert.equal(agent.combatState, 'war')
  assert.equal(agent.mood, 'angry')

  agent.applyGodCommand('make_peace')
  assert.equal(agent.combatState, 'peace')
  assert.equal(agent.mood, 'calm')

  agent.trust = 10
  agent.applyGodCommand('bless_people')
  assert.equal(agent.trust, 10)
})

test('applyNpcTurn clamps trust delta and marks rebellious at low trust', () => {
  const agent = new Agent({ name: 'Eli', role: 'Guard', faction: 'Pilgrims' })

  agent.trust = 2
  agent.applyNpcTurn({ trust_delta: -100, tone: 'fearful' })
  assert.equal(agent.trust, 0)
  assert.equal(agent.mood, 'fearful')
  assert.equal(agent.flags.rebellious, true)
})
