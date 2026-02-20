const test = require('node:test')
const assert = require('node:assert/strict')

const { sanitizeTurn } = require('../src/turnGuard')

test('sanitizeTurn falls back for invalid payloads', () => {
  const fallback = {
    say: 'Speak.',
    tone: 'wary',
    trust_delta: 0,
    memory_writes: [],
    proposed_actions: [{ type: 'none', target: 'none', confidence: 0, reason: 'fallback' }]
  }

  const safe = sanitizeTurn(null, fallback)
  assert.equal(safe.say, 'Speak.')
  assert.equal(safe.tone, 'wary')
  assert.equal(safe.trust_delta, 0)
  assert.equal(safe.proposed_actions[0].type, 'none')
})

test('sanitizeTurn clamps trust delta and filters bad writes/actions', () => {
  const fallback = {
    say: 'Speak.',
    tone: 'wary',
    trust_delta: 0,
    memory_writes: [],
    proposed_actions: [{ type: 'none', target: 'none', confidence: 0, reason: 'fallback' }]
  }

  const safe = sanitizeTurn({
    say: '  hello  ',
    tone: 'hostile',
    trust_delta: 999,
    memory_writes: [
      { scope: 'agent', text: 'x', importance: 20 },
      { scope: 'bad', text: 'ignored', importance: 5 }
    ],
    proposed_actions: [
      { type: 'attack_player', target: 'player', confidence: 9, reason: 'x' },
      { type: 'not_real', target: 'none', confidence: 0, reason: 'bad' }
    ]
  }, fallback)

  assert.equal(safe.say, 'hello')
  assert.equal(safe.tone, 'hostile')
  assert.equal(safe.trust_delta, 2)
  assert.equal(safe.memory_writes.length, 1)
  assert.equal(safe.memory_writes[0].importance, 10)
  assert.equal(safe.proposed_actions.length, 1)
  assert.equal(safe.proposed_actions[0].type, 'attack_player')
  assert.equal(safe.proposed_actions[0].confidence, 1)
})
