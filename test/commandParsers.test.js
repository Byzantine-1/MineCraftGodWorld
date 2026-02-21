const test = require('node:test')
const assert = require('node:assert/strict')

const { parseCliInput, parseBridgeChat } = require('../src/commandParsers')

test('parseCliInput parses talk command with target and message', () => {
  const parsed = parseCliInput('talk mara hello there')
  assert.equal(parsed.type, 'talk')
  assert.equal(parsed.target, 'mara')
  assert.equal(parsed.message, 'hello there')
})

test('parseCliInput rejects malformed talk command', () => {
  const parsed = parseCliInput('talk mara')
  assert.equal(parsed.type, 'error')
})

test('parseBridgeChat parses npc talk and party commands', () => {
  const talk = parseBridgeChat('mara: scout ahead')
  assert.equal(talk.type, 'npc_talk')
  assert.equal(talk.target, 'mara')

  const partyOn = parseBridgeChat('party on')
  assert.equal(partyOn.type, 'party_on')
})

test('parseCliInput preserves full multi-word god command payload', () => {
  const parsed = parseCliInput('god intent set Mara follow Eli')
  assert.equal(parsed.type, 'god')
  assert.equal(parsed.command, 'intent set Mara follow Eli')
})

test('parseBridgeChat preserves full god command payload', () => {
  const parsed = parseBridgeChat('god say Mara Hold this line')
  assert.equal(parsed.type, 'god')
  assert.equal(parsed.command, 'say Mara Hold this line')
})
