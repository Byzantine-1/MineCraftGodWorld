const test = require('node:test')
const assert = require('node:assert/strict')

const {
  parseCliInput,
  parseBridgeChat,
  sanitizeMinecraftName
} = require('../src/commandParsers')

function mulberry32(seed) {
  let a = seed >>> 0
  return function rng() {
    a |= 0
    a = (a + 0x6d2b79f5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

function randInt(rng, min, maxInclusive) {
  return Math.floor(rng() * (maxInclusive - min + 1)) + min
}

function hasControlChars(value) {
  return /[\u0000-\u001f\u007f]/.test(String(value || ''))
}

function randomCodePoint(rng) {
  const roll = rng()
  if (roll < 0.62) return randInt(rng, 0x20, 0x7e)
  if (roll < 0.77) return randInt(rng, 0x00, 0x1f)
  if (roll < 0.92) return randInt(rng, 0x80, 0x2ff)
  return randInt(rng, 0x1f300, 0x1f64f)
}

function randomString(rng, maxLen = 1200) {
  const length = randInt(rng, 0, maxLen)
  let out = ''
  for (let idx = 0; idx < length; idx += 1) {
    out += String.fromCodePoint(randomCodePoint(rng))
  }
  return out
}

function assertCliParseInvariants(parsed) {
  const allowed = new Set(['noop', 'error', 'talk', 'god', 'exit', 'unknown'])
  assert.equal(allowed.has(parsed.type), true, `unexpected cli type: ${parsed.type}`)

  if (parsed.type === 'god') {
    assert.equal(typeof parsed.command, 'string')
    assert.equal(parsed.command.length <= 240, true)
    assert.equal(hasControlChars(parsed.command), false)
  }

  if (parsed.type === 'talk') {
    assert.equal(typeof parsed.target, 'string')
    assert.equal(typeof parsed.message, 'string')
    assert.equal(parsed.target, parsed.target.toLowerCase())
    assert.equal(parsed.target.length <= 96, true)
    assert.equal(parsed.message.length <= 600, true)
    assert.equal(hasControlChars(parsed.message), false)
  }

  if (parsed.type === 'unknown') {
    assert.equal(typeof parsed.command, 'string')
  }
}

function assertBridgeParseInvariants(parsed) {
  const allowed = new Set(['noop', 'party_on', 'party_off', 'party_leader', 'god', 'npc_talk'])
  assert.equal(allowed.has(parsed.type), true, `unexpected bridge type: ${parsed.type}`)

  if (parsed.type === 'god') {
    assert.equal(typeof parsed.command, 'string')
    assert.equal(parsed.command.length <= 240, true)
    assert.equal(hasControlChars(parsed.command), false)
  }

  if (parsed.type === 'npc_talk') {
    assert.equal(typeof parsed.target, 'string')
    assert.equal(typeof parsed.message, 'string')
    assert.equal(parsed.target, parsed.target.toLowerCase())
    assert.equal(parsed.target.length <= 96, true)
    assert.equal(parsed.message.length <= 600, true)
    assert.equal(hasControlChars(parsed.message), false)
  }

  if (parsed.type === 'party_leader') {
    const leaderName = parsed.leaderName
    if (leaderName !== null) {
      assert.equal(typeof leaderName, 'string')
      assert.equal(sanitizeMinecraftName(leaderName), leaderName)
    }
  }
}

test('command parsers survive seeded fuzz corpus with bounded sanitized output', () => {
  const rng = mulberry32(0x7f4a7c15)
  const corpus = [
    '',
    ' ',
    '\u0000\u0001\u0002',
    'talk mara hello there',
    'talk mara',
    'talk  mara    hello   there',
    'god mint Mara 1',
    'god',
    'party on',
    'party off',
    'party leader Steve_123',
    'party leader !@#$',
    'mara: scout ahead',
    'not-a-command',
    'A'.repeat(20_000),
    'god ' + 'x'.repeat(50_000),
    'talk mara ' + 'y'.repeat(50_000),
    'mara: ' + 'z'.repeat(50_000),
    'god \u0000\u0001\u001b\u007f'
  ]

  for (let idx = 0; idx < 1500; idx += 1) {
    corpus.push(randomString(rng, 1500))
  }

  for (const input of corpus) {
    const cliA = parseCliInput(input)
    const cliB = parseCliInput(input)
    assert.deepEqual(cliA, cliB, 'parseCliInput should be deterministic for same input')
    assertCliParseInvariants(cliA)

    const bridgeA = parseBridgeChat(input)
    const bridgeB = parseBridgeChat(input)
    assert.deepEqual(bridgeA, bridgeB, 'parseBridgeChat should be deterministic for same input')
    assertBridgeParseInvariants(bridgeA)
  }
})

test('command parsers enforce output length caps on oversized payloads', () => {
  const giant = 'w'.repeat(200_000)

  const cliGod = parseCliInput(`god ${giant}`)
  assert.equal(cliGod.type, 'god')
  assert.equal(cliGod.command.length <= 240, true)

  const cliTalk = parseCliInput(`talk mara ${giant}`)
  assert.equal(cliTalk.type, 'talk')
  assert.equal(cliTalk.message.length <= 600, true)

  const bridgeGod = parseBridgeChat(`god ${giant}`)
  assert.equal(bridgeGod.type, 'god')
  assert.equal(bridgeGod.command.length <= 240, true)

  const bridgeTalk = parseBridgeChat(`mara: ${giant}`)
  assert.equal(bridgeTalk.type, 'npc_talk')
  assert.equal(bridgeTalk.message.length <= 600, true)
})
