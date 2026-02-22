const fs = require('fs')
const os = require('os')
const path = require('path')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createMemoryStore } = require('../src/memory')
const { createGodCommandService } = require('../src/godCommands')
const { deriveOperationId } = require('../src/flowControl')

function createStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-op-boundary-'))
  const filePath = path.join(dir, 'memory.json')
  return createMemoryStore({ filePath })
}

function createAgents() {
  return [
    { name: 'Mara', faction: 'Pilgrims', applyGodCommand: () => {} },
    { name: 'Eli', faction: 'Pilgrims', applyGodCommand: () => {} }
  ]
}

test('deriveOperationId changes across window boundaries', () => {
  const parts = ['cli', 'god', 'mint mara 1']
  const opWithinA = deriveOperationId(parts, { windowMs: 5000, now: () => 4_999 })
  const opWithinB = deriveOperationId(parts, { windowMs: 5000, now: () => 4_001 })
  const opNext = deriveOperationId(parts, { windowMs: 5000, now: () => 5_000 })

  assert.equal(opWithinA, opWithinB)
  assert.notEqual(opWithinA, opNext)
})

test('same explicit operationId remains single-apply under retries', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  const first = await service.applyGodCommand({
    agents,
    command: 'mint Mara 3',
    operationId: 'fixed-op-id'
  })
  const replay = await service.applyGodCommand({
    agents,
    command: 'mint Mara 3',
    operationId: 'fixed-op-id'
  })

  assert.equal(first.applied, true)
  assert.equal(replay.applied, false)
  assert.equal(memoryStore.getSnapshot().world.economy.ledger.Mara, 3)
})

test('different explicit operationIds apply independently for identical commands', async () => {
  const memoryStore = createStore()
  const service = createGodCommandService({ memoryStore })
  const agents = createAgents()

  const opA = await service.applyGodCommand({
    agents,
    command: 'mint Mara 4',
    operationId: 'op-a'
  })
  const opB = await service.applyGodCommand({
    agents,
    command: 'mint Mara 4',
    operationId: 'op-b'
  })

  assert.equal(opA.applied, true)
  assert.equal(opB.applied, true)
  assert.equal(memoryStore.getSnapshot().world.economy.ledger.Mara, 8)
})
