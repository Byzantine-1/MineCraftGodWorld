const fs = require('fs')
const os = require('os')
const path = require('path')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createMemoryStore } = require('../src/memory')

function createTempMemoryPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-memory-'))
  return path.join(dir, 'memory.json')
}

test('memory store deduplicates idempotent world events', async () => {
  const filePath = createTempMemoryPath()
  const store = createMemoryStore({ filePath })

  await store.rememberWorld('event-one', false, 'op-1')
  await store.rememberWorld('event-one-duplicate', false, 'op-1')

  const snapshot = store.loadAllMemory()
  assert.equal(snapshot.world.archive.length, 1)
  assert.equal(snapshot.world.archive[0].event, 'event-one')
})

test('memory store snapshots are isolated from external mutation', () => {
  const filePath = createTempMemoryPath()
  const store = createMemoryStore({ filePath })

  const snapshotA = store.loadAllMemory()
  snapshotA.world.player.alive = false

  const snapshotB = store.loadAllMemory()
  assert.equal(snapshotB.world.player.alive, true)
})
