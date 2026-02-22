const fs = require('fs')
const os = require('os')
const path = require('path')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createMemoryStore } = require('../src/memory')
const { createGodCommandService } = require('../src/godCommands')

function createStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-feed-cap-'))
  const filePath = path.join(dir, 'memory.json')
  return {
    filePath,
    store: createMemoryStore({ filePath })
  }
}

function createAgents() {
  return [
    { name: 'Mara', faction: 'Pilgrims', applyGodCommand: () => {} },
    { name: 'Eli', faction: 'Pilgrims', applyGodCommand: () => {} }
  ]
}

function parseTailMetrics(outputLines, regex) {
  for (const line of outputLines || []) {
    const match = String(line).match(regex)
    if (!match) continue
    return {
      count: Number(match[1]),
      total: Number(match[2])
    }
  }
  return null
}

test('news and chronicle feeds stay bounded at 200 under write burst and reload', async () => {
  const { filePath, store } = createStore()
  const service = createGodCommandService({ memoryStore: store })
  const agents = createAgents()

  const burstCount = 265
  for (let idx = 0; idx < burstCount; idx += 1) {
    const result = await service.applyGodCommand({
      agents,
      command: `mark add cap_${idx} ${idx} 64 ${-idx} cap`,
      operationId: `feed-cap-mark-${idx}`
    })
    assert.equal(result.applied, true)
  }

  const snapshot = store.getSnapshot()
  assert.equal(snapshot.world.news.length, 200)
  assert.equal(snapshot.world.chronicle.length, 200)
  assert.equal(store.validateMemoryIntegrity().ok, true)

  const earliestDroppedName = 'cap_0'
  const latestName = `cap_${burstCount - 1}`
  const hasOldNews = snapshot.world.news.some(entry => entry?.meta?.marker === earliestDroppedName)
  const hasOldChronicle = snapshot.world.chronicle.some(entry => entry?.meta?.marker === earliestDroppedName)
  const hasLatestNews = snapshot.world.news.some(entry => entry?.meta?.marker === latestName)
  const hasLatestChronicle = snapshot.world.chronicle.some(entry => entry?.meta?.marker === latestName)

  assert.equal(hasOldNews, false)
  assert.equal(hasOldChronicle, false)
  assert.equal(hasLatestNews, true)
  assert.equal(hasLatestChronicle, true)

  const newsTail = await service.applyGodCommand({
    agents,
    command: 'news tail 999',
    operationId: 'feed-cap-news-tail'
  })
  const chronicleTail = await service.applyGodCommand({
    agents,
    command: 'chronicle tail 999',
    operationId: 'feed-cap-chronicle-tail'
  })
  assert.equal(newsTail.applied, true)
  assert.equal(chronicleTail.applied, true)

  const newsMetrics = parseTailMetrics(newsTail.outputLines, /GOD NEWS TAIL:\s*count=(\d+)\s+total=(\d+)/i)
  const chronicleMetrics = parseTailMetrics(chronicleTail.outputLines, /GOD CHRONICLE TAIL:\s*count=(\d+)\s+total=(\d+)/i)
  assert.ok(newsMetrics)
  assert.ok(chronicleMetrics)
  assert.equal(newsMetrics.count, 200)
  assert.equal(newsMetrics.total, 200)
  assert.equal(chronicleMetrics.count, 200)
  assert.equal(chronicleMetrics.total, 200)

  const reloadedStore = createMemoryStore({ filePath })
  const reloaded = reloadedStore.loadAllMemory()
  assert.equal(reloaded.world.news.length, 200)
  assert.equal(reloaded.world.chronicle.length, 200)
  assert.equal(reloadedStore.validateMemoryIntegrity().ok, true)
})
