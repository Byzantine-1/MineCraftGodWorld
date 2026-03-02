const test = require('node:test')
const assert = require('node:assert/strict')

const { runLiveWorldMemoryCheck } = require('../scripts/liveWorldMemoryCheck')

test('live CLI world-memory retrieval responds with canonical bounded deterministic JSON', async () => {
  const result = await runLiveWorldMemoryCheck({
    backend: 'memory',
    timeoutMs: 15000
  })

  assert.equal(result.responsesCapturedLive, 2)
  assert.equal(result.response.type, 'world-memory-context.v1')
  assert.equal(result.response.schemaVersion, 1)
  assert.deepEqual(result.response.scope, {
    townId: 'alpha',
    factionId: 'iron_pact',
    chronicleLimit: 2,
    historyLimit: 3
  })
  assert.equal(result.response.recentChronicle.length, 2)
  assert.equal(result.response.recentHistory.length, 3)
  assert.equal(result.response.townSummary.townId, 'alpha')
  assert.equal(result.response.factionSummary.factionId, 'iron_pact')
  assert(result.stdoutLines.some((line) => line.includes('--- WORLD ONLINE ---')))
})
