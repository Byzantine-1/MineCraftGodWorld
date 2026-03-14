const test = require('node:test')
const assert = require('node:assert/strict')

const { runLiveStarterTownSpawnCheck } = require('../scripts/liveStarterTownSpawnCheck')

test('live CLI starter town spawn keeps assignment and teleport hints coherent in one engine process', async () => {
  const result = await runLiveStarterTownSpawnCheck({
    backend: 'sqlite',
    timeoutMs: 15000
  })

  assert.equal(result.backend, 'sqlite')
  assert.equal(result.setSpawnResult.status, 'executed')
  assert.equal(result.assignResult.status, 'executed')
  assert.equal(result.spawnResult.status, 'executed')
  assert.equal(result.spawnResult.embodiment.actions[0].type, 'teleport')
  assert.equal(result.playerRecord.playerId, 'Builder01')
  assert.equal(result.playerRecord.townId, 'alpha')
  assert.equal(result.deterministicReplayVerified, true)
})
