const test = require('node:test')
const assert = require('node:assert/strict')

const { runLiveExecutionCheck } = require('../scripts/liveExecutionCheck')

test('live CLI execution handoff yields canonical deterministic authoritative results', async () => {
  const result = await runLiveExecutionCheck({
    backend: 'memory',
    timeoutMs: 15000
  })

  assert.equal(result.capturedLiveFromChildProcess, true)
  assert.equal(result.deterministicReplayVerified, true)
  assert.equal(result.result.type, 'execution-result.v1')
  assert.equal(result.result.schemaVersion, 1)
  assert.equal(result.result.status, 'executed')
  assert.equal(result.result.accepted, true)
  assert.equal(result.result.executed, true)
  assert.equal(result.result.reasonCode, 'EXECUTED')
  assert.equal(result.result.handoffId, result.handoff.handoffId)
  assert.equal(result.receipt.executionId, result.result.executionId)
  assert.equal(result.pendingExecutions.length, 0)
  assert(result.stdoutLines.some((line) => line.includes('--- WORLD ONLINE ---')))
})
