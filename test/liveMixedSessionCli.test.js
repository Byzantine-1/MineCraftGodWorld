const test = require('node:test')
const assert = require('node:assert/strict')

const { runLiveMixedSessionCheck } = require('../scripts/liveMixedSessionCheck')

test('live CLI mixed session keeps retrieval and execution coherent in one engine process', async () => {
  const result = await runLiveMixedSessionCheck({
    backend: 'memory',
    timeoutMs: 15000
  })

  assert.equal(result.responsesCapturedLiveFromSameChildProcess, true)
  assert.equal(result.deterministicReplayVerified, true)
  assert.equal(result.preExecutionContext.type, 'world-memory-context.v1')
  assert.equal(result.executionResult.type, 'execution-result.v1')
  assert.equal(result.postExecutionContext.type, 'world-memory-context.v1')
  assert.equal(result.executionResult.status, 'executed')
  assert.equal(result.executionResult.accepted, true)
  assert.equal(result.executionResult.executed, true)
  assert.equal(result.preExecutionContext.townSummary.historyCount, 0)
  assert(result.postExecutionContext.townSummary.historyCount > result.preExecutionContext.townSummary.historyCount)
  assert(result.postExecutionContext.recentHistory.some((entry) => entry.handoffId === result.handoff.handoffId))
  assert.equal(result.pendingExecutions.length, 0)
  assert.equal(result.projectStage, 2)
})
