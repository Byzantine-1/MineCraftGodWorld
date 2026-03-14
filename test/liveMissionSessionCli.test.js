const test = require('node:test')
const assert = require('node:assert/strict')

const { runLiveMissionSessionCheck } = require('../scripts/liveMissionSessionCheck')

test('live CLI mission session keeps mission lifecycle and world-memory coherent in one engine process', async () => {
  const result = await runLiveMissionSessionCheck({
    backend: 'sqlite',
    timeoutMs: 15000
  })

  assert.equal(result.responsesCapturedLiveFromSameChildProcess, true)
  assert.equal(result.deterministicReplayVerified, true)
  assert.equal(result.preExecutionContext.type, 'world-memory-context.v1')
  assert.equal(result.acceptResult.type, 'execution-result.v1')
  assert.equal(result.advanceResult.type, 'execution-result.v1')
  assert.equal(result.completeResult.type, 'execution-result.v1')
  assert.equal(result.postCompleteContext.type, 'world-memory-context.v1')
  assert.equal(result.acceptResult.status, 'executed')
  assert.equal(result.advanceResult.status, 'executed')
  assert.equal(result.completeResult.status, 'executed')
  assert.equal(result.preExecutionContext.townSummary.activeMajorMissionId, null)
  assert(result.postAcceptContext.townSummary.activeMajorMissionId)
  assert.equal(result.postAdvanceContext.townSummary.activeMajorMissionId, result.postAcceptContext.townSummary.activeMajorMissionId)
  assert.equal(result.postCompleteContext.townSummary.activeMajorMissionId, null)
  assert(result.postCompleteContext.townSummary.historyCount > result.preExecutionContext.townSummary.historyCount)
  assert(result.postCompleteContext.townSummary.hope > result.preExecutionContext.townSummary.hope)
  assert(result.postCompleteContext.townSummary.dread < result.preExecutionContext.townSummary.dread)
  assert.equal(result.pendingExecutions.length, 0)
  assert.equal(result.completedMissionStatus, 'completed')
})
