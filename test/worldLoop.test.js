const fs = require('fs')
const os = require('os')
const path = require('path')
const test = require('node:test')
const assert = require('node:assert/strict')

const { createMemoryStore } = require('../src/memory')
const { createWorldLoop } = require('../src/worldLoop')

function stableObservability() {
  return {
    txDurationTotalMs: 0,
    txDurationCount: 0,
    txDurationMaxMs: 0,
    slowTransactionCount: 0,
    lockAcquisitionTotalMs: 0,
    lockAcquisitionCount: 0,
    txDurationP50Ms: 0,
    txDurationP95Ms: 0,
    txDurationP99Ms: 0,
    txPhaseP95Ms: {
      lockWaitMs: 0,
      cloneMs: 0,
      stringifyMs: 0,
      writeMs: 0,
      renameMs: 0,
      totalTxMs: 0
    },
    txPhaseP99Ms: {
      lockWaitMs: 0,
      cloneMs: 0,
      stringifyMs: 0,
      writeMs: 0,
      renameMs: 0,
      totalTxMs: 0
    }
  }
}

function createStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'mvp-world-loop-'))
  const filePath = path.join(dir, 'memory.json')
  return createMemoryStore({ filePath })
}

test('world loop start/stop is safe and tick execution does not run concurrently', async () => {
  const memoryStore = createStore()
  const agent = { name: 'Mara', faction: 'Pilgrims' }
  let cleared = false
  const loop = createWorldLoop({
    memoryStore,
    getAgents: () => [agent],
    setIntervalFn: () => ({ id: 1 }),
    clearIntervalFn: () => {
      cleared = true
    },
    getObservabilitySnapshotFn: stableObservability
  })

  const started = loop.startWorldLoop({ tickMs: 2000 })
  assert.equal(started.running, true)

  const firstTick = loop.runTickOnce()
  const secondTick = loop.runTickOnce()
  const [firstResult, secondResult] = await Promise.all([firstTick, secondTick])

  assert.ok(firstResult.scheduled >= 0)
  assert.equal(secondResult.reason, 'tick_in_flight')

  const stopped = loop.stopWorldLoop()
  assert.equal(stopped.running, false)
  assert.equal(cleared, true)
})

test('world loop intent writes are idempotent when the same eventId is re-applied', async () => {
  const memoryStore = createStore()
  const agent = { name: 'Mara', faction: 'Pilgrims' }
  const fixedNow = 1700000000000

  const loopA = createWorldLoop({
    memoryStore,
    getAgents: () => [agent],
    now: () => fixedNow,
    getObservabilitySnapshotFn: stableObservability
  })
  const loopB = createWorldLoop({
    memoryStore,
    getAgents: () => [agent],
    now: () => fixedNow,
    getObservabilitySnapshotFn: stableObservability
  })

  await loopA.runTickOnce()
  const first = memoryStore.recallAgent('Mara').profile.world_intent
  assert.equal(first.budgets.events_in_min, 1)

  await loopB.runTickOnce()
  const second = memoryStore.recallAgent('Mara').profile.world_intent
  assert.equal(second.budgets.events_in_min, 1)
})

test('world loop runtime side effects execute only after durable commit', async () => {
  const trace = []
  const processed = new Set()
  const memory = {
    agents: {
      Mara: {
        short: [],
        long: [],
        summary: '',
        archive: [],
        recentUtterances: [],
        lastProcessedTime: 0,
        profile: {
          world_intent: {
            manual_override: true,
            intent: 'respond'
          }
        }
      }
    },
    world: {
      warActive: false,
      rules: { allowLethalPolitics: true },
      player: { name: 'Player', alive: true, legitimacy: 50 },
      factions: {},
      archive: [],
      processedEventIds: []
    }
  }

  const memoryStore = {
    getRuntimeMetrics: () => ({
      eventsProcessed: 0,
      duplicateEventsSkipped: 0,
      lockRetries: 0,
      lockTimeouts: 0,
      transactionsCommitted: 0,
      transactionsAborted: 0,
      openAiRequests: 0,
      openAiTimeouts: 0
    }),
    getSnapshot: () => JSON.parse(JSON.stringify(memory)),
    transact: async (mutator, opts = {}) => {
      if (processed.has(opts.eventId)) return { skipped: true, result: null }
      trace.push('tx_start')
      const result = await mutator(memory)
      processed.add(opts.eventId)
      trace.push('tx_commit')
      return { skipped: false, result }
    }
  }

  const loop = createWorldLoop({
    memoryStore,
    getAgents: () => [{ name: 'Mara', faction: 'Pilgrims' }],
    getObservabilitySnapshotFn: stableObservability,
    runtimeActions: {
      onRespond: () => {
        trace.push('runtime_respond')
      }
    }
  })

  await loop.runTickOnce()
  assert.deepEqual(trace, ['tx_start', 'tx_commit', 'runtime_respond'])
})

test('world loop backpressure suppresses scheduling without mutating state', async () => {
  let transactCalls = 0
  const memoryStore = {
    getRuntimeMetrics: () => ({
      eventsProcessed: 0,
      duplicateEventsSkipped: 0,
      lockRetries: 0,
      lockTimeouts: 1,
      transactionsCommitted: 0,
      transactionsAborted: 0,
      openAiRequests: 0,
      openAiTimeouts: 0
    }),
    getSnapshot: () => ({
      agents: {},
      world: {
        warActive: false,
        rules: { allowLethalPolitics: true },
        player: { name: 'Player', alive: true, legitimacy: 50 },
        factions: {},
        archive: [],
        processedEventIds: []
      }
    }),
    transact: async () => {
      transactCalls += 1
      return { skipped: false, result: null }
    }
  }

  const loop = createWorldLoop({
    memoryStore,
    getAgents: () => [{ name: 'Mara', faction: 'Pilgrims' }],
    getObservabilitySnapshotFn: stableObservability
  })

  const result = await loop.runTickOnce()
  assert.equal(result.scheduled, 0)
  assert.equal(result.backpressure, true)
  assert.equal(transactCalls, 0)
})
