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

function emptyAgentRecord(profile = {}) {
  return {
    short: [],
    long: [],
    summary: '',
    archive: [],
    recentUtterances: [],
    lastProcessedTime: 0,
    profile
  }
}

/**
 * @param {ReturnType<typeof createMemoryStore>} memoryStore
 */
function snapshotHash(memoryStore) {
  return JSON.stringify(memoryStore.getSnapshot())
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

test('job-driven intents are selected when no overrides apply', async () => {
  const memoryStore = createStore()
  await memoryStore.transact((memory) => {
    memory.agents.Mara = emptyAgentRecord({
      job: { role: 'scout', assigned_at: '2026-02-21T00:00:00.000Z' }
    })
  }, { eventId: 'seed-job-scout' })

  let nowMs = 1700000000000
  const loop = createWorldLoop({
    memoryStore,
    getAgents: () => [{ name: 'Mara', faction: 'Pilgrims' }],
    now: () => {
      nowMs += 10
      return nowMs
    },
    getObservabilitySnapshotFn: stableObservability
  })

  await loop.runTickOnce()
  const worldIntent = memoryStore.recallAgent('Mara').profile.world_intent
  assert.equal(worldIntent.intent, 'wander')
})

test('respond priority beats job intent when chat is pending', async () => {
  const memoryStore = createStore()
  await memoryStore.transact((memory) => {
    memory.agents.Mara = emptyAgentRecord({
      job: { role: 'scout', assigned_at: '2026-02-21T00:00:00.000Z' }
    })
  }, { eventId: 'seed-job-chat-priority' })

  const runtimeAgent = {
    name: 'Mara',
    faction: 'Pilgrims',
    pendingPlayerMessage: 'hello there'
  }

  let nowMs = 1700001000000
  const loop = createWorldLoop({
    memoryStore,
    getAgents: () => [runtimeAgent],
    now: () => {
      nowMs += 10
      return nowMs
    },
    getObservabilitySnapshotFn: stableObservability
  })

  await loop.runTickOnce()
  const worldIntent = memoryStore.recallAgent('Mara').profile.world_intent
  assert.equal(worldIntent.intent, 'respond')
})

test('intent repetition breaker triggers and resets', async () => {
  const memoryStore = createStore()
  await memoryStore.transact((memory) => {
    memory.world.markers = [{ name: 'home', x: 10, y: 64, z: 10, tag: 'base', created_at: 1 }]
    memory.agents.Mara = emptyAgentRecord({
      job: {
        role: 'builder',
        assigned_at: '2026-02-21T00:00:00.000Z',
        home_marker: 'home'
      }
    })
  }, { eventId: 'seed-repetition-breaker' })

  let nowMs = 1700002000000
  const loop = createWorldLoop({
    memoryStore,
    getAgents: () => [{ name: 'Mara', faction: 'Pilgrims' }],
    now: () => {
      nowMs += 10
      return nowMs
    },
    maxEventsPerAgentPerMin: 100,
    getObservabilitySnapshotFn: stableObservability
  })

  for (let i = 0; i < 9; i += 1) {
    await loop.runTickOnce()
    const intent = memoryStore.recallAgent('Mara').profile.world_intent
    assert.equal(intent.intent, 'follow')
    assert.equal(intent.intent_target, 'home')
  }

  await loop.runTickOnce()
  const fallbackIntent = memoryStore.recallAgent('Mara').profile.world_intent
  assert.equal(fallbackIntent.intent, 'wander')

  const afterBreakState = loop.getAgentRuntimeState('Mara')
  assert.equal(afterBreakState.repetitionCount, 0)

  await loop.runTickOnce()
  const resumedIntent = memoryStore.recallAgent('Mara').profile.world_intent
  assert.equal(resumedIntent.intent, 'follow')
  assert.equal(resumedIntent.intent_target, 'home')

  const resumedState = loop.getAgentRuntimeState('Mara')
  assert.equal(resumedState.repetitionCount, 1)
})

test('town crier defaults are off and runtime does not broadcast when disabled', async () => {
  const memoryStore = createStore()
  await memoryStore.transact((memory) => {
    memory.world.news = [
      { id: 'news-default-off-1', topic: 'trade', msg: 'A trade happened.', at: 1, town: 'alpha' }
    ]
  }, { eventId: 'seed-town-crier-default-off' })

  const broadcasts = []
  let nowMs = 1700003000000
  const loop = createWorldLoop({
    memoryStore,
    getAgents: () => [],
    now: () => {
      nowMs += 1000
      return nowMs
    },
    setIntervalFn: () => ({ id: 1 }),
    clearIntervalFn: () => {},
    env: {},
    runtimeActions: {
      onNews: (payload) => broadcasts.push(payload)
    },
    getObservabilitySnapshotFn: stableObservability
  })

  const started = loop.startWorldLoop({ tickMs: 100 })
  assert.equal(started.townCrierEnabled, false)
  assert.equal(started.townCrierIntervalMs, 15000)
  assert.equal(started.townCrierMaxPerTick, 1)
  assert.equal(started.townCrierRecentWindow, 25)
  assert.equal(started.townCrierDedupeWindow, 100)

  await loop.runTickOnce()
  await loop.runTickOnce()

  assert.equal(broadcasts.length, 0)
  loop.stopWorldLoop()
})

test('town crier broadcasts when enabled and remains read-only', async () => {
  const memoryStore = createStore()
  await memoryStore.transact((memory) => {
    memory.world.news = [
      { id: 'news-read-only-1', topic: 'market', msg: 'Market opened in alpha.', at: 1, town: 'alpha' },
      { id: 'news-read-only-2', topic: 'trade', msg: 'Mara bought 2 @ 3 from Eli.', at: 2, town: 'alpha' }
    ]
  }, { eventId: 'seed-town-crier-read-only' })

  const beforeHash = snapshotHash(memoryStore)
  const broadcastLines = []
  let nowMs = 1700003100000
  const loop = createWorldLoop({
    memoryStore,
    getAgents: () => [],
    now: () => {
      nowMs += 20
      return nowMs
    },
    setIntervalFn: () => ({ id: 1 }),
    clearIntervalFn: () => {},
    env: {
      TOWN_CRIER_ENABLED: '1',
      TOWN_CRIER_INTERVAL_MS: '10',
      TOWN_CRIER_MAX_PER_TICK: '1',
      TOWN_CRIER_RECENT_WINDOW: '25',
      TOWN_CRIER_DEDUPE_WINDOW: '100'
    },
    runtimeActions: {
      onNews: ({ line }) => broadcastLines.push(line)
    },
    getObservabilitySnapshotFn: stableObservability
  })

  const started = loop.startWorldLoop({ tickMs: 50 })
  assert.equal(started.townCrierEnabled, true)
  assert.equal(started.townCrierIntervalMs, 10)
  assert.equal(started.townCrierMaxPerTick, 1)
  assert.equal(started.townCrierRecentWindow, 25)
  assert.equal(started.townCrierDedupeWindow, 100)

  await loop.runTickOnce()
  assert.ok(broadcastLines.length >= 1)
  assert.equal(snapshotHash(memoryStore), beforeHash)

  await loop.runTickOnce()
  await loop.runTickOnce()
  assert.equal(snapshotHash(memoryStore), beforeHash)
  loop.stopWorldLoop()
})

test('town crier dedupe prevents broadcasting the same news id twice during a run', async () => {
  const memoryStore = createStore()
  await memoryStore.transact((memory) => {
    memory.world.news = [
      { id: 'news-dedupe-1', topic: 'trade', msg: 'Dedupe me once.', at: 1, town: 'alpha' }
    ]
  }, { eventId: 'seed-town-crier-dedupe' })

  const broadcastIds = []
  let nowMs = 1700003200000
  const loop = createWorldLoop({
    memoryStore,
    getAgents: () => [],
    now: () => {
      nowMs += 10
      return nowMs
    },
    setIntervalFn: () => ({ id: 1 }),
    clearIntervalFn: () => {},
    env: {
      TOWN_CRIER_ENABLED: '1',
      TOWN_CRIER_INTERVAL_MS: '1',
      TOWN_CRIER_MAX_PER_TICK: '1',
      TOWN_CRIER_RECENT_WINDOW: '25',
      TOWN_CRIER_DEDUPE_WINDOW: '100'
    },
    runtimeActions: {
      onNews: ({ id }) => broadcastIds.push(id)
    },
    getObservabilitySnapshotFn: stableObservability
  })

  loop.startWorldLoop({ tickMs: 50 })
  for (let i = 0; i < 5; i += 1) {
    await loop.runTickOnce()
  }

  assert.equal(broadcastIds.length, 1)
  assert.equal(new Set(broadcastIds).size, broadcastIds.length)
  loop.stopWorldLoop()
})

test('town crier has no backlog queue and honors max-per-tick', async () => {
  const memoryStore = createStore()
  await memoryStore.transact((memory) => {
    memory.world.news = []
    for (let i = 0; i < 30; i += 1) {
      memory.world.news.push({
        id: `news-backpressure-${i}`,
        topic: 'trade',
        msg: `news-${i}`,
        at: i + 1
      })
    }
  }, { eventId: 'seed-town-crier-backpressure' })

  let broadcasts = 0
  let nowMs = 1700003300000
  const loop = createWorldLoop({
    memoryStore,
    getAgents: () => [],
    now: () => {
      nowMs += 5
      return nowMs
    },
    setIntervalFn: () => ({ id: 1 }),
    clearIntervalFn: () => {},
    env: {
      TOWN_CRIER_ENABLED: '1',
      TOWN_CRIER_INTERVAL_MS: '1',
      TOWN_CRIER_MAX_PER_TICK: '1',
      TOWN_CRIER_RECENT_WINDOW: '30',
      TOWN_CRIER_DEDUPE_WINDOW: '5'
    },
    runtimeActions: {
      onNews: () => {
        broadcasts += 1
      }
    },
    getObservabilitySnapshotFn: stableObservability
  })

  loop.startWorldLoop({ tickMs: 50 })
  const ticks = 12
  for (let i = 0; i < ticks; i += 1) {
    await loop.runTickOnce()
  }

  const status = loop.getWorldLoopStatus()
  assert.ok(broadcasts <= ticks)
  assert.ok(status.townCrierDedupeSize <= status.townCrierDedupeWindow)
  loop.stopWorldLoop()
})

test('town crier dedupe resets after stop/start restart', async () => {
  const memoryStore = createStore()
  await memoryStore.transact((memory) => {
    memory.world.news = [
      { id: 'news-restart-1', topic: 'trade', msg: 'Restart-sensitive news.', at: 1, town: 'alpha' }
    ]
  }, { eventId: 'seed-town-crier-restart' })

  let nowMs = 1700003400000
  const broadcastIds = []
  const loop = createWorldLoop({
    memoryStore,
    getAgents: () => [],
    now: () => {
      nowMs += 50
      return nowMs
    },
    setIntervalFn: () => ({ id: 1 }),
    clearIntervalFn: () => {},
    env: {
      TOWN_CRIER_ENABLED: '1',
      TOWN_CRIER_INTERVAL_MS: '1',
      TOWN_CRIER_MAX_PER_TICK: '1'
    },
    runtimeActions: {
      onNews: ({ id }) => broadcastIds.push(id)
    },
    getObservabilitySnapshotFn: stableObservability
  })

  loop.startWorldLoop({ tickMs: 50 })
  await loop.runTickOnce()
  loop.stopWorldLoop()

  loop.startWorldLoop({ tickMs: 50 })
  await loop.runTickOnce()
  loop.stopWorldLoop()

  assert.deepEqual(broadcastIds, ['news-restart-1', 'news-restart-1'])
})

test('town crier side effects execute only after transaction commit', async () => {
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
      news: [{ id: 'news-ordering-1', topic: 'trade', msg: 'Ordering test.', at: 1, town: 'alpha' }],
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

  let nowMs = 1700003500000
  const loop = createWorldLoop({
    memoryStore,
    getAgents: () => [{ name: 'Mara', faction: 'Pilgrims' }],
    now: () => {
      nowMs += 20
      return nowMs
    },
    setIntervalFn: () => ({ id: 1 }),
    clearIntervalFn: () => {},
    env: {
      TOWN_CRIER_ENABLED: '1',
      TOWN_CRIER_INTERVAL_MS: '1',
      TOWN_CRIER_MAX_PER_TICK: '1'
    },
    getObservabilitySnapshotFn: stableObservability,
    runtimeActions: {
      onRespond: () => {
        trace.push('runtime_respond')
      },
      onNews: () => {
        trace.push('runtime_news')
      }
    }
  })

  loop.startWorldLoop({ tickMs: 100 })
  await loop.runTickOnce()
  loop.stopWorldLoop()
  assert.deepEqual(trace, ['tx_start', 'tx_commit', 'runtime_respond', 'runtime_news'])
})
