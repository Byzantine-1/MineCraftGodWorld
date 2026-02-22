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

test('memory store sanitizes additive economy ledger shape on load', () => {
  const filePath = createTempMemoryPath()
  const payload = {
    world: {
      economy: {
        currency: 'coins',
        ledger: {
          Mara: 8,
          Eli: 0,
          Ghost: -2,
          Bad: Infinity,
          Empty: NaN
        },
        minted_total: -1
      }
    }
  }
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()
  assert.equal(snapshot.world.economy.currency, 'emerald')
  assert.deepEqual(snapshot.world.economy.ledger, { Mara: 8, Eli: 0 })
  assert.equal(Object.prototype.hasOwnProperty.call(snapshot.world.economy, 'minted_total'), false)
  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store sanitizes additive markets shape on load', () => {
  const filePath = createTempMemoryPath()
  const payload = {
    world: {
      markets: [
        {
          name: 'plaza',
          marker: 'hub',
          created_at: 1,
          offers: [
            {
              offer_id: 'offer-a',
              owner: 'Mara',
              side: 'sell',
              amount: 3,
              price: 2,
              created_at: 2,
              active: true
            },
            {
              offer_id: 'offer-bad',
              owner: 'Eli',
              side: 'buy',
              amount: -1,
              price: 1,
              created_at: 3,
              active: true
            }
          ]
        },
        {
          name: '',
          offers: []
        },
        {
          name: 'broken-offers',
          offers: 'nope'
        }
      ]
    }
  }
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()

  assert.equal(Array.isArray(snapshot.world.markets), true)
  assert.equal(snapshot.world.markets.length, 2)
  assert.equal(snapshot.world.markets[0].name, 'plaza')
  assert.equal(snapshot.world.markets[0].offers.length, 1)
  assert.equal(snapshot.world.markets[0].offers[0].offer_id, 'offer-a')
  assert.equal(snapshot.world.markets[1].name, 'broken-offers')
  assert.deepEqual(snapshot.world.markets[1].offers, [])
  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store sanitizes additive chronicle and news shapes on load', () => {
  const filePath = createTempMemoryPath()
  const payload = {
    world: {
      chronicle: [
        {
          id: 'c1',
          type: 'trade',
          msg: 'good chronicle',
          at: 100,
          town: 'alpha',
          meta: { market: 'alpha_market', amount: 2 }
        },
        {
          id: '',
          type: 'bad',
          msg: 'invalid',
          at: 101
        }
      ],
      news: [
        {
          id: 'n1',
          topic: 'market',
          msg: 'good news',
          at: 200,
          meta: { ok: true }
        },
        {
          id: 'n2',
          topic: '',
          msg: 'bad news',
          at: 201
        }
      ]
    }
  }
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()
  assert.equal(snapshot.world.chronicle.length, 1)
  assert.equal(snapshot.world.news.length, 1)
  assert.equal(snapshot.world.chronicle[0].id, 'c1')
  assert.equal(snapshot.world.news[0].id, 'n1')
  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store defaults additive quests field to empty array when missing', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({ world: {} }, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()
  assert.equal(Array.isArray(snapshot.world.quests), true)
  assert.equal(snapshot.world.quests.length, 0)
  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store sanitizes additive quests shape on load', () => {
  const filePath = createTempMemoryPath()
  const payload = {
    world: {
      quests: [
        {
          id: 'q-valid-trade',
          type: 'trade_n',
          state: 'accepted',
          town: 'alpha',
          offered_at: '2026-02-22T00:00:00.000Z',
          accepted_at: '2026-02-22T00:01:00.000Z',
          owner: 'Mara',
          objective: { kind: 'trade_n', n: 2, market: 'bazaar' },
          progress: { done: 1 },
          reward: 7,
          title: 'Supply Run',
          desc: 'Buy 2 lots at bazaar.'
        },
        {
          id: 'q-valid-visit',
          type: 'visit_town',
          state: 'offered',
          offered_at: '2026-02-22T00:02:00.000Z',
          objective: { kind: 'visit_town', town: 'alpha' },
          progress: { visited: false },
          reward: 0,
          title: 'Scout the Roads',
          desc: 'Visit alpha and report.'
        },
        {
          id: 'q-bad-reward',
          type: 'trade_n',
          state: 'offered',
          offered_at: '2026-02-22T00:03:00.000Z',
          objective: { kind: 'trade_n', n: 1 },
          progress: { done: 0 },
          reward: 'ten',
          title: 'Bad',
          desc: 'Bad reward.'
        },
        {
          id: 'q-bad-type',
          type: 'slay_dragon',
          state: 'offered',
          offered_at: '2026-02-22T00:04:00.000Z',
          objective: { kind: 'slay_dragon' },
          progress: {},
          reward: 5,
          title: 'Bad',
          desc: 'Unknown type.'
        },
        {
          id: 'q-bad-shape',
          type: 'visit_town',
          state: 'offered',
          offered_at: 'not-a-date',
          objective: { kind: 'visit_town', town: 'alpha' },
          progress: { visited: false },
          reward: 5,
          title: 'Bad',
          desc: 'Bad date.'
        }
      ]
    }
  }
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()
  assert.equal(snapshot.world.quests.length, 2)
  assert.equal(snapshot.world.quests[0].id, 'q-valid-trade')
  assert.equal(snapshot.world.quests[1].id, 'q-valid-visit')
  assert.equal(snapshot.world.quests[0].reward, 7)
  assert.equal(snapshot.world.quests[1].reward, 0)
  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store defaults additive clock/threat/faction/rep fields when missing', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({
    agents: {
      Mara: {
        profile: {}
      }
    },
    world: {}
  }, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()

  assert.equal(snapshot.world.clock.day, 1)
  assert.equal(snapshot.world.clock.phase, 'day')
  assert.equal(snapshot.world.clock.season, 'dawn')
  assert.equal(Number.isFinite(Date.parse(snapshot.world.clock.updated_at)), true)
  assert.deepEqual(snapshot.world.threat.byTown, {})
  assert.equal(Array.isArray(snapshot.world.factions.iron_pact.towns), true)
  assert.equal(Array.isArray(snapshot.world.factions.veil_church.towns), true)
  assert.deepEqual(snapshot.agents.Mara.profile.rep, {})
  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store sanitizes additive clock/threat/faction/rep shapes on load', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({
    agents: {
      Mara: {
        profile: {
          rep: {
            iron_pact: 2,
            veil_church: 1.5,
            bad: 'x'
          }
        }
      },
      Eli: {
        profile: {
          rep: 'invalid'
        }
      }
    },
    world: {
      clock: {
        day: 0,
        phase: 'sunset',
        season: 'winter',
        updated_at: 'not-a-date'
      },
      threat: {
        byTown: {
          alpha: 150,
          beta: -5,
          bad: 'oops'
        }
      },
      factions: {
        iron_pact: {
          name: 'wrong',
          towns: ['alpha', 'alpha', ''],
          doctrine: 42,
          rivals: ['veil_church', 'nobody'],
          hostilityToPlayer: 12.2,
          stability: 71.9
        },
        veil_church: 'invalid-shape',
        Pilgrims: {
          hostilityToPlayer: 40,
          stability: 65
        }
      }
    }
  }, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()

  assert.equal(snapshot.world.clock.day, 1)
  assert.equal(snapshot.world.clock.phase, 'day')
  assert.equal(snapshot.world.clock.season, 'dawn')
  assert.equal(Number.isFinite(Date.parse(snapshot.world.clock.updated_at)), true)

  assert.equal(snapshot.world.threat.byTown.alpha, 100)
  assert.equal(snapshot.world.threat.byTown.beta, 0)
  assert.equal(Object.prototype.hasOwnProperty.call(snapshot.world.threat.byTown, 'bad'), false)

  assert.equal(snapshot.world.factions.iron_pact.name, 'iron_pact')
  assert.deepEqual(snapshot.world.factions.iron_pact.towns, ['alpha'])
  assert.equal(snapshot.world.factions.iron_pact.doctrine, 'Order through steel.')
  assert.deepEqual(snapshot.world.factions.iron_pact.rivals, ['veil_church'])
  assert.equal(snapshot.world.factions.veil_church.name, 'veil_church')
  assert.equal(snapshot.world.factions.Pilgrims.hostilityToPlayer, 40)

  assert.deepEqual(snapshot.agents.Mara.profile.rep, { iron_pact: 2 })
  assert.deepEqual(snapshot.agents.Eli.profile.rep, {})
  assert.equal(store.validateMemoryIntegrity().ok, true)
})
