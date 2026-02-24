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

test('memory store defaults additive major mission fields for known towns when missing', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({
    world: {
      markers: [
        { name: 'alpha_hall', x: 0, y: 64, z: 0, tag: 'town:alpha' }
      ],
      threat: { byTown: { beta: 12 } }
    }
  }, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()

  assert.deepEqual(snapshot.world.majorMissions, [])
  assert.equal(snapshot.world.towns.alpha.activeMajorMissionId, null)
  assert.equal(snapshot.world.towns.alpha.majorMissionCooldownUntilDay, 0)
  assert.deepEqual(snapshot.world.towns.alpha.crierQueue, [])
  assert.equal(snapshot.world.towns.beta.activeMajorMissionId, null)
  assert.equal(snapshot.world.towns.beta.majorMissionCooldownUntilDay, 0)
  assert.deepEqual(snapshot.world.towns.beta.crierQueue, [])
  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store sanitizes additive major mission + town crier shapes on load', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({
    world: {
      markers: [
        { name: 'alpha_hall', x: 0, y: 64, z: 0, tag: 'town:alpha' },
        { name: 'beta_gate', x: 10, y: 64, z: 10, tag: 'town:beta' }
      ],
      majorMissions: [
        {
          id: 'mm_alpha_1',
          townId: 'alpha',
          templateId: 'iron_convoy',
          status: 'active',
          phase: 1,
          issuedAtDay: 3,
          acceptedAtDay: 3,
          stakes: { risk: 'high', escortCount: 2 },
          progress: { advances: 0 }
        },
        {
          id: 'mm_alpha_2',
          townId: 'alpha',
          templateId: 'fog_watch',
          status: 'active',
          phase: 1,
          issuedAtDay: 3,
          acceptedAtDay: 0,
          stakes: { risk: 'moderate' },
          progress: { advances: 0 }
        },
        {
          id: '',
          townId: 'alpha',
          templateId: 'broken',
          status: 'active',
          phase: 1,
          issuedAtDay: 3,
          acceptedAtDay: 0,
          stakes: {},
          progress: {}
        }
      ],
      towns: {
        alpha: {
          activeMajorMissionId: 'mm_alpha_2',
          majorMissionCooldownUntilDay: -4,
          crierQueue: [
            { id: 'a', day: 3, type: 'mission_available', message: 'Briefing posted', missionId: 'mm_alpha_1' },
            { id: '', day: -1, type: '', message: '' }
          ]
        }
      }
    }
  }, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()

  assert.equal(snapshot.world.majorMissions.length, 2)
  assert.equal(snapshot.world.majorMissions[0].status, 'active')
  assert.equal(snapshot.world.majorMissions[1].status, 'briefed')
  assert.equal(snapshot.world.towns.alpha.activeMajorMissionId, 'mm_alpha_1')
  assert.equal(snapshot.world.towns.alpha.majorMissionCooldownUntilDay, 0)
  assert.equal(snapshot.world.towns.alpha.crierQueue.length, 1)
  assert.equal(snapshot.world.towns.alpha.crierQueue[0].id, 'a')
  assert.equal(snapshot.world.towns.beta.activeMajorMissionId, null)
  assert.deepEqual(snapshot.world.towns.beta.crierQueue, [])
  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store defaults additive nether fields when missing', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({
    world: {
      events: { seed: 2468 }
    }
  }, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()

  assert.deepEqual(snapshot.world.nether.eventLedger, [])
  assert.deepEqual(snapshot.world.nether.modifiers, {
    longNight: 0,
    omen: 0,
    scarcity: 0,
    threat: 0
  })
  assert.equal(snapshot.world.nether.deckState.seed, 2468)
  assert.equal(snapshot.world.nether.deckState.cursor, 0)
  assert.equal(snapshot.world.nether.lastTickDay, 0)
  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store sanitizes additive nether ledger + townsfolk quest bounds on load', () => {
  const filePath = createTempMemoryPath()
  const townsfolkQuests = Array.from({ length: 40 }).map((_, idx) => ({
    id: `sq_alpha_${idx}`,
    type: 'trade_n',
    state: idx % 7 === 0 ? 'accepted' : 'completed',
    origin: 'townsfolk',
    town: 'alpha',
    townId: 'alpha',
    npcKey: 'baker',
    offered_at: `2026-02-${String((idx % 20) + 1).padStart(2, '0')}T00:00:00.000Z`,
    objective: { kind: 'trade_n', n: 1 },
    progress: { done: 1 },
    reward: 1,
    title: 'SIDE: Test',
    desc: 'Bound me.'
  }))
  fs.writeFileSync(filePath, JSON.stringify({
    world: {
      events: { seed: 999 },
      quests: townsfolkQuests,
      nether: {
        eventLedger: Array.from({ length: 200 }).map((_, idx) => ({
          id: `ne_${idx}`,
          day: idx + 1,
          type: idx % 2 === 0 ? 'OMEN' : 'SCARCITY',
          payload: { threat: 1, cursor: idx },
          applied: true
        })),
        modifiers: {
          longNight: 44,
          omen: -77,
          scarcity: 2,
          threat: 1000
        },
        deckState: {
          seed: 42,
          cursor: 7
        },
        lastTickDay: -3
      }
    }
  }, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()

  const townsfolk = snapshot.world.quests.filter(quest => quest.origin === 'townsfolk' && quest.townId === 'alpha')
  assert.ok(townsfolk.length <= 24)
  assert.equal(snapshot.world.nether.eventLedger.length, 120)
  assert.equal(snapshot.world.nether.modifiers.longNight, 9)
  assert.equal(snapshot.world.nether.modifiers.omen, -9)
  assert.equal(snapshot.world.nether.modifiers.scarcity, 2)
  assert.equal(snapshot.world.nether.modifiers.threat, 9)
  assert.equal(snapshot.world.nether.deckState.seed, 42)
  assert.equal(snapshot.world.nether.deckState.cursor, 7)
  assert.equal(snapshot.world.nether.lastTickDay, 200)
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

test('memory store defaults additive moods/events fields when missing', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({ world: {} }, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()
  assert.deepEqual(snapshot.world.moods.byTown, {})
  assert.equal(snapshot.world.events.seed, 1337)
  assert.equal(snapshot.world.events.index, 0)
  assert.deepEqual(snapshot.world.events.active, [])
  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store sanitizes additive moods/events shapes on load', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({
    world: {
      moods: {
        byTown: {
          alpha: { fear: 51.9, unrest: -4, prosperity: 120.2 },
          beta: { fear: 'bad', unrest: 20.8, prosperity: Infinity },
          broken: 'invalid'
        }
      },
      events: {
        seed: 'oops',
        index: -5,
        active: [
          {
            id: 'e-valid-1',
            type: 'fog',
            town: 'alpha',
            starts_day: 2,
            ends_day: 2,
            mods: { fear: 3, visit_reward_bonus: 2, ignored: 7 }
          },
          {
            id: 'e-valid-2',
            type: 'festival',
            town: 'beta',
            starts_day: 1,
            ends_day: 1,
            mods: { prosperity: 3.9, trade_reward_bonus: 'x' }
          },
          {
            id: 'e-bad-type',
            type: 'storm',
            town: 'alpha',
            starts_day: 1,
            ends_day: 1,
            mods: { fear: 2 }
          },
          {
            id: 'e-bad-window',
            type: 'omen',
            town: 'alpha',
            starts_day: 4,
            ends_day: 3,
            mods: { fear: 2 }
          }
        ]
      }
    }
  }, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()

  assert.deepEqual(snapshot.world.moods.byTown.alpha, { fear: 51, unrest: 0, prosperity: 100 })
  assert.deepEqual(snapshot.world.moods.byTown.beta, { fear: 0, unrest: 20, prosperity: 0 })
  assert.equal(Object.prototype.hasOwnProperty.call(snapshot.world.moods.byTown, 'broken'), false)

  assert.equal(snapshot.world.events.seed, 1337)
  assert.equal(snapshot.world.events.index, 0)
  assert.equal(snapshot.world.events.active.length, 2)
  assert.equal(snapshot.world.events.active[0].id, 'e-valid-1')
  assert.equal(snapshot.world.events.active[0].mods.fear, 3)
  assert.equal(snapshot.world.events.active[0].mods.visit_reward_bonus, 2)
  assert.equal(Object.prototype.hasOwnProperty.call(snapshot.world.events.active[0].mods, 'ignored'), false)
  assert.equal(snapshot.world.events.active[1].id, 'e-valid-2')
  assert.equal(snapshot.world.events.active[1].mods.prosperity, 3)
  assert.equal(Object.prototype.hasOwnProperty.call(snapshot.world.events.active[1].mods, 'trade_reward_bonus'), false)

  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store defaults additive rumors/decisions and trait/title fields when missing', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({
    agents: {
      Mara: { profile: {} }
    },
    world: {}
  }, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()

  assert.deepEqual(snapshot.world.rumors, [])
  assert.deepEqual(snapshot.world.decisions, [])
  assert.deepEqual(snapshot.agents.Mara.profile.traits, { courage: 1, greed: 1, faith: 1 })
  assert.deepEqual(snapshot.agents.Mara.profile.titles, [])
  assert.equal(snapshot.agents.Mara.profile.rumors_completed, 0)
  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store sanitizes additive rumors and decisions shapes on load', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({
    world: {
      rumors: [
        {
          id: 'r-valid',
          town: 'alpha',
          text: 'Fog carries whispers.',
          kind: 'supernatural',
          severity: 2,
          starts_day: 3,
          expires_day: 4,
          created_at: 100
        },
        {
          id: 'r-bad-severity',
          town: 'alpha',
          text: 'Bad severity',
          kind: 'grounded',
          severity: 9,
          starts_day: 3,
          expires_day: 4,
          created_at: 101
        },
        {
          id: '',
          town: 'alpha',
          text: 'Bad id',
          kind: 'grounded',
          severity: 1,
          starts_day: 3,
          expires_day: 4,
          created_at: 102
        }
      ],
      decisions: [
        {
          id: 'd-valid',
          town: 'alpha',
          event_id: 'e1',
          event_type: 'fog',
          prompt: 'What now?',
          options: [
            { key: 'light_beacons', label: 'Light Beacons', effects: { mood: { fear: -1 } } },
            { key: 'send_scouts', label: 'Send Scouts', effects: { rumor_spawn: { kind: 'supernatural', severity: 2, templateKey: 'mist_shapes' } } },
            { key: 'send_scouts', label: 'Duplicate', effects: { mood: { unrest: 1 } } },
            { key: 'stay_inside', label: 'Stay Inside', effects: { threat_delta: -1 } }
          ],
          state: 'open',
          starts_day: 3,
          expires_day: 3,
          created_at: 200
        },
        {
          id: 'd-bad',
          town: 'alpha',
          event_id: 'e2',
          event_type: 'fog',
          prompt: 'Bad options',
          options: [
            { key: 'x', label: 'X', effects: null }
          ],
          state: 'open',
          starts_day: 3,
          expires_day: 3,
          created_at: 201
        }
      ]
    }
  }, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()

  assert.equal(snapshot.world.rumors.length, 1)
  assert.equal(snapshot.world.rumors[0].id, 'r-valid')
  assert.equal(snapshot.world.decisions.length, 1)
  assert.equal(snapshot.world.decisions[0].id, 'd-valid')
  assert.equal(snapshot.world.decisions[0].options.length, 3)
  assert.equal(store.validateMemoryIntegrity().ok, true)
})

test('memory store sanitizes additive traits/titles/rumors_completed on load', () => {
  const filePath = createTempMemoryPath()
  fs.writeFileSync(filePath, JSON.stringify({
    agents: {
      Mara: {
        profile: {
          traits: {
            courage: 7,
            greed: 1.8,
            faith: -2
          },
          titles: [
            'Pact Friend',
            '',
            'Pact Friend',
            'A very very very very very long title'
          ],
          rumors_completed: -4
        }
      },
      Eli: {
        profile: 'invalid'
      }
    },
    world: {}
  }, null, 2), 'utf-8')

  const store = createMemoryStore({ filePath })
  const snapshot = store.loadAllMemory()

  assert.deepEqual(snapshot.agents.Mara.profile.traits, { courage: 3, greed: 1, faith: 0 })
  assert.equal(snapshot.agents.Mara.profile.titles.length, 2)
  assert.equal(snapshot.agents.Mara.profile.titles[0], 'Pact Friend')
  assert.equal(snapshot.agents.Mara.profile.rumors_completed, 0)

  assert.deepEqual(snapshot.agents.Eli.profile.traits, { courage: 1, greed: 1, faith: 1 })
  assert.deepEqual(snapshot.agents.Eli.profile.titles, [])
  assert.equal(snapshot.agents.Eli.profile.rumors_completed, 0)
  assert.equal(store.validateMemoryIntegrity().ok, true)
})
