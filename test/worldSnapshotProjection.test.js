const test = require('node:test')
const assert = require('node:assert/strict')

const { freshMemoryShape } = require('../src/memory')
const { createAuthoritativeSnapshotProjection } = require('../src/worldSnapshotProjection')

function createRawWorldFixture() {
  return {
    warActive: true,
    player: { name: 'Player', alive: true, legitimacy: 61 },
    factions: {
      veil_church: {
        name: 'veil_church',
        hostilityToPlayer: 12,
        stability: 74,
        towns: ['beta'],
        doctrine: 'Truth through shadow.',
        rivals: ['iron_pact']
      },
      iron_pact: {
        name: 'iron_pact',
        hostilityToPlayer: 18,
        stability: 69,
        towns: ['alpha'],
        doctrine: 'Order through steel.',
        rivals: ['veil_church']
      }
    },
    clock: {
      day: 4,
      phase: 'night',
      season: 'long_night',
      updated_at: '2026-02-22T00:00:00.000Z'
    },
    threat: {
      byTown: {
        beta: 62,
        alpha: 31
      }
    },
    moods: {
      byTown: {
        beta: { fear: 40, unrest: 21, prosperity: 17 },
        alpha: { fear: 11, unrest: 13, prosperity: 28 }
      }
    },
    events: {
      seed: 1337,
      index: 2,
      active: [
        { id: 'event-b', type: 'omen', town: 'beta', starts_day: 4, ends_day: 5, mods: { fear: 2 } },
        { id: 'event-a', type: 'festival', town: 'alpha', starts_day: 4, ends_day: 4, mods: { prosperity: 1 } }
      ]
    },
    rumors: [
      {
        id: 'rumor-b',
        town: 'beta',
        text: 'Shadow bells again.',
        kind: 'supernatural',
        severity: 2,
        starts_day: 4,
        expires_day: 6,
        created_at: 20
      },
      {
        id: 'rumor-a',
        town: 'alpha',
        text: 'Supplies are late.',
        kind: 'grounded',
        severity: 1,
        starts_day: 4,
        expires_day: 5,
        created_at: 19
      }
    ],
    decisions: [
      {
        id: 'decision-b',
        town: 'beta',
        event_id: 'event-b',
        event_type: 'omen',
        prompt: 'How should beta respond?',
        options: [
          {
            key: 'pray',
            label: 'Pray',
            effects: {
              mood: { fear: -1, unrest: 0, prosperity: 0 },
              rep_delta: { veil_church: 1 }
            }
          },
          {
            key: 'patrol',
            label: 'Patrol',
            effects: {
              mood: { fear: -1, unrest: -1, prosperity: 0 },
              threat_delta: -1
            }
          }
        ],
        state: 'open',
        starts_day: 4,
        expires_day: 5,
        created_at: 22
      }
    ],
    markers: [
      { name: 'beta_hall', x: 10, y: 64, z: 10, tag: 'town:beta', created_at: 2 },
      { name: 'alpha_hall', x: 0, y: 64, z: 0, tag: 'town:alpha', created_at: 1 }
    ],
    markets: [
      {
        name: 'beta_bazaar',
        marker: 'beta_hall',
        created_at: 3,
        offers: [
          { offer_id: 'offer-b', owner: 'Eli', side: 'buy', amount: 2, price: 5, created_at: 5, active: true },
          { offer_id: 'offer-a', owner: 'Mara', side: 'sell', amount: 3, price: 4, created_at: 4, active: true }
        ]
      }
    ],
    economy: {
      currency: 'emerald',
      ledger: { Eli: 5, Mara: 7 },
      minted_total: 12
    },
    quests: [
      {
        id: 'quest-b',
        type: 'visit_town',
        state: 'offered',
        offered_at: '2026-02-22T00:10:00.000Z',
        objective: { kind: 'visit_town', town: 'beta' },
        progress: { visited: false },
        reward: 2,
        title: 'Visit Beta',
        desc: 'Head to beta.'
      },
      {
        id: 'quest-a',
        type: 'trade_n',
        state: 'accepted',
        offered_at: '2026-02-22T00:09:00.000Z',
        accepted_at: '2026-02-22T00:11:00.000Z',
        objective: { kind: 'trade_n', n: 1, market: 'beta_bazaar' },
        progress: { done: 0 },
        reward: 4,
        title: 'Trade Once',
        desc: 'Make one trade.'
      }
    ],
    majorMissions: [
      {
        id: 'mission-b',
        townId: 'beta',
        templateId: 'veil_watch',
        status: 'active',
        phase: 1,
        issuedAtDay: 4,
        acceptedAtDay: 4,
        stakes: { dread: 2 },
        progress: { patrols: 1 }
      }
    ],
    projects: [
      {
        id: 'project-b',
        townId: 'beta',
        type: 'watchtower_line',
        status: 'active',
        stage: 2,
        requirements: { timber: 3 },
        effects: { hope: 1 },
        startedAtDay: 3,
        updatedAtDay: 4
      },
      {
        id: 'project-a',
        townId: 'alpha',
        type: 'lantern_line',
        status: 'planned',
        stage: 0,
        requirements: { coal: 1 },
        effects: { dread: -1 },
        startedAtDay: 4,
        updatedAtDay: 4
      }
    ],
    salvageRuns: [
      {
        id: 'salvage-b',
        townId: 'beta',
        targetKey: 'abandoned_shrine_relics',
        status: 'planned',
        plannedAtDay: 4,
        resolvedAtDay: 0,
        result: { risk: 2 }
      },
      {
        id: 'salvage-a',
        townId: 'alpha',
        targetKey: 'ruined_hamlet_supplies',
        status: 'resolved',
        plannedAtDay: 3,
        resolvedAtDay: 4,
        result: { food: 3 },
        outcomeKey: 'supplies_found'
      }
    ],
    towns: {
      beta: {
        activeMajorMissionId: 'mission-b',
        majorMissionCooldownUntilDay: 0,
        hope: 43,
        dread: 61,
        crierQueue: [
          { id: 'crier-b', day: 4, type: 'warning', message: 'Keep lamps lit.' },
          { id: 'crier-a', day: 4, type: 'market', message: 'Bazaar opens late.' }
        ],
        recentImpacts: [
          { id: 'impact-b', day: 4, type: 'nether_event', summary: 'A bad omen spreads.', netherEventId: 'nether-b' },
          { id: 'impact-a', day: 4, type: 'project_start', summary: 'Watchtowers rise.', projectId: 'project-b' }
        ]
      },
      alpha: {
        activeMajorMissionId: null,
        majorMissionCooldownUntilDay: 0,
        hope: 55,
        dread: 35,
        crierQueue: [],
        recentImpacts: []
      }
    },
    nether: {
      eventLedger: [
        { id: 'nether-b', day: 4, type: 'OMEN', payload: { beta: 1 }, applied: true },
        { id: 'nether-a', day: 3, type: 'SCARCITY', payload: { alpha: 2 }, applied: true }
      ],
      modifiers: { longNight: 2, omen: 1, scarcity: 3, threat: 1 },
      deckState: { seed: 1337, cursor: 7 },
      lastTickDay: 4
    }
  }
}

function buildWorld(rawWorld) {
  return freshMemoryShape({ world: rawWorld }).world
}

test('authoritative snapshot projection hash is stable for equivalent world state', () => {
  const left = buildWorld(createRawWorldFixture())
  const rightFixture = createRawWorldFixture()
  rightFixture.events.active.reverse()
  rightFixture.rumors.reverse()
  rightFixture.decisions[0].options.reverse()
  rightFixture.markers.reverse()
  rightFixture.markets[0].offers.reverse()
  rightFixture.quests.reverse()
  rightFixture.projects.reverse()
  rightFixture.salvageRuns.reverse()
  rightFixture.towns.beta.crierQueue.reverse()
  rightFixture.towns.beta.recentImpacts.reverse()
  rightFixture.nether.eventLedger.reverse()
  const right = buildWorld(rightFixture)

  const leftProjection = createAuthoritativeSnapshotProjection(left)
  const rightProjection = createAuthoritativeSnapshotProjection(right)

  assert.deepEqual(rightProjection.snapshot, leftProjection.snapshot)
  assert.equal(rightProjection.snapshotHash, leftProjection.snapshotHash)
})

test('authoritative snapshot projection excludes incidental logs and execution receipts', () => {
  const left = buildWorld(createRawWorldFixture())
  const right = buildWorld(createRawWorldFixture())

  left.chronicle.push({ id: 'chronicle-a', type: 'note', msg: 'Ignore me', at: 1 })
  left.news.push({ id: 'news-a', topic: 'ops', msg: 'Ignore me too', at: 2 })
  left.archive.push({ time: 3, event: 'archive noise' })
  left.processedEventIds.push('event:1')
  left.execution.history.push({
    type: 'execution-result.v1',
    schemaVersion: 1,
    executionId: 'result_a'.padEnd(71, 'a'),
    resultId: 'result_a'.padEnd(71, 'a'),
    handoffId: 'handoff_b'.padEnd(72, 'b'),
    proposalId: 'proposal_c'.padEnd(73, 'c'),
    idempotencyKey: 'proposal_c'.padEnd(73, 'c'),
    actorId: 'mara',
    townId: 'alpha',
    proposalType: 'PROJECT_ADVANCE',
    command: 'project advance alpha p-1',
    authorityCommands: ['project advance alpha p-1'],
    status: 'executed',
    accepted: true,
    executed: true,
    reasonCode: 'EXECUTED',
    actualSnapshotHash: 'd'.repeat(64),
    actualDecisionEpoch: 4,
    postExecutionSnapshotHash: 'e'.repeat(64),
    postExecutionDecisionEpoch: 4
  })

  const leftProjection = createAuthoritativeSnapshotProjection(left)
  const rightProjection = createAuthoritativeSnapshotProjection(right)

  assert.equal(leftProjection.snapshotHash, rightProjection.snapshotHash)
  assert.deepEqual(leftProjection.snapshot, rightProjection.snapshot)
})
