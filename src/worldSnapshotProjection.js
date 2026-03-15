const { createHash } = require('crypto')
const { defaultActorName, defaultTownNameFromId } = require('./worldRegistry')
const { normalizePlayerAssignment, normalizeTownSpawn } = require('./playerSpawn')

const WORLD_SNAPSHOT_TYPE = 'world-snapshot.v1'
const WORLD_SNAPSHOT_SCHEMA_VERSION = 1
const TOWN_STOCKPILE_KEYS = ['food', 'tools', 'munitions', 'timber', 'stone', 'lampOil', 'sanctity']
const TOWN_READINESS_KEYS = ['defense', 'economy', 'morale', 'gate', 'shelter']
const TOWN_ECONOMY_KEYS = ['market', 'labor', 'build', 'caravan', 'wealth']
const TOWN_ARMORY_KEYS = ['reserve', 'issued', 'repair', 'distribution']
const DEFAULT_TOWN_GATE = Object.freeze({
  pressure: 18,
  status: 'quiet',
  criticalEvent: 'none',
  travelRisk: 'low',
  lastEventDay: 0,
  lastEventId: null
})
const DEFAULT_TOWN_STOCKPILES = Object.freeze({
  food: 55,
  tools: 48,
  munitions: 44,
  timber: 46,
  stone: 44,
  lampOil: 42,
  sanctity: 50
})
const DEFAULT_TOWN_READINESS = Object.freeze({
  defense: 46,
  economy: 48,
  morale: 50,
  gate: 45,
  shelter: 47
})
const DEFAULT_TOWN_ECONOMY = Object.freeze({
  market: 48,
  labor: 50,
  build: 46,
  caravan: 44,
  wealth: 47
})
const DEFAULT_TOWN_ARMORY = Object.freeze({
  reserve: 46,
  issued: 24,
  repair: 42,
  distribution: 34
})

function isPlainObject(value) {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value))
}

function asText(value, fallback = '') {
  if (typeof value !== 'string') return fallback
  const trimmed = value.trim()
  return trimmed || fallback
}

function asInteger(value, fallback = 0) {
  const parsed = Number(value)
  return Number.isInteger(parsed) ? parsed : fallback
}

function stableStringify(value) {
  if (Array.isArray(value)) {
    return `[${value.map((entry) => stableStringify(entry)).join(',')}]`
  }

  if (isPlainObject(value)) {
    const keys = Object.keys(value)
      .filter((key) => value[key] !== undefined)
      .sort()
    return `{${keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(',')}}`
  }

  return JSON.stringify(value)
}

function hashStableValue(value) {
  return createHash('sha256').update(stableStringify(value)).digest('hex')
}

function sortRecord(source, mapValue) {
  if (!isPlainObject(source)) return {}
  const out = {}
  for (const key of Object.keys(source).sort()) {
    out[key] = mapValue(source[key], key)
  }
  return out
}

function sortObjects(entries, normalizeEntry, keyFn) {
  return (Array.isArray(entries) ? entries : [])
    .map((entry) => normalizeEntry(entry))
    .filter(Boolean)
    .sort((left, right) => keyFn(left).localeCompare(keyFn(right)))
}

function sortStrings(entries) {
  return (Array.isArray(entries) ? entries : [])
    .map((entry) => asText(entry))
    .filter(Boolean)
    .sort((left, right) => left.localeCompare(right))
}

function normalizeScalarRecord(source) {
  return sortRecord(source, (value) => {
    if (value === null) return null
    if (typeof value === 'boolean') return value
    if (typeof value === 'number') return Number.isFinite(value) ? Math.trunc(value) : null
    if (typeof value === 'string') return asText(value) || null
    return null
  })
}

function normalizeTownStockpiles(entry) {
  const source = isPlainObject(entry) ? entry : {}
  const stockpiles = {}
  for (const key of TOWN_STOCKPILE_KEYS) {
    stockpiles[key] = asInteger(source[key], DEFAULT_TOWN_STOCKPILES[key])
  }
  return stockpiles
}

function normalizeTownReadiness(entry) {
  const source = isPlainObject(entry) ? entry : {}
  const readiness = {}
  for (const key of TOWN_READINESS_KEYS) {
    readiness[key] = asInteger(source[key], DEFAULT_TOWN_READINESS[key])
  }
  return readiness
}

function normalizeTownAutonomy(entry) {
  const source = isPlainObject(entry) ? entry : {}
  const mode = asText(source.mode, 'allied_autonomy')
  return {
    mode: mode === 'home_priority' ? 'home_priority' : 'allied_autonomy',
    lastPlannedDay: asInteger(source.lastPlannedDay),
    lastResolvedDay: asInteger(source.lastResolvedDay)
  }
}

function normalizeTownEconomy(entry) {
  const source = isPlainObject(entry) ? entry : {}
  const economy = {}
  for (const key of TOWN_ECONOMY_KEYS) {
    economy[key] = asInteger(source[key], DEFAULT_TOWN_ECONOMY[key])
  }
  return economy
}

function normalizeTownArmory(entry) {
  const source = isPlainObject(entry) ? entry : {}
  const armory = {}
  for (const key of TOWN_ARMORY_KEYS) {
    armory[key] = asInteger(source[key], DEFAULT_TOWN_ARMORY[key])
  }
  return armory
}

function normalizeTownGate(entry) {
  const source = isPlainObject(entry) ? entry : {}
  return {
    pressure: asInteger(source.pressure, DEFAULT_TOWN_GATE.pressure),
    status: asText(source.status, DEFAULT_TOWN_GATE.status),
    criticalEvent: asText(source.criticalEvent, DEFAULT_TOWN_GATE.criticalEvent),
    travelRisk: asText(source.travelRisk, DEFAULT_TOWN_GATE.travelRisk),
    lastEventDay: asInteger(source.lastEventDay, DEFAULT_TOWN_GATE.lastEventDay),
    lastEventId: asText(source.lastEventId) || null
  }
}

function normalizeTownBuildQueueEntry(entry) {
  const id = asText(entry?.id)
  const projectType = asText(entry?.projectType)
  if (!id || !projectType) return null
  return {
    id,
    projectType,
    priority: asText(entry?.priority),
    queuedAtDay: asInteger(entry?.queuedAtDay),
    reason: asText(entry?.reason),
    autoManaged: entry?.autoManaged === true
  }
}

function normalizeFaction(entry) {
  return {
    name: asText(entry?.name),
    hostilityToPlayer: asInteger(entry?.hostilityToPlayer),
    stability: asInteger(entry?.stability),
    towns: sortStrings(entry?.towns),
    doctrine: asText(entry?.doctrine),
    rivals: sortStrings(entry?.rivals)
  }
}

function normalizeMood(entry) {
  return {
    fear: asInteger(entry?.fear),
    unrest: asInteger(entry?.unrest),
    prosperity: asInteger(entry?.prosperity)
  }
}

function normalizeWorldEvent(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    type: asText(entry?.type),
    town: asText(entry?.town),
    starts_day: asInteger(entry?.starts_day),
    ends_day: asInteger(entry?.ends_day),
    mods: normalizeScalarRecord(entry?.mods)
  }
}

function normalizeRumor(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    town: asText(entry?.town),
    text: asText(entry?.text),
    kind: asText(entry?.kind),
    severity: asInteger(entry?.severity),
    starts_day: asInteger(entry?.starts_day),
    expires_day: asInteger(entry?.expires_day),
    created_at: asInteger(entry?.created_at),
    spawned_by_event_id: asText(entry?.spawned_by_event_id) || null,
    resolved_by_quest_id: asText(entry?.resolved_by_quest_id) || null
  }
}

function normalizeDecisionOption(entry) {
  return {
    key: asText(entry?.key),
    label: asText(entry?.label),
    effects: {
      mood: {
        fear: asInteger(entry?.effects?.mood?.fear),
        unrest: asInteger(entry?.effects?.mood?.unrest),
        prosperity: asInteger(entry?.effects?.mood?.prosperity)
      },
      threat_delta: asInteger(entry?.effects?.threat_delta),
      rep_delta: sortRecord(entry?.effects?.rep_delta, (value) => asInteger(value)),
      rumor_spawn: {
        kind: asText(entry?.effects?.rumor_spawn?.kind) || null,
        severity: asInteger(entry?.effects?.rumor_spawn?.severity),
        templateKey: asText(entry?.effects?.rumor_spawn?.templateKey) || null,
        expiresInDays: asInteger(entry?.effects?.rumor_spawn?.expiresInDays)
      }
    }
  }
}

function normalizeDecision(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    town: asText(entry?.town),
    event_id: asText(entry?.event_id),
    event_type: asText(entry?.event_type),
    prompt: asText(entry?.prompt),
    options: sortObjects(entry?.options, normalizeDecisionOption, (option) => option.key),
    state: asText(entry?.state),
    chosen_key: asText(entry?.chosen_key) || null,
    starts_day: asInteger(entry?.starts_day),
    expires_day: asInteger(entry?.expires_day),
    created_at: asInteger(entry?.created_at)
  }
}

function normalizeMarker(entry) {
  const name = asText(entry?.name)
  if (!name) return null
  return {
    name,
    x: asInteger(entry?.x),
    y: asInteger(entry?.y),
    z: asInteger(entry?.z),
    tag: asText(entry?.tag),
    created_at: asInteger(entry?.created_at)
  }
}

function normalizeOffer(entry) {
  const offerId = asText(entry?.offer_id)
  if (!offerId) return null
  return {
    offer_id: offerId,
    owner: asText(entry?.owner),
    side: asText(entry?.side),
    amount: asInteger(entry?.amount),
    price: asInteger(entry?.price),
    created_at: asInteger(entry?.created_at),
    active: entry?.active !== false
  }
}

function normalizeMarket(entry) {
  const name = asText(entry?.name)
  if (!name) return null
  return {
    name,
    marker: asText(entry?.marker) || null,
    created_at: asInteger(entry?.created_at),
    offers: sortObjects(entry?.offers, normalizeOffer, (offer) => offer.offer_id)
  }
}

function normalizeQuestObjective(entry) {
  if (!isPlainObject(entry)) return {}
  return sortRecord(entry, (value) => {
    if (typeof value === 'boolean') return value
    if (typeof value === 'number') return Number.isFinite(value) ? Math.trunc(value) : null
    if (typeof value === 'string') return asText(value) || null
    return null
  })
}

function normalizeQuest(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    type: asText(entry?.type),
    state: asText(entry?.state),
    origin: asText(entry?.origin) || null,
    town: asText(entry?.town) || null,
    townId: asText(entry?.townId) || null,
    npcKey: asText(entry?.npcKey) || null,
    supportsMajorMissionId: asText(entry?.supportsMajorMissionId) || null,
    offered_at: asText(entry?.offered_at),
    accepted_at: asText(entry?.accepted_at) || null,
    owner: asText(entry?.owner) || null,
    objective: normalizeQuestObjective(entry?.objective),
    progress: normalizeQuestObjective(entry?.progress),
    reward: asInteger(entry?.reward),
    title: asText(entry?.title),
    desc: asText(entry?.desc),
    meta: normalizeScalarRecord(entry?.meta)
  }
}

function normalizeMajorMission(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    townId: asText(entry?.townId),
    templateId: asText(entry?.templateId),
    status: asText(entry?.status),
    phase: typeof entry?.phase === 'number' ? Math.trunc(entry.phase) : asText(entry?.phase),
    issuedAtDay: asInteger(entry?.issuedAtDay),
    acceptedAtDay: asInteger(entry?.acceptedAtDay),
    stakes: normalizeScalarRecord(entry?.stakes),
    progress: normalizeScalarRecord(entry?.progress)
  }
}

function normalizeProject(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    townId: asText(entry?.townId),
    type: asText(entry?.type),
    status: asText(entry?.status),
    stage: asInteger(entry?.stage),
    requirements: normalizeScalarRecord(entry?.requirements),
    effects: normalizeScalarRecord(entry?.effects),
    startedAtDay: asInteger(entry?.startedAtDay),
    updatedAtDay: asInteger(entry?.updatedAtDay),
    supportsMajorMissionId: asText(entry?.supportsMajorMissionId) || null
  }
}

function normalizeSalvageRun(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    townId: asText(entry?.townId),
    targetKey: asText(entry?.targetKey),
    status: asText(entry?.status),
    plannedAtDay: asInteger(entry?.plannedAtDay),
    resolvedAtDay: asInteger(entry?.resolvedAtDay),
    result: normalizeScalarRecord(entry?.result),
    outcomeKey: asText(entry?.outcomeKey) || null,
    supportsMajorMissionId: asText(entry?.supportsMajorMissionId) || null,
    supportsProjectId: asText(entry?.supportsProjectId) || null
  }
}

function normalizeTownCrierEntry(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    day: asInteger(entry?.day),
    type: asText(entry?.type),
    message: asText(entry?.message),
    missionId: asText(entry?.missionId) || null
  }
}

function normalizeTownImpact(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    day: asInteger(entry?.day),
    type: asText(entry?.type),
    summary: asText(entry?.summary),
    missionId: asText(entry?.missionId) || null,
    questId: asText(entry?.questId) || null,
    netherEventId: asText(entry?.netherEventId) || null,
    projectId: asText(entry?.projectId) || null,
    salvageRunId: asText(entry?.salvageRunId) || null
  }
}

function normalizeTown(entry) {
  const townId = asText(entry?.townId)
  const spawn = normalizeTownSpawn(entry?.spawn)
  return {
    townId,
    name: asText(entry?.name, defaultTownNameFromId(townId)),
    status: asText(entry?.status, 'active'),
    region: asText(entry?.region) || null,
    tags: sortStrings(entry?.tags),
    activeMajorMissionId: asText(entry?.activeMajorMissionId) || null,
    majorMissionCooldownUntilDay: asInteger(entry?.majorMissionCooldownUntilDay),
    hope: asInteger(entry?.hope),
    dread: asInteger(entry?.dread),
    stockpiles: normalizeTownStockpiles(entry?.stockpiles),
    readiness: normalizeTownReadiness(entry?.readiness),
    economy: normalizeTownEconomy(entry?.economy),
    armory: normalizeTownArmory(entry?.armory),
    gate: normalizeTownGate(entry?.gate),
    buildQueue: sortObjects(entry?.buildQueue, normalizeTownBuildQueueEntry, (row) => `${String(row.queuedAtDay).padStart(6, '0')}:${row.id}`),
    autonomy: normalizeTownAutonomy(entry?.autonomy),
    crierQueue: sortObjects(entry?.crierQueue, normalizeTownCrierEntry, (row) => `${String(row.day).padStart(6, '0')}:${row.id}`),
    recentImpacts: sortObjects(entry?.recentImpacts, normalizeTownImpact, (row) => `${String(row.day).padStart(6, '0')}:${row.id}`),
    ...(spawn ? { spawn } : {})
  }
}

function normalizeActor(entry) {
  const actorId = asText(entry?.actorId)
  const townId = asText(entry?.townId)
  const role = asText(entry?.role)
  return {
    actorId,
    townId,
    name: asText(entry?.name, defaultActorName({ role, townName: defaultTownNameFromId(townId) })),
    role,
    status: asText(entry?.status, 'active')
  }
}

function normalizePlayer(entry, playerIdHint) {
  const normalized = normalizePlayerAssignment(entry, asText(playerIdHint))
  if (!normalized) return null
  return {
    playerId: normalized.playerId,
    townId: asText(normalized.townId),
    assignedAtDay: asInteger(normalized.assignedAtDay),
    spawnPolicy: asText(normalized.spawnPolicy)
  }
}

function normalizeNetherLedgerEntry(entry) {
  const id = asText(entry?.id)
  if (!id) return null
  return {
    id,
    day: asInteger(entry?.day),
    type: asText(entry?.type),
    payload: normalizeScalarRecord(entry?.payload),
    applied: entry?.applied !== false
  }
}

function projectAuthoritativeSnapshot(world) {
  const source = isPlainObject(world) ? world : {}
  const players = sortRecord(source.players, (entry, playerId) => normalizePlayer(entry, playerId))
  return {
    type: WORLD_SNAPSHOT_TYPE,
    schemaVersion: WORLD_SNAPSHOT_SCHEMA_VERSION,
    warActive: source.warActive === true,
    rules: {
      allowLethalPolitics: source.rules?.allowLethalPolitics !== false
    },
    player: {
      name: asText(source.player?.name),
      alive: source.player?.alive !== false,
      legitimacy: asInteger(source.player?.legitimacy)
    },
    factions: sortRecord(source.factions, (entry) => normalizeFaction(entry)),
    clock: {
      day: asInteger(source.clock?.day, 1),
      phase: asText(source.clock?.phase, 'day'),
      season: asText(source.clock?.season, 'dawn'),
      updated_at: asText(source.clock?.updated_at)
    },
    threat: {
      byTown: sortRecord(source.threat?.byTown, (value) => asInteger(value))
    },
    moods: {
      byTown: sortRecord(source.moods?.byTown, (entry) => normalizeMood(entry))
    },
    events: {
      seed: asInteger(source.events?.seed),
      index: asInteger(source.events?.index),
      active: sortObjects(source.events?.active, normalizeWorldEvent, (entry) => entry.id)
    },
    rumors: sortObjects(source.rumors, normalizeRumor, (entry) => entry.id),
    decisions: sortObjects(source.decisions, normalizeDecision, (entry) => entry.id),
    markers: sortObjects(source.markers, normalizeMarker, (entry) => `${entry.tag}:${entry.name}:${entry.x}:${entry.y}:${entry.z}`),
    markets: sortObjects(source.markets, normalizeMarket, (entry) => entry.name),
    economy: {
      currency: asText(source.economy?.currency),
      ledger: sortRecord(source.economy?.ledger, (value) => asInteger(value)),
      minted_total: asInteger(source.economy?.minted_total)
    },
    quests: sortObjects(source.quests, normalizeQuest, (entry) => entry.id),
    majorMissions: sortObjects(source.majorMissions, normalizeMajorMission, (entry) => entry.id),
    projects: sortObjects(source.projects, normalizeProject, (entry) => entry.id),
    salvageRuns: sortObjects(source.salvageRuns, normalizeSalvageRun, (entry) => entry.id),
    towns: sortRecord(source.towns, (entry) => normalizeTown(entry)),
    ...(Object.keys(players).length > 0 ? { players } : {}),
    actors: sortRecord(source.actors, (entry) => normalizeActor(entry)),
    nether: {
      eventLedger: sortObjects(source.nether?.eventLedger, normalizeNetherLedgerEntry, (entry) => `${String(entry.day).padStart(6, '0')}:${entry.id}`),
      modifiers: {
        longNight: asInteger(source.nether?.modifiers?.longNight),
        omen: asInteger(source.nether?.modifiers?.omen),
        scarcity: asInteger(source.nether?.modifiers?.scarcity),
        threat: asInteger(source.nether?.modifiers?.threat)
      },
      deckState: {
        seed: asInteger(source.nether?.deckState?.seed),
        cursor: asInteger(source.nether?.deckState?.cursor)
      },
      lastTickDay: asInteger(source.nether?.lastTickDay)
    }
  }
}

function createAuthoritativeSnapshotProjection(world) {
  const snapshot = projectAuthoritativeSnapshot(world)
  const snapshotHash = hashStableValue(snapshot)
  const decisionEpoch = Number.isInteger(snapshot.clock.day) && snapshot.clock.day >= 0
    ? snapshot.clock.day
    : null
  return {
    snapshot,
    snapshotHash,
    decisionEpoch
  }
}

module.exports = {
  WORLD_SNAPSHOT_SCHEMA_VERSION,
  WORLD_SNAPSHOT_TYPE,
  createAuthoritativeSnapshotProjection,
  hashStableValue,
  projectAuthoritativeSnapshot,
  stableStringify
}
