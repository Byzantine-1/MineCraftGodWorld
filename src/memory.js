const fs = require('fs')
const path = require('path')
const { createLogger } = require('./logger')
const { AppError } = require('./errors')
const {
  incrementMetric,
  getRuntimeMetrics,
  recordTransactionDuration,
  recordLockAcquisition
} = require('./runtimeMetrics')

/**
 * @typedef {{
 *   agents: Record<string, {
 *     short: string[],
 *     long: string[],
 *     summary: string,
 *     archive: Array<{time: number, event: string}>,
 *     recentUtterances: string[],
 *     lastProcessedTime: number,
 *     profile?: {
 *       rep?: Record<string, number>,
 *       traits?: {
 *         courage: number,
 *         greed: number,
 *         faith: number
 *       },
 *       titles?: string[],
 *       rumors_completed?: number
 *     }
 *   }>,
 *   factions: Record<string, {
 *     long: string[],
 *     summary: string,
 *     archive: Array<{time: number, event: string}>
 *   }>,
 *   world: {
 *     warActive: boolean,
 *     rules: { allowLethalPolitics: boolean },
 *     player: { name: string, alive: boolean, legitimacy: number },
 *     factions: Record<string, {
 *       hostilityToPlayer?: number,
 *       stability?: number,
 *       name?: string,
 *       towns?: string[],
 *       doctrine?: string,
 *       rivals?: string[]
 *     }>,
 *     clock: {
 *       day: number,
 *       phase: 'day' | 'night',
 *       season: 'dawn' | 'long_night',
 *       updated_at: string
 *     },
 *     threat: {
 *       byTown: Record<string, number>
 *     },
 *     moods: {
 *       byTown: Record<string, {
 *         fear: number,
 *         unrest: number,
 *         prosperity: number
 *       }>
 *     },
 *     events: {
 *       seed: number,
 *       index: number,
 *       active: Array<{
 *         id: string,
 *         type: 'festival' | 'shortage' | 'omen' | 'patrol' | 'fog' | 'tax_day',
 *         town: string,
 *         starts_day: number,
 *         ends_day: number,
 *         mods: Record<string, number>
 *       }>
 *     },
 *     rumors: Array<{
 *       id: string,
 *       town: string,
 *       text: string,
 *       kind: 'grounded' | 'supernatural' | 'political',
 *       severity: number,
 *       starts_day: number,
 *       expires_day: number,
 *       created_at: number,
 *       spawned_by_event_id?: string,
 *       resolved_by_quest_id?: string
 *     }>,
 *     decisions: Array<{
 *       id: string,
 *       town: string,
 *       event_id: string,
 *       event_type: 'festival' | 'shortage' | 'omen' | 'patrol' | 'fog' | 'tax_day',
 *       prompt: string,
 *       options: Array<{
 *         key: string,
 *         label: string,
 *         effects: {
 *           mood?: { fear?: number, unrest?: number, prosperity?: number },
 *           threat_delta?: number,
 *           rep_delta?: Record<string, number>,
 *           rumor_spawn?: {
 *             kind: 'grounded' | 'supernatural' | 'political',
 *             severity: number,
 *             templateKey: string,
 *             expiresInDays?: number
 *           }
 *         }
 *       }>,
 *       state: 'open' | 'chosen' | 'expired',
 *       chosen_key?: string,
 *       starts_day: number,
 *       expires_day: number,
 *       created_at: number
 *     }>,
 *     markers: Array<{name: string, x: number, y: number, z: number, tag: string, created_at: number}>,
 *     markets: Array<{
 *       name: string,
 *       marker?: string,
 *       created_at: number,
 *       offers: Array<{
 *         offer_id: string,
 *         owner: string,
 *         side: 'buy' | 'sell',
 *         amount: number,
 *         price: number,
 *         created_at: number,
 *         active: boolean
 *       }>
 *     }>,
 *     economy: {
 *       currency: 'emerald',
 *       ledger: Record<string, number>,
 *       minted_total?: number
 *     },
 *     chronicle: Array<{
 *       id: string,
 *       type: string,
 *       msg: string,
 *       at: number,
 *       town?: string,
 *       meta?: Record<string, string | number | boolean | null>
 *     }>,
 *     news: Array<{
 *       id: string,
 *       topic: string,
 *       msg: string,
 *       at: number,
 *       town?: string,
 *       meta?: Record<string, string | number | boolean | null>
 *     }>,
 *     quests: Array<{
 *       id: string,
 *       type: 'trade_n' | 'visit_town' | 'rumor_task',
 *       state: 'offered' | 'accepted' | 'in_progress' | 'completed' | 'cancelled' | 'failed',
 *       origin?: string,
 *       town?: string,
 *       townId?: string,
 *       npcKey?: string,
 *       supportsMajorMissionId?: string,
 *       offered_at: string,
 *       accepted_at?: string,
 *       owner?: string,
 *       objective: {
 *         kind: 'trade_n',
 *         n: number,
 *         market?: string
 *       } | {
 *         kind: 'visit_town',
 *         town: string
 *       },
 *       progress: {
 *         done: number
 *       } | {
 *         visited: boolean
 *       },
 *       reward: number,
 *       title: string,
 *       desc: string,
 *       meta?: Record<string, string | number | boolean | null>
 *     }>,
 *     majorMissions: Array<{
 *       id: string,
 *       townId: string,
 *       templateId: string,
 *       status: 'teased' | 'briefed' | 'active' | 'completed' | 'failed',
 *       phase: number | string,
 *       issuedAtDay: number,
 *       acceptedAtDay: number,
 *       stakes: Record<string, string | number | boolean | null>,
 *       progress: Record<string, string | number | boolean | null>
 *     }>,
 *     projects: Array<{
 *       id: string,
 *       townId: string,
 *       type: 'trench_reinforcement' | 'watchtower_line' | 'ration_depot' | 'field_chapel' | 'lantern_line',
 *       status: 'planned' | 'active' | 'completed' | 'failed',
 *       stage: number,
 *       requirements: Record<string, string | number | boolean | null>,
 *       effects: Record<string, string | number | boolean | null>,
 *       startedAtDay: number,
 *       updatedAtDay: number,
 *       supportsMajorMissionId?: string
 *     }>,
 *     salvageRuns: Array<{
 *       id: string,
 *       townId: string,
 *       targetKey: 'no_mans_land_scrap' | 'ruined_hamlet_supplies' | 'abandoned_shrine_relics' | 'collapsed_tunnel_tools',
 *       status: 'planned' | 'resolved' | 'failed',
 *       plannedAtDay: number,
 *       resolvedAtDay: number,
 *       result: Record<string, string | number | boolean | null>,
 *       outcomeKey?: string,
 *       supportsMajorMissionId?: string,
 *       supportsProjectId?: string
 *     }>,
 *     towns: Record<string, {
 *       activeMajorMissionId: string | null,
 *       majorMissionCooldownUntilDay: number,
 *       hope: number,
 *       dread: number,
 *       crierQueue: Array<{
 *         id: string,
 *         day: number,
 *         type: string,
 *         message: string,
 *         missionId?: string
 *       }>,
 *       recentImpacts: Array<{
 *         id: string,
 *         day: number,
 *         type: string,
 *         summary: string,
 *         missionId?: string,
 *         questId?: string,
 *         netherEventId?: string,
 *         projectId?: string,
 *         salvageRunId?: string
 *       }>
 *     }>,
 *     nether: {
 *       eventLedger: Array<{
 *         id: string,
 *         day: number,
 *         type: string,
 *         payload: Record<string, string | number | boolean | null>,
 *         applied: boolean
 *       }>,
 *       modifiers: {
 *         longNight: number,
 *         omen: number,
 *         scarcity: number,
 *         threat: number
 *       },
 *       deckState: {
 *         seed: number,
 *         cursor: number
 *       },
 *       lastTickDay: number
 *     },
 *     archive: Array<{time: number, event: string, important?: boolean}>,
 *     processedEventIds: string[]
 *   }
 * }} MemoryState
 */

/**
 * @param {unknown} value
 * @param {string} fallback
 * @param {number} maxLen
 */
function asText(value, fallback, maxLen) {
  if (typeof value !== 'string') return fallback
  const trimmed = value.trim()
  if (!trimmed) return fallback
  return trimmed.slice(0, maxLen)
}

/**
 * @param {number} n
 * @param {number} min
 * @param {number} max
 */
function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n))
}

const QUEST_TYPES = new Set(['trade_n', 'visit_town', 'rumor_task'])
const RUMOR_TASK_KINDS = new Set(['rumor_trade', 'rumor_visit', 'rumor_choice'])
const QUEST_STATES = new Set(['offered', 'accepted', 'in_progress', 'completed', 'cancelled', 'failed'])
const CLOCK_PHASES = new Set(['day', 'night'])
const CLOCK_SEASONS = new Set(['dawn', 'long_night'])
const STORY_FACTION_NAMES = ['iron_pact', 'veil_church']
const STORY_FACTION_NAME_SET = new Set(STORY_FACTION_NAMES)
const WORLD_EVENT_TYPES = new Set(['festival', 'shortage', 'omen', 'patrol', 'fog', 'tax_day'])
const RUMOR_KINDS = new Set(['grounded', 'supernatural', 'political'])
const DECISION_STATES = new Set(['open', 'chosen', 'expired'])
const TRAIT_NAMES = ['courage', 'greed', 'faith']
const TRAIT_NAME_SET = new Set(TRAIT_NAMES)
const DEFAULT_AGENT_TRAITS = { courage: 1, greed: 1, faith: 1 }
const MAX_AGENT_TITLE_LEN = 32
const MAX_AGENT_TITLE_COUNT = 20
const MAJOR_MISSION_STATUSES = new Set(['teased', 'briefed', 'active', 'completed', 'failed'])
const MAX_MAJOR_MISSION_STAKES_KEYS = 12
const MAX_MAJOR_MISSION_PROGRESS_KEYS = 12
const PROJECT_TYPES = new Set([
  'trench_reinforcement',
  'watchtower_line',
  'ration_depot',
  'field_chapel',
  'lantern_line'
])
const PROJECT_STATUSES = new Set(['planned', 'active', 'completed', 'failed'])
const SALVAGE_TARGET_KEYS = new Set([
  'no_mans_land_scrap',
  'ruined_hamlet_supplies',
  'abandoned_shrine_relics',
  'collapsed_tunnel_tools'
])
const SALVAGE_STATUSES = new Set(['planned', 'resolved', 'failed'])
const MAX_PROJECT_REQUIREMENTS_KEYS = 12
const MAX_PROJECT_EFFECTS_KEYS = 12
const MAX_PROJECT_ENTRIES = 120
const MAX_SALVAGE_RESULT_KEYS = 12
const MAX_SALVAGE_RUN_ENTRIES = 120
const MAX_TOWN_CRIER_QUEUE_ENTRIES = 40
const MAX_TOWNSFOLK_QUESTS_PER_TOWN = 24
const MAX_NETHER_EVENT_LEDGER_ENTRIES = 120
const MAX_NETHER_EVENT_PAYLOAD_KEYS = 10
const MAX_TOWN_RECENT_IMPACTS = 30
const TOWN_PRESSURE_MIN = 0
const TOWN_PRESSURE_MAX = 100
const DEFAULT_TOWN_HOPE = 50
const DEFAULT_TOWN_DREAD = 50
const TOWN_IMPACT_TYPE_KEYS = new Set([
  'nether_event',
  'mission_complete',
  'mission_fail',
  'townsfolk_complete',
  'townsfolk_fail',
  'project_start',
  'project_complete',
  'project_fail',
  'salvage_plan',
  'salvage_resolve',
  'salvage_fail'
])
const NETHER_EVENT_TYPE_KEYS = new Set([
  'LONG_NIGHT',
  'OMEN',
  'SCARCITY',
  'THREAT_SURGE',
  'CALM_BEFORE_STORM'
])
const WORLD_EVENT_MOD_KEYS = new Set([
  'fear',
  'unrest',
  'prosperity',
  'trade_reward_bonus',
  'visit_reward_bonus',
  'iron_pact_rep_bonus',
  'veil_church_rep_bonus'
])
const STORY_FACTION_DEFAULTS = {
  iron_pact: {
    towns: ['alpha'],
    doctrine: 'Order through steel.',
    rivals: ['veil_church']
  },
  veil_church: {
    towns: ['beta'],
    doctrine: 'Truth through shadow.',
    rivals: ['iron_pact']
  }
}

/**
 * @param {unknown} repInput
 */
function normalizeAgentRepShape(repInput) {
  const source = (repInput && typeof repInput === 'object' && !Array.isArray(repInput))
    ? repInput
    : {}
  const rep = {}
  for (const [factionRaw, valueRaw] of Object.entries(source)) {
    const faction = asText(factionRaw, '', 80)
    if (!faction) continue
    const value = Number(valueRaw)
    // Reputation sanitization policy: drop non-integer values on load.
    if (!Number.isInteger(value)) continue
    rep[faction] = value
  }
  return rep
}

/**
 * @param {unknown} traitsInput
 */
function normalizeAgentTraitsShape(traitsInput) {
  const source = (traitsInput && typeof traitsInput === 'object' && !Array.isArray(traitsInput))
    ? traitsInput
    : {}
  const traits = {}
  for (const traitName of TRAIT_NAMES) {
    const value = Number(source[traitName])
    traits[traitName] = Number.isFinite(value)
      ? clamp(Math.trunc(value), 0, 3)
      : DEFAULT_AGENT_TRAITS[traitName]
  }
  return traits
}

/**
 * @param {unknown} titlesInput
 */
function normalizeAgentTitlesShape(titlesInput) {
  const source = Array.isArray(titlesInput) ? titlesInput : []
  const titles = []
  const seen = new Set()
  for (const rawTitle of source) {
    const title = asText(rawTitle, '', MAX_AGENT_TITLE_LEN)
    if (!title) continue
    const key = title.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    titles.push(title)
    if (titles.length >= MAX_AGENT_TITLE_COUNT) break
  }
  return titles
}

/**
 * @param {unknown} value
 */
function normalizeRumorsCompletedShape(value) {
  const count = Number(value)
  if (!Number.isInteger(count) || count < 0) return 0
  return count
}

/**
 * @param {unknown} agentsInput
 */
function normalizeAgentsShape(agentsInput) {
  if (!agentsInput || typeof agentsInput !== 'object' || Array.isArray(agentsInput)) return {}
  const agents = {}
  for (const [nameRaw, record] of Object.entries(agentsInput)) {
    const name = asText(nameRaw, '', 80)
    if (!name) continue
    if (!record || typeof record !== 'object' || Array.isArray(record)) {
      agents[name] = record
      continue
    }
    const next = { ...record }
    const profileSource = (next.profile && typeof next.profile === 'object' && !Array.isArray(next.profile))
      ? next.profile
      : {}
    const profile = { ...profileSource }
    profile.rep = normalizeAgentRepShape(profile.rep)
    profile.traits = normalizeAgentTraitsShape(profile.traits)
    profile.titles = normalizeAgentTitlesShape(profile.titles)
    profile.rumors_completed = normalizeRumorsCompletedShape(profile.rumors_completed)
    next.profile = profile
    agents[name] = next
  }
  return agents
}

/**
 * @param {unknown} townsInput
 */
function normalizeStoryTowns(townsInput) {
  if (!Array.isArray(townsInput)) return []
  const towns = []
  const seen = new Set()
  for (const item of townsInput) {
    const town = asText(item, '', 80)
    if (!town) continue
    const key = town.toLowerCase()
    if (seen.has(key)) continue
    seen.add(key)
    towns.push(town)
  }
  towns.sort((a, b) => a.localeCompare(b))
  return towns
}

/**
 * @param {unknown} rivalsInput
 * @param {string} factionName
 */
function normalizeStoryRivals(rivalsInput, factionName) {
  const fallback = STORY_FACTION_DEFAULTS[factionName]?.rivals || []
  const source = Array.isArray(rivalsInput) ? rivalsInput : fallback
  const rivals = []
  const seen = new Set()
  for (const item of source) {
    const rival = asText(item, '', 80).toLowerCase()
    if (!rival || rival === factionName) continue
    if (!STORY_FACTION_NAME_SET.has(rival)) continue
    if (seen.has(rival)) continue
    seen.add(rival)
    rivals.push(rival)
  }
  if (rivals.length === 0) return [...fallback]
  return rivals
}

/**
 * @param {unknown} worldFactionsInput
 */
function normalizeWorldFactionsShape(worldFactionsInput) {
  const source = (worldFactionsInput && typeof worldFactionsInput === 'object' && !Array.isArray(worldFactionsInput))
    ? worldFactionsInput
    : {}
  const factions = {}

  // Preserve legacy faction records so existing hostility/stability logic remains compatible.
  for (const [nameRaw, entryRaw] of Object.entries(source)) {
    const name = asText(nameRaw, '', 80)
    if (!name || !entryRaw || typeof entryRaw !== 'object' || Array.isArray(entryRaw)) continue
    const entry = { ...entryRaw }
    const hostility = Number(entryRaw.hostilityToPlayer)
    const stability = Number(entryRaw.stability)
    if (Number.isFinite(hostility)) entry.hostilityToPlayer = clamp(Math.trunc(hostility), 0, 100)
    if (Number.isFinite(stability)) entry.stability = clamp(Math.trunc(stability), 0, 100)
    factions[name] = entry
  }

  for (const factionName of STORY_FACTION_NAMES) {
    const defaults = STORY_FACTION_DEFAULTS[factionName]
    const raw = (source[factionName] && typeof source[factionName] === 'object' && !Array.isArray(source[factionName]))
      ? source[factionName]
      : {}
    const towns = normalizeStoryTowns(raw.towns)
    const hostility = Number(raw.hostilityToPlayer)
    const stability = Number(raw.stability)
    const entry = {
      ...raw,
      name: factionName,
      towns: towns.length > 0 ? towns : [...defaults.towns],
      doctrine: asText(raw.doctrine, defaults.doctrine, 160),
      rivals: normalizeStoryRivals(raw.rivals, factionName),
      hostilityToPlayer: Number.isFinite(hostility) ? clamp(Math.trunc(hostility), 0, 100) : 10,
      stability: Number.isFinite(stability) ? clamp(Math.trunc(stability), 0, 100) : 70
    }
    factions[factionName] = entry
  }

  return factions
}

/**
 * @param {unknown} clockInput
 */
function normalizeClockShape(clockInput) {
  const source = (clockInput && typeof clockInput === 'object' && !Array.isArray(clockInput))
    ? clockInput
    : {}
  const day = Number(source.day)
  const phase = asText(source.phase, '', 20).toLowerCase()
  const season = asText(source.season, '', 20).toLowerCase()
  const updatedAt = normalizeIsoDateText(source.updated_at) || new Date().toISOString()
  return {
    day: Number.isInteger(day) && day >= 1 ? day : 1,
    phase: CLOCK_PHASES.has(phase) ? phase : 'day',
    season: CLOCK_SEASONS.has(season) ? season : 'dawn',
    updated_at: updatedAt
  }
}

/**
 * @param {unknown} threatInput
 */
function normalizeThreatShape(threatInput) {
  const source = (threatInput && typeof threatInput === 'object' && !Array.isArray(threatInput))
    ? threatInput
    : {}
  const byTownSource = (source.byTown && typeof source.byTown === 'object' && !Array.isArray(source.byTown))
    ? source.byTown
    : {}
  const byTown = {}
  for (const [townRaw, valueRaw] of Object.entries(byTownSource)) {
    const town = asText(townRaw, '', 80)
    const value = Number(valueRaw)
    if (!town || !Number.isFinite(value)) continue
    byTown[town] = clamp(Math.trunc(value), 0, 100)
  }
  return { byTown }
}

/**
 * @param {unknown} moodInput
 */
function normalizeMoodTownShape(moodInput) {
  if (!moodInput || typeof moodInput !== 'object' || Array.isArray(moodInput)) return null
  const fear = Number(moodInput.fear)
  const unrest = Number(moodInput.unrest)
  const prosperity = Number(moodInput.prosperity)
  const safeFear = Number.isFinite(fear) ? clamp(Math.floor(fear), 0, 100) : 0
  const safeUnrest = Number.isFinite(unrest) ? clamp(Math.floor(unrest), 0, 100) : 0
  const safeProsperity = Number.isFinite(prosperity) ? clamp(Math.floor(prosperity), 0, 100) : 0
  return {
    fear: safeFear,
    unrest: safeUnrest,
    prosperity: safeProsperity
  }
}

/**
 * @param {unknown} moodsInput
 */
function normalizeMoodsShape(moodsInput) {
  const source = (moodsInput && typeof moodsInput === 'object' && !Array.isArray(moodsInput))
    ? moodsInput
    : {}
  const byTownSource = (source.byTown && typeof source.byTown === 'object' && !Array.isArray(source.byTown))
    ? source.byTown
    : {}
  const byTown = {}
  for (const [townRaw, moodRaw] of Object.entries(byTownSource)) {
    const town = asText(townRaw, '', 80)
    if (!town) continue
    const mood = normalizeMoodTownShape(moodRaw)
    if (!mood) continue
    byTown[town] = mood
  }
  return { byTown }
}

/**
 * @param {unknown} modsInput
 */
function normalizeEventModsShape(modsInput) {
  if (!modsInput || typeof modsInput !== 'object' || Array.isArray(modsInput)) return {}
  const mods = {}
  for (const [keyRaw, valueRaw] of Object.entries(modsInput)) {
    const key = asText(keyRaw, '', 80)
    if (!key || !WORLD_EVENT_MOD_KEYS.has(key)) continue
    const value = Number(valueRaw)
    if (!Number.isFinite(value)) continue
    const safeValue = Math.trunc(value)
    if (safeValue === 0) continue
    mods[key] = safeValue
  }
  return mods
}

/**
 * @param {unknown} eventInput
 */
function normalizeEventShape(eventInput) {
  if (!eventInput || typeof eventInput !== 'object' || Array.isArray(eventInput)) return null
  const id = asText(eventInput.id, '', 200)
  const type = asText(eventInput.type, '', 40).toLowerCase()
  const town = asText(eventInput.town, '', 80)
  const startsDay = Number(eventInput.starts_day)
  const endsDay = Number(eventInput.ends_day)
  const mods = normalizeEventModsShape(eventInput.mods)
  if (!id || !town) return null
  if (!WORLD_EVENT_TYPES.has(type)) return null
  if (!Number.isInteger(startsDay) || startsDay < 1) return null
  if (!Number.isInteger(endsDay) || endsDay < startsDay) return null
  return {
    id,
    type,
    town,
    starts_day: startsDay,
    ends_day: endsDay,
    mods
  }
}

/**
 * @param {unknown} eventsInput
 */
function normalizeEventsShape(eventsInput) {
  const source = (eventsInput && typeof eventsInput === 'object' && !Array.isArray(eventsInput))
    ? eventsInput
    : {}
  const seed = Number(source.seed)
  const index = Number(source.index)
  const active = (Array.isArray(source.active) ? source.active : [])
    .map(normalizeEventShape)
    .filter(Boolean)
  return {
    seed: Number.isInteger(seed) ? seed : 1337,
    index: Number.isInteger(index) && index >= 0 ? index : 0,
    active
  }
}

/**
 * @param {unknown} rumorInput
 */
function normalizeRumorShape(rumorInput) {
  if (!rumorInput || typeof rumorInput !== 'object' || Array.isArray(rumorInput)) return null
  const id = asText(rumorInput.id, '', 200)
  const town = asText(rumorInput.town, '', 80)
  const text = asText(rumorInput.text, '', 240)
  const kind = asText(rumorInput.kind, '', 20).toLowerCase()
  const severity = Number(rumorInput.severity)
  const startsDay = Number(rumorInput.starts_day)
  const expiresDay = Number(rumorInput.expires_day)
  const createdAt = Number(rumorInput.created_at)
  const spawnedByEventId = asText(rumorInput.spawned_by_event_id, '', 200)
  const resolvedByQuestId = asText(rumorInput.resolved_by_quest_id, '', 200)
  if (!id || !town || !text) return null
  if (!RUMOR_KINDS.has(kind)) return null
  // Rumor sanitize policy: drop entries with invalid severity instead of coercing.
  if (!Number.isInteger(severity) || severity < 1 || severity > 3) return null
  if (!Number.isInteger(startsDay) || startsDay < 1) return null
  if (!Number.isInteger(expiresDay) || expiresDay < startsDay) return null
  if (!Number.isFinite(createdAt) || createdAt < 0) return null
  const rumor = {
    id,
    town,
    text,
    kind,
    severity,
    starts_day: startsDay,
    expires_day: expiresDay,
    created_at: createdAt
  }
  if (spawnedByEventId) rumor.spawned_by_event_id = spawnedByEventId
  if (resolvedByQuestId) rumor.resolved_by_quest_id = resolvedByQuestId
  return rumor
}

/**
 * @param {unknown} rumorsInput
 */
function normalizeRumorsShape(rumorsInput) {
  return (Array.isArray(rumorsInput) ? rumorsInput : [])
    .map(normalizeRumorShape)
    .filter(Boolean)
}

/**
 * @param {unknown} repDeltaInput
 */
function normalizeDecisionRepDelta(repDeltaInput) {
  if (!repDeltaInput || typeof repDeltaInput !== 'object' || Array.isArray(repDeltaInput)) return null
  const repDelta = {}
  for (const [factionRaw, valueRaw] of Object.entries(repDeltaInput)) {
    const faction = asText(factionRaw, '', 80).toLowerCase()
    const value = Number(valueRaw)
    if (!faction || !Number.isInteger(value) || value === 0) continue
    repDelta[faction] = value
  }
  return Object.keys(repDelta).length > 0 ? repDelta : null
}

/**
 * @param {unknown} rumorSpawnInput
 */
function normalizeDecisionRumorSpawn(rumorSpawnInput) {
  if (!rumorSpawnInput || typeof rumorSpawnInput !== 'object' || Array.isArray(rumorSpawnInput)) return null
  const kind = asText(rumorSpawnInput.kind, '', 20).toLowerCase()
  const severity = Number(rumorSpawnInput.severity)
  const templateKey = asText(rumorSpawnInput.templateKey, '', 80)
  const expiresInDays = Number(rumorSpawnInput.expiresInDays)
  if (!RUMOR_KINDS.has(kind)) return null
  if (!Number.isInteger(severity) || severity < 1 || severity > 3) return null
  if (!templateKey) return null
  const rumorSpawn = {
    kind,
    severity,
    templateKey
  }
  if (Number.isInteger(expiresInDays) && expiresInDays >= 0) {
    rumorSpawn.expiresInDays = expiresInDays
  }
  return rumorSpawn
}

/**
 * @param {unknown} effectsInput
 */
function normalizeDecisionEffects(effectsInput) {
  if (!effectsInput || typeof effectsInput !== 'object' || Array.isArray(effectsInput)) return null
  const effects = {}

  const moodSource = (effectsInput.mood && typeof effectsInput.mood === 'object' && !Array.isArray(effectsInput.mood))
    ? effectsInput.mood
    : null
  if (moodSource) {
    const mood = {}
    for (const key of ['fear', 'unrest', 'prosperity']) {
      const value = Number(moodSource[key])
      if (!Number.isInteger(value) || value === 0) continue
      mood[key] = value
    }
    if (Object.keys(mood).length > 0) effects.mood = mood
  }

  const threatDelta = Number(effectsInput.threat_delta)
  if (Number.isInteger(threatDelta) && threatDelta !== 0) {
    effects.threat_delta = threatDelta
  }

  const repDelta = normalizeDecisionRepDelta(effectsInput.rep_delta)
  if (repDelta) effects.rep_delta = repDelta

  const rumorSpawn = normalizeDecisionRumorSpawn(effectsInput.rumor_spawn)
  if (rumorSpawn) effects.rumor_spawn = rumorSpawn

  return Object.keys(effects).length > 0 ? effects : null
}

/**
 * @param {unknown} optionInput
 */
function normalizeDecisionOption(optionInput) {
  if (!optionInput || typeof optionInput !== 'object' || Array.isArray(optionInput)) return null
  const key = asText(optionInput.key, '', 40).toLowerCase()
  const label = asText(optionInput.label, '', 120)
  const effects = normalizeDecisionEffects(optionInput.effects)
  if (!key || !label || !effects) return null
  return { key, label, effects }
}

/**
 * @param {unknown} decisionInput
 */
function normalizeDecisionShape(decisionInput) {
  if (!decisionInput || typeof decisionInput !== 'object' || Array.isArray(decisionInput)) return null
  const id = asText(decisionInput.id, '', 200)
  const town = asText(decisionInput.town, '', 80)
  const eventId = asText(decisionInput.event_id, '', 200)
  const eventType = asText(decisionInput.event_type, '', 40).toLowerCase()
  const prompt = asText(decisionInput.prompt, '', 240)
  const state = asText(decisionInput.state, '', 20).toLowerCase()
  const chosenKey = asText(decisionInput.chosen_key, '', 40).toLowerCase()
  const startsDay = Number(decisionInput.starts_day)
  const expiresDay = Number(decisionInput.expires_day)
  const createdAt = Number(decisionInput.created_at)
  if (!id || !town || !eventId || !prompt) return null
  if (!WORLD_EVENT_TYPES.has(eventType)) return null
  if (!DECISION_STATES.has(state)) return null
  if (!Number.isInteger(startsDay) || startsDay < 1) return null
  if (!Number.isInteger(expiresDay) || expiresDay < startsDay) return null
  if (!Number.isFinite(createdAt) || createdAt < 0) return null

  const optionsRaw = Array.isArray(decisionInput.options) ? decisionInput.options : []
  const options = []
  const optionKeys = new Set()
  for (const rawOption of optionsRaw) {
    const option = normalizeDecisionOption(rawOption)
    if (!option) continue
    if (optionKeys.has(option.key)) continue
    optionKeys.add(option.key)
    options.push(option)
    if (options.length >= 3) break
  }
  if (options.length < 2) return null
  if (state === 'chosen') {
    if (!chosenKey || !optionKeys.has(chosenKey)) return null
  }

  const decision = {
    id,
    town,
    event_id: eventId,
    event_type: eventType,
    prompt,
    options,
    state,
    starts_day: startsDay,
    expires_day: expiresDay,
    created_at: createdAt
  }
  if (chosenKey) decision.chosen_key = chosenKey
  return decision
}

/**
 * @param {unknown} decisionsInput
 */
function normalizeDecisionsShape(decisionsInput) {
  return (Array.isArray(decisionsInput) ? decisionsInput : [])
    .map(normalizeDecisionShape)
    .filter(Boolean)
}

/**
 * @param {unknown} economyInput
 */
function normalizeEconomyShape(economyInput) {
  const source = (economyInput && typeof economyInput === 'object' && !Array.isArray(economyInput))
    ? economyInput
    : {}
  const ledgerSource = (source.ledger && typeof source.ledger === 'object' && !Array.isArray(source.ledger))
    ? source.ledger
    : {}
  const ledger = {}
  for (const [agentName, balanceRaw] of Object.entries(ledgerSource)) {
    const safeName = asText(agentName, '', 80)
    if (!safeName) continue
    // Economy v0 sanitization policy: drop malformed/non-finite/negative ledger entries.
    if (typeof balanceRaw !== 'number' || !Number.isFinite(balanceRaw) || balanceRaw < 0) continue
    ledger[safeName] = balanceRaw
  }
  const economy = {
    currency: 'emerald',
    ledger
  }
  if (typeof source.minted_total === 'number' && Number.isFinite(source.minted_total) && source.minted_total >= 0) {
    economy.minted_total = source.minted_total
  }
  return economy
}

/**
 * @param {unknown} offerInput
 */
function normalizeMarketOfferShape(offerInput) {
  if (!offerInput || typeof offerInput !== 'object' || Array.isArray(offerInput)) return null
  const offerId = asText(offerInput.offer_id, '', 160)
  const owner = asText(offerInput.owner, '', 80)
  const side = asText(offerInput.side, '', 20).toLowerCase()
  const amount = Number(offerInput.amount)
  const price = Number(offerInput.price)
  const createdAt = Number(offerInput.created_at || 0) || 0
  const active = !!offerInput.active
  if (!offerId || !owner) return null
  if (side !== 'buy' && side !== 'sell') return null
  if (!Number.isInteger(amount) || amount < 0) return null
  if (active && amount <= 0) return null
  if (!Number.isInteger(price) || price <= 0) return null
  return {
    offer_id: offerId,
    owner,
    side,
    amount,
    price,
    created_at: createdAt,
    active
  }
}

/**
 * @param {unknown} marketInput
 */
function normalizeMarketShape(marketInput) {
  if (!marketInput || typeof marketInput !== 'object' || Array.isArray(marketInput)) return null
  const name = asText(marketInput.name, '', 80)
  const marker = asText(marketInput.marker, '', 80)
  const createdAt = Number(marketInput.created_at || 0) || 0
  const offers = Array.isArray(marketInput.offers)
    ? marketInput.offers.map(normalizeMarketOfferShape).filter(Boolean)
    : []
  if (!name) return null
  const market = { name, created_at: createdAt, offers }
  if (marker) market.marker = marker
  return market
}

/**
 * @param {unknown} marketsInput
 */
function normalizeMarketsShape(marketsInput) {
  return (Array.isArray(marketsInput) ? marketsInput : [])
    .map(normalizeMarketShape)
    .filter(Boolean)
}

/**
 * @param {unknown} metaInput
 */
function normalizeFeedMetaShape(metaInput) {
  if (!metaInput || typeof metaInput !== 'object' || Array.isArray(metaInput)) return null
  const meta = {}
  for (const [keyRaw, value] of Object.entries(metaInput)) {
    const key = asText(keyRaw, '', 80)
    if (!key) continue
    if (typeof value === 'string') {
      const text = asText(value, '', 160)
      if (text) meta[key] = text
      continue
    }
    if (typeof value === 'number') {
      if (Number.isFinite(value)) meta[key] = value
      continue
    }
    if (typeof value === 'boolean' || value === null) {
      meta[key] = value
    }
  }
  return Object.keys(meta).length > 0 ? meta : null
}

/**
 * @param {unknown} entryInput
 */
function normalizeChronicleEntryShape(entryInput) {
  if (!entryInput || typeof entryInput !== 'object' || Array.isArray(entryInput)) return null
  const id = asText(entryInput.id, '', 200)
  const type = asText(entryInput.type, '', 40).toLowerCase()
  const msg = asText(entryInput.msg, '', 240)
  const at = Number(entryInput.at)
  const town = asText(entryInput.town, '', 80)
  const meta = normalizeFeedMetaShape(entryInput.meta)
  if (!id || !type || !msg) return null
  if (!Number.isFinite(at) || at < 0) return null
  const entry = { id, type, msg, at }
  if (town) entry.town = town
  if (meta) entry.meta = meta
  return entry
}

/**
 * @param {unknown} chronicleInput
 */
function normalizeChronicleShape(chronicleInput) {
  return (Array.isArray(chronicleInput) ? chronicleInput : [])
    .map(normalizeChronicleEntryShape)
    .filter(Boolean)
}

/**
 * @param {unknown} entryInput
 */
function normalizeNewsEntryShape(entryInput) {
  if (!entryInput || typeof entryInput !== 'object' || Array.isArray(entryInput)) return null
  const id = asText(entryInput.id, '', 200)
  const topic = asText(entryInput.topic, '', 40).toLowerCase()
  const msg = asText(entryInput.msg, '', 240)
  const at = Number(entryInput.at)
  const town = asText(entryInput.town, '', 80)
  const meta = normalizeFeedMetaShape(entryInput.meta)
  if (!id || !topic || !msg) return null
  if (!Number.isFinite(at) || at < 0) return null
  const entry = { id, topic, msg, at }
  if (town) entry.town = town
  if (meta) entry.meta = meta
  return entry
}

/**
 * @param {unknown} newsInput
 */
function normalizeNewsShape(newsInput) {
  return (Array.isArray(newsInput) ? newsInput : [])
    .map(normalizeNewsEntryShape)
    .filter(Boolean)
}

/**
 * @param {unknown} value
 */
function normalizeIsoDateText(value) {
  const text = asText(value, '', 80)
  if (!text) return ''
  const atMs = Date.parse(text)
  if (!Number.isFinite(atMs)) return ''
  return new Date(atMs).toISOString()
}

/**
 * @param {'trade_n' | 'visit_town' | 'rumor_task'} type
 * @param {unknown} objectiveInput
 * @param {unknown} progressInput
 */
function normalizeQuestObjectiveAndProgress(type, objectiveInput, progressInput) {
  const objective = (objectiveInput && typeof objectiveInput === 'object' && !Array.isArray(objectiveInput))
    ? objectiveInput
    : null
  const progress = (progressInput && typeof progressInput === 'object' && !Array.isArray(progressInput))
    ? progressInput
    : null
  if (!objective || !progress) return null

  if (type === 'trade_n') {
    const objectiveKind = asText(objective.kind, '', 20).toLowerCase()
    const n = Number(objective.n)
    const market = asText(objective.market, '', 80)
    const done = Number(progress.done)
    if (objectiveKind !== 'trade_n') return null
    if (!Number.isInteger(n) || n < 1) return null
    if (!Number.isInteger(done) || done < 0) return null
    const normalizedObjective = { kind: 'trade_n', n }
    if (market) normalizedObjective.market = market
    return {
      objective: normalizedObjective,
      progress: { done }
    }
  }

  if (type === 'visit_town') {
    const objectiveKind = asText(objective.kind, '', 20).toLowerCase()
    const town = asText(objective.town, '', 80)
    if (objectiveKind !== 'visit_town' || !town) return null
    if (typeof progress.visited !== 'boolean') return null
    return {
      objective: { kind: 'visit_town', town },
      progress: { visited: progress.visited }
    }
  }

  if (type === 'rumor_task') {
    const objectiveKind = asText(objective.kind, '', 20).toLowerCase()
    const rumorId = asText(objective.rumor_id, '', 200)
    const rumorTask = asText(objective.rumor_task, '', 20).toLowerCase()
    if (objectiveKind !== 'rumor_task') return null
    if (!rumorId || !RUMOR_TASK_KINDS.has(rumorTask)) return null
    const normalizedObjective = {
      kind: 'rumor_task',
      rumor_id: rumorId,
      rumor_task: rumorTask
    }
    if (rumorTask === 'rumor_trade') {
      const n = Number(objective.n)
      const market = asText(objective.market, '', 80)
      const done = Number(progress.done)
      if (!Number.isInteger(n) || n < 1) return null
      if (!Number.isInteger(done) || done < 0) return null
      normalizedObjective.n = n
      if (market) normalizedObjective.market = market
      return {
        objective: normalizedObjective,
        progress: { done }
      }
    }
    if (rumorTask === 'rumor_visit' || rumorTask === 'rumor_choice') {
      const town = asText(objective.town, '', 80)
      if (!town) return null
      if (typeof progress.visited !== 'boolean') return null
      normalizedObjective.town = town
      return {
        objective: normalizedObjective,
        progress: { visited: progress.visited }
      }
    }
    return null
  }

  return null
}

/**
 * @param {unknown} questInput
 */
function normalizeQuestShape(questInput) {
  if (!questInput || typeof questInput !== 'object' || Array.isArray(questInput)) return null
  const id = asText(questInput.id, '', 200)
  const type = asText(questInput.type, '', 20).toLowerCase()
  const state = asText(questInput.state, '', 20).toLowerCase()
  const origin = asText(questInput.origin, '', 40).toLowerCase()
  const town = asText(questInput.town, '', 80)
  const townId = asText(questInput.townId, '', 80)
  const npcKey = asText(questInput.npcKey, '', 80)
  const supportsMajorMissionId = asText(questInput.supportsMajorMissionId, '', 200)
  const offeredAt = normalizeIsoDateText(questInput.offered_at)
  const acceptedAt = normalizeIsoDateText(questInput.accepted_at)
  const owner = asText(questInput.owner, '', 80)
  const reward = Number(questInput.reward)
  const title = asText(questInput.title, '', 120)
  const desc = asText(questInput.desc, '', 120)
  const meta = normalizeFeedMetaShape(questInput.meta)
  const rumorIdRaw = asText(questInput.rumor_id, '', 200)

  if (!id) return null
  if (!QUEST_TYPES.has(type)) return null
  if (!QUEST_STATES.has(state)) return null
  if (!offeredAt) return null
  if (!Number.isInteger(reward) || reward < 0) return null
  if (!title || !desc) return null

  const objectiveProgress = normalizeQuestObjectiveAndProgress(type, questInput.objective, questInput.progress)
  if (!objectiveProgress) return null

  const quest = {
    id,
    type,
    state,
    offered_at: offeredAt,
    objective: objectiveProgress.objective,
    progress: objectiveProgress.progress,
    reward,
    title,
    desc
  }
  const objectiveRumorId = asText(objectiveProgress.objective?.rumor_id, '', 200)
  const rumorId = rumorIdRaw || objectiveRumorId
  if (type === 'rumor_task' && !rumorId) return null
  if (rumorId) quest.rumor_id = rumorId
  if (origin) quest.origin = origin
  if (town) quest.town = town
  if (townId) quest.townId = townId
  if (npcKey) quest.npcKey = npcKey
  if (supportsMajorMissionId) quest.supportsMajorMissionId = supportsMajorMissionId
  if (acceptedAt) quest.accepted_at = acceptedAt
  if (owner) quest.owner = owner
  if (meta) quest.meta = meta
  return quest
}

/**
 * @param {unknown} questsInput
 */
function normalizeQuestsShape(questsInput) {
  return (Array.isArray(questsInput) ? questsInput : [])
    .map(normalizeQuestShape)
    .filter(Boolean)
}

/**
 * @param {unknown} questInput
 */
function isTownsfolkQuestShape(questInput) {
  const quest = normalizeQuestShape(questInput)
  if (!quest) return false
  return asText(quest.origin, '', 40).toLowerCase() === 'townsfolk'
}

/**
 * @param {any} quest
 */
function asQuestTime(quest) {
  const offeredAt = normalizeIsoDateText(quest?.offered_at)
  const atMs = Date.parse(offeredAt)
  if (Number.isFinite(atMs)) return atMs
  return 0
}

/**
 * @param {any} quest
 */
function isQuestActiveState(quest) {
  const state = asText(quest?.state, '', 20).toLowerCase()
  return state === 'accepted' || state === 'in_progress'
}

/**
 * @param {any[]} quests
 */
function boundTownsfolkQuestHistoryShape(quests) {
  const safeQuests = (Array.isArray(quests) ? quests : [])
    .map(normalizeQuestShape)
    .filter(Boolean)

  const byTown = new Map()
  for (const quest of safeQuests) {
    if (!isTownsfolkQuestShape(quest)) continue
    const townKey = asText(quest.townId || quest.town, '', 80).toLowerCase()
    if (!townKey) continue
    if (!byTown.has(townKey)) byTown.set(townKey, [])
    byTown.get(townKey).push(quest)
  }

  const dropIds = new Set()
  for (const questsForTown of byTown.values()) {
    if (questsForTown.length <= MAX_TOWNSFOLK_QUESTS_PER_TOWN) continue
    const active = questsForTown
      .filter(isQuestActiveState)
      .sort((left, right) => asQuestTime(right) - asQuestTime(left) || left.id.localeCompare(right.id))
    const inactive = questsForTown
      .filter(quest => !isQuestActiveState(quest))
      .sort((left, right) => asQuestTime(right) - asQuestTime(left) || left.id.localeCompare(right.id))

    const keep = new Set()
    const ordered = [...active, ...inactive]
    for (const quest of ordered) {
      if (keep.size >= MAX_TOWNSFOLK_QUESTS_PER_TOWN) break
      keep.add(quest.id.toLowerCase())
    }
    for (const quest of questsForTown) {
      const key = quest.id.toLowerCase()
      if (!keep.has(key)) dropIds.add(key)
    }
  }
  if (dropIds.size === 0) return safeQuests
  return safeQuests.filter(quest => !dropIds.has(quest.id.toLowerCase()))
}

/**
 * @param {unknown} value
 */
function normalizeMajorMissionPayloadValue(value) {
  if (value === null) return null
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return null
    return Math.trunc(value)
  }
  if (typeof value === 'string') {
    const text = asText(value, '', 120)
    return text || null
  }
  return null
}

/**
 * @param {unknown} payloadInput
 * @param {number} maxKeys
 */
function normalizeMajorMissionPayload(payloadInput, maxKeys) {
  if (!payloadInput || typeof payloadInput !== 'object' || Array.isArray(payloadInput)) return {}
  const payload = {}
  let used = 0
  for (const [keyRaw, valueRaw] of Object.entries(payloadInput)) {
    if (used >= maxKeys) break
    const key = asText(keyRaw, '', 40)
    if (!key) continue
    const value = normalizeMajorMissionPayloadValue(valueRaw)
    if (value === null) continue
    payload[key] = value
    used += 1
  }
  return payload
}

/**
 * @param {unknown} valueInput
 */
function normalizeNetherModifierValue(valueInput) {
  const value = Number(valueInput)
  if (!Number.isFinite(value)) return 0
  return clamp(Math.trunc(value), -9, 9)
}

/**
 * @param {unknown} modifiersInput
 */
function normalizeNetherModifiersShape(modifiersInput) {
  const source = (modifiersInput && typeof modifiersInput === 'object' && !Array.isArray(modifiersInput))
    ? modifiersInput
    : {}
  return {
    longNight: normalizeNetherModifierValue(source.longNight),
    omen: normalizeNetherModifierValue(source.omen),
    scarcity: normalizeNetherModifierValue(source.scarcity),
    threat: normalizeNetherModifierValue(source.threat)
  }
}

/**
 * @param {unknown} deckStateInput
 * @param {number} fallbackSeed
 */
function normalizeNetherDeckStateShape(deckStateInput, fallbackSeed) {
  const source = (deckStateInput && typeof deckStateInput === 'object' && !Array.isArray(deckStateInput))
    ? deckStateInput
    : {}
  const seed = Number(source.seed)
  const cursor = Number(source.cursor)
  return {
    seed: Number.isInteger(seed) ? seed : fallbackSeed,
    cursor: Number.isInteger(cursor) && cursor >= 0 ? cursor : 0
  }
}

/**
 * @param {unknown} ledgerEntryInput
 */
function normalizeNetherEventLedgerEntryShape(ledgerEntryInput) {
  if (!ledgerEntryInput || typeof ledgerEntryInput !== 'object' || Array.isArray(ledgerEntryInput)) return null
  const id = asText(ledgerEntryInput.id, '', 200)
  const day = Number(ledgerEntryInput.day)
  const typeRaw = asText(ledgerEntryInput.type, '', 40).toUpperCase()
  const type = NETHER_EVENT_TYPE_KEYS.has(typeRaw) ? typeRaw : ''
  if (!id || !Number.isInteger(day) || day < 1 || !type) return null
  return {
    id,
    day,
    type,
    payload: normalizeMajorMissionPayload(ledgerEntryInput.payload, MAX_NETHER_EVENT_PAYLOAD_KEYS),
    applied: ledgerEntryInput.applied !== false
  }
}

/**
 * @param {unknown} ledgerInput
 */
function normalizeNetherEventLedgerShape(ledgerInput) {
  const ledger = (Array.isArray(ledgerInput) ? ledgerInput : [])
    .map(normalizeNetherEventLedgerEntryShape)
    .filter(Boolean)
  if (ledger.length > MAX_NETHER_EVENT_LEDGER_ENTRIES) {
    return ledger.slice(-MAX_NETHER_EVENT_LEDGER_ENTRIES)
  }
  return ledger
}

/**
 * @param {unknown} netherInput
 * @param {number} fallbackSeed
 */
function normalizeNetherShape(netherInput, fallbackSeed) {
  const source = (netherInput && typeof netherInput === 'object' && !Array.isArray(netherInput))
    ? netherInput
    : {}
  const seed = Number.isInteger(fallbackSeed) ? fallbackSeed : 1337
  const lastTickDay = Number(source.lastTickDay)
  const nether = {
    eventLedger: normalizeNetherEventLedgerShape(source.eventLedger),
    modifiers: normalizeNetherModifiersShape(source.modifiers),
    deckState: normalizeNetherDeckStateShape(source.deckState, seed),
    lastTickDay: Number.isInteger(lastTickDay) && lastTickDay >= 0 ? lastTickDay : 0
  }
  if (nether.lastTickDay > 0) return nether
  let inferred = 0
  for (const entry of nether.eventLedger) {
    if (entry.day > inferred) inferred = entry.day
  }
  nether.lastTickDay = inferred
  return nether
}

/**
 * @param {unknown} majorMissionInput
 */
function normalizeMajorMissionShape(majorMissionInput) {
  if (!majorMissionInput || typeof majorMissionInput !== 'object' || Array.isArray(majorMissionInput)) return null
  const id = asText(majorMissionInput.id, '', 200)
  const townId = asText(majorMissionInput.townId, '', 80)
  const templateId = asText(majorMissionInput.templateId, '', 80).toLowerCase()
  const status = asText(majorMissionInput.status, '', 20).toLowerCase()
  const issuedAtDay = Number(majorMissionInput.issuedAtDay)
  const acceptedAtDayRaw = Number(majorMissionInput.acceptedAtDay)
  const phaseRaw = majorMissionInput.phase

  if (!id || !townId || !templateId) return null
  if (!MAJOR_MISSION_STATUSES.has(status)) return null
  if (!Number.isInteger(issuedAtDay) || issuedAtDay < 1) return null
  if (!Number.isInteger(acceptedAtDayRaw) || acceptedAtDayRaw < 0) return null

  let phase = null
  if (Number.isInteger(phaseRaw) && phaseRaw >= 0) {
    phase = phaseRaw
  } else {
    const phaseText = asText(phaseRaw, '', 40)
    if (phaseText) phase = phaseText
  }
  if (phase === null) return null

  return {
    id,
    townId,
    templateId,
    status,
    phase,
    issuedAtDay,
    acceptedAtDay: acceptedAtDayRaw,
    stakes: normalizeMajorMissionPayload(majorMissionInput.stakes, MAX_MAJOR_MISSION_STAKES_KEYS),
    progress: normalizeMajorMissionPayload(majorMissionInput.progress, MAX_MAJOR_MISSION_PROGRESS_KEYS)
  }
}

/**
 * @param {unknown} majorMissionsInput
 */
function normalizeMajorMissionsShape(majorMissionsInput) {
  return (Array.isArray(majorMissionsInput) ? majorMissionsInput : [])
    .map(normalizeMajorMissionShape)
    .filter(Boolean)
}

/**
 * @param {unknown} projectInput
 */
function normalizeProjectShape(projectInput) {
  if (!projectInput || typeof projectInput !== 'object' || Array.isArray(projectInput)) return null
  const id = asText(projectInput.id, '', 200)
  const townId = asText(projectInput.townId, '', 80)
  const type = asText(projectInput.type, '', 40).toLowerCase()
  const status = asText(projectInput.status, '', 20).toLowerCase()
  const stage = Number(projectInput.stage)
  const startedAtDay = Number(projectInput.startedAtDay)
  const updatedAtDay = Number(projectInput.updatedAtDay)
  const supportsMajorMissionId = asText(projectInput.supportsMajorMissionId, '', 200)

  if (!id || !townId || !PROJECT_TYPES.has(type) || !PROJECT_STATUSES.has(status)) return null
  if (!Number.isInteger(stage) || stage < 0) return null
  if (!Number.isInteger(startedAtDay) || startedAtDay < 1) return null
  if (!Number.isInteger(updatedAtDay) || updatedAtDay < startedAtDay) return null

  const project = {
    id,
    townId,
    type,
    status,
    stage,
    requirements: normalizeMajorMissionPayload(projectInput.requirements, MAX_PROJECT_REQUIREMENTS_KEYS),
    effects: normalizeMajorMissionPayload(projectInput.effects, MAX_PROJECT_EFFECTS_KEYS),
    startedAtDay,
    updatedAtDay
  }
  if (supportsMajorMissionId) project.supportsMajorMissionId = supportsMajorMissionId
  return project
}

/**
 * @param {unknown} projectsInput
 */
function normalizeProjectsShape(projectsInput) {
  const projects = (Array.isArray(projectsInput) ? projectsInput : [])
    .map(normalizeProjectShape)
    .filter(Boolean)
  if (projects.length > MAX_PROJECT_ENTRIES) {
    return projects.slice(-MAX_PROJECT_ENTRIES)
  }
  return projects
}

/**
 * @param {unknown} salvageInput
 */
function normalizeSalvageRunShape(salvageInput) {
  if (!salvageInput || typeof salvageInput !== 'object' || Array.isArray(salvageInput)) return null
  const id = asText(salvageInput.id, '', 200)
  const townId = asText(salvageInput.townId, '', 80)
  const targetKey = asText(salvageInput.targetKey, '', 80).toLowerCase()
  const status = asText(salvageInput.status, '', 20).toLowerCase()
  const plannedAtDay = Number(salvageInput.plannedAtDay)
  const resolvedAtDayRaw = Number(salvageInput.resolvedAtDay)
  const outcomeKey = asText(salvageInput.outcomeKey, '', 80).toLowerCase()
  const supportsMajorMissionId = asText(salvageInput.supportsMajorMissionId, '', 200)
  const supportsProjectId = asText(salvageInput.supportsProjectId, '', 200)

  if (!id || !townId || !SALVAGE_TARGET_KEYS.has(targetKey) || !SALVAGE_STATUSES.has(status)) return null
  if (!Number.isInteger(plannedAtDay) || plannedAtDay < 1) return null
  if (!Number.isInteger(resolvedAtDayRaw) || resolvedAtDayRaw < 0) return null

  const resolvedAtDay = status === 'planned'
    ? 0
    : Math.max(plannedAtDay, resolvedAtDayRaw)
  const salvageRun = {
    id,
    townId,
    targetKey,
    status,
    plannedAtDay,
    resolvedAtDay,
    result: normalizeMajorMissionPayload(salvageInput.result, MAX_SALVAGE_RESULT_KEYS)
  }
  if (outcomeKey) salvageRun.outcomeKey = outcomeKey
  if (supportsMajorMissionId) salvageRun.supportsMajorMissionId = supportsMajorMissionId
  if (supportsProjectId) salvageRun.supportsProjectId = supportsProjectId
  return salvageRun
}

/**
 * @param {unknown} salvageRunsInput
 */
function normalizeSalvageRunsShape(salvageRunsInput) {
  const runs = (Array.isArray(salvageRunsInput) ? salvageRunsInput : [])
    .map(normalizeSalvageRunShape)
    .filter(Boolean)
  if (runs.length > MAX_SALVAGE_RUN_ENTRIES) {
    return runs.slice(-MAX_SALVAGE_RUN_ENTRIES)
  }
  return runs
}

/**
 * @param {unknown} crierEntryInput
 */
function normalizeTownCrierEntryShape(crierEntryInput) {
  if (!crierEntryInput || typeof crierEntryInput !== 'object' || Array.isArray(crierEntryInput)) return null
  const id = asText(crierEntryInput.id, '', 200)
  const day = Number(crierEntryInput.day)
  const type = asText(crierEntryInput.type, '', 40).toLowerCase()
  const message = asText(crierEntryInput.message, '', 240)
  const missionId = asText(crierEntryInput.missionId, '', 200)
  if (!id || !Number.isInteger(day) || day < 0 || !type || !message) return null
  const entry = { id, day, type, message }
  if (missionId) entry.missionId = missionId
  return entry
}

/**
 * @param {unknown} crierQueueInput
 */
function normalizeTownCrierQueueShape(crierQueueInput) {
  const queue = (Array.isArray(crierQueueInput) ? crierQueueInput : [])
    .map(normalizeTownCrierEntryShape)
    .filter(Boolean)
  if (queue.length > MAX_TOWN_CRIER_QUEUE_ENTRIES) {
    return queue.slice(-MAX_TOWN_CRIER_QUEUE_ENTRIES)
  }
  return queue
}

/**
 * @param {unknown} impactInput
 */
function normalizeTownImpactEntryShape(impactInput) {
  if (!impactInput || typeof impactInput !== 'object' || Array.isArray(impactInput)) return null
  const id = asText(impactInput.id, '', 200)
  const day = Number(impactInput.day)
  const typeRaw = asText(impactInput.type, '', 40).toLowerCase()
  const type = TOWN_IMPACT_TYPE_KEYS.has(typeRaw) ? typeRaw : ''
  const summary = asText(impactInput.summary, '', 160)
  const missionId = asText(impactInput.missionId, '', 200)
  const questId = asText(impactInput.questId, '', 200)
  const netherEventId = asText(impactInput.netherEventId, '', 200)
  const projectId = asText(impactInput.projectId, '', 200)
  const salvageRunId = asText(impactInput.salvageRunId, '', 200)
  if (!id || !Number.isInteger(day) || day < 0 || !type || !summary) return null
  const entry = { id, day, type, summary }
  if (missionId) entry.missionId = missionId
  if (questId) entry.questId = questId
  if (netherEventId) entry.netherEventId = netherEventId
  if (projectId) entry.projectId = projectId
  if (salvageRunId) entry.salvageRunId = salvageRunId
  return entry
}

/**
 * @param {unknown} impactsInput
 */
function normalizeTownRecentImpactsShape(impactsInput) {
  const impacts = (Array.isArray(impactsInput) ? impactsInput : [])
    .map(normalizeTownImpactEntryShape)
    .filter(Boolean)
  if (impacts.length > MAX_TOWN_RECENT_IMPACTS) {
    return impacts.slice(-MAX_TOWN_RECENT_IMPACTS)
  }
  return impacts
}

/**
 * @param {unknown} valueInput
 * @param {number} fallback
 */
function normalizeTownPressureValueShape(valueInput, fallback) {
  const value = Number(valueInput)
  if (!Number.isFinite(value)) return fallback
  return clamp(Math.trunc(value), TOWN_PRESSURE_MIN, TOWN_PRESSURE_MAX)
}

/**
 * @param {unknown} townInput
 */
function normalizeTownMissionStateShape(townInput) {
  const source = (townInput && typeof townInput === 'object' && !Array.isArray(townInput))
    ? townInput
    : {}
  const activeMajorMissionId = asText(source.activeMajorMissionId, '', 200) || null
  const cooldown = Number(source.majorMissionCooldownUntilDay)
  return {
    activeMajorMissionId,
    majorMissionCooldownUntilDay: Number.isInteger(cooldown) && cooldown >= 0 ? cooldown : 0,
    hope: normalizeTownPressureValueShape(source.hope, DEFAULT_TOWN_HOPE),
    dread: normalizeTownPressureValueShape(source.dread, DEFAULT_TOWN_DREAD),
    crierQueue: normalizeTownCrierQueueShape(source.crierQueue),
    recentImpacts: normalizeTownRecentImpactsShape(source.recentImpacts)
  }
}

/**
 * @param {unknown} townsInput
 */
function normalizeTownsShape(townsInput) {
  const source = (townsInput && typeof townsInput === 'object' && !Array.isArray(townsInput))
    ? townsInput
    : {}
  const towns = {}
  for (const [townRaw, townState] of Object.entries(source)) {
    const townName = asText(townRaw, '', 80)
    if (!townName) continue
    towns[townName] = normalizeTownMissionStateShape(townState)
  }
  return towns
}

/**
 * @param {unknown} tag
 */
function parseTownNameFromTag(tag) {
  const safeTag = asText(tag, '', 80)
  if (!safeTag) return ''
  const match = /^(town|settlement)\s*:\s*(.+)$/i.exec(safeTag)
  if (!match) return ''
  return asText(match[2], '', 80)
}

/**
 * @param {MemoryState['world']} world
 */
function collectTownNamesForMissionState(world) {
  const names = new Map()
  const addTown = (nameRaw) => {
    const townName = asText(nameRaw, '', 80)
    if (!townName) return
    const key = townName.toLowerCase()
    if (!names.has(key)) names.set(key, townName)
  }

  for (const townName of Object.keys(world?.towns || {})) addTown(townName)
  for (const mission of world?.majorMissions || []) addTown(mission?.townId)
  for (const project of world?.projects || []) addTown(project?.townId)
  for (const salvageRun of world?.salvageRuns || []) addTown(salvageRun?.townId)
  for (const marker of world?.markers || []) addTown(parseTownNameFromTag(marker?.tag))
  for (const townName of Object.keys(world?.threat?.byTown || {})) addTown(townName)
  for (const townName of Object.keys(world?.moods?.byTown || {})) addTown(townName)
  for (const faction of Object.values(world?.factions || {})) {
    for (const townName of normalizeStoryTowns(faction?.towns)) addTown(townName)
  }
  return Array.from(names.values()).sort((a, b) => a.localeCompare(b))
}

/**
 * @param {MemoryState['world']} world
 */
function reconcileMajorMissionState(world) {
  world.quests = boundTownsfolkQuestHistoryShape(world.quests)
  world.nether = normalizeNetherShape(world.nether, Number(world?.events?.seed))
  world.majorMissions = normalizeMajorMissionsShape(world.majorMissions)
  world.projects = normalizeProjectsShape(world.projects)
  world.salvageRuns = normalizeSalvageRunsShape(world.salvageRuns)
  world.towns = normalizeTownsShape(world.towns)

  const activeByTown = new Map()
  for (let idx = 0; idx < world.majorMissions.length; idx += 1) {
    const mission = normalizeMajorMissionShape(world.majorMissions[idx])
    if (!mission) continue
    if (mission.status === 'active') {
      const key = mission.townId.toLowerCase()
      if (activeByTown.has(key)) {
        mission.status = 'briefed'
      } else {
        activeByTown.set(key, mission.id)
      }
    }
    world.majorMissions[idx] = mission
  }

  for (const townName of collectTownNamesForMissionState(world)) {
    if (!Object.prototype.hasOwnProperty.call(world.towns, townName)) {
      world.towns[townName] = normalizeTownMissionStateShape(null)
    }
    const key = townName.toLowerCase()
    const expectedActiveId = activeByTown.get(key) || null
    world.towns[townName].activeMajorMissionId = expectedActiveId
    world.towns[townName].hope = normalizeTownPressureValueShape(world.towns[townName].hope, DEFAULT_TOWN_HOPE)
    world.towns[townName].dread = normalizeTownPressureValueShape(world.towns[townName].dread, DEFAULT_TOWN_DREAD)
    world.towns[townName].crierQueue = normalizeTownCrierQueueShape(world.towns[townName].crierQueue)
    world.towns[townName].recentImpacts = normalizeTownRecentImpactsShape(world.towns[townName].recentImpacts)
    const cooldown = Number(world.towns[townName].majorMissionCooldownUntilDay)
    world.towns[townName].majorMissionCooldownUntilDay = Number.isInteger(cooldown) && cooldown >= 0 ? cooldown : 0
  }
}

/**
 * @param {Partial<MemoryState> | null | undefined} input
 * @returns {MemoryState}
 */
function freshMemoryShape(input) {
  const source = input || {}
  const world = {
    warActive: !!source.world?.warActive,
    rules: {
      allowLethalPolitics: source.world?.rules?.allowLethalPolitics !== false
    },
    player: {
      name: asText(source.world?.player?.name, 'Player', 60),
      alive: source.world?.player?.alive !== false,
      legitimacy: clamp(Number(source.world?.player?.legitimacy ?? 50), 0, 100)
    },
    factions: normalizeWorldFactionsShape(source.world?.factions),
    clock: normalizeClockShape(source.world?.clock),
    threat: normalizeThreatShape(source.world?.threat),
    moods: normalizeMoodsShape(source.world?.moods),
    events: normalizeEventsShape(source.world?.events),
    rumors: normalizeRumorsShape(source.world?.rumors),
    decisions: normalizeDecisionsShape(source.world?.decisions),
    markers: Array.isArray(source.world?.markers)
      ? source.world.markers
        .filter(item => !!item && typeof item === 'object')
        .map(item => ({
          name: asText(item.name, '', 80),
          x: Number(item.x || 0),
          y: Number(item.y || 0),
          z: Number(item.z || 0),
          tag: asText(item.tag, '', 80),
          created_at: Number(item.created_at || 0) || 0
        }))
        .filter(item => item.name && Number.isFinite(item.x) && Number.isFinite(item.y) && Number.isFinite(item.z))
      : [],
    markets: normalizeMarketsShape(source.world?.markets),
    economy: normalizeEconomyShape(source.world?.economy),
    chronicle: normalizeChronicleShape(source.world?.chronicle),
    news: normalizeNewsShape(source.world?.news),
    quests: boundTownsfolkQuestHistoryShape(normalizeQuestsShape(source.world?.quests)),
    majorMissions: normalizeMajorMissionsShape(source.world?.majorMissions),
    projects: normalizeProjectsShape(source.world?.projects),
    salvageRuns: normalizeSalvageRunsShape(source.world?.salvageRuns),
    towns: normalizeTownsShape(source.world?.towns),
    nether: normalizeNetherShape(source.world?.nether, Number(source.world?.events?.seed)),
    archive: Array.isArray(source.world?.archive) ? source.world.archive : [],
    processedEventIds: Array.isArray(source.world?.processedEventIds) ? source.world.processedEventIds : []
  }
  world.nether = normalizeNetherShape(world.nether, Number(world.events?.seed))
  reconcileMajorMissionState(world)

  return {
    agents: normalizeAgentsShape(source.agents),
    factions: source.factions || {},
    world
  }
}

/**
 * @param {MemoryState} memory
 * @param {string} agent
 */
function initAgent(memory, agent) {
  if (!memory.agents[agent]) {
    memory.agents[agent] = {
      short: [],
      long: [],
      summary: '',
      archive: [],
      recentUtterances: [],
      lastProcessedTime: 0
    }
    return
  }
  memory.agents[agent].recentUtterances = memory.agents[agent].recentUtterances || []
  memory.agents[agent].lastProcessedTime = memory.agents[agent].lastProcessedTime || 0
}

/**
 * @param {MemoryState} memory
 * @param {string} faction
 */
function initFaction(memory, faction) {
  if (!memory.factions[faction]) {
    memory.factions[faction] = {
      long: [],
      summary: '',
      archive: []
    }
  }
  memory.world.factions[faction] = memory.world.factions[faction] || {
    hostilityToPlayer: 10,
    stability: 70
  }
}

/**
 * @param {string[]} ids
 * @param {string} eventId
 */
function hasEvent(ids, eventId) {
  return ids.includes(eventId)
}

/**
 * @param {MemoryState} memory
 * @param {string} eventId
 */
function markEvent(memory, eventId) {
  memory.world.processedEventIds.push(eventId)
  if (memory.world.processedEventIds.length > 1000) {
    memory.world.processedEventIds = memory.world.processedEventIds.slice(-1000)
  }
}

/**
 * @param {string[]} entries
 */
function summarize(entries) {
  return `History shaped by: ${entries.slice(-10).join(' ')}`.slice(0, 500)
}

/**
 * @param {MemoryState} memory
 * @returns {MemoryState}
 */
function cloneMemory(memory) {
  if (typeof structuredClone === 'function') return structuredClone(memory)
  return JSON.parse(JSON.stringify(memory))
}

/**
 * @param {MemoryState} memory
 * @returns {{ok: boolean, issues: string[]}}
 */
function validateMemoryIntegritySnapshot(memory) {
  const issues = []
  const world = memory?.world || {}
  const ids = Array.isArray(world.processedEventIds) ? world.processedEventIds : null

  if (!ids) {
    issues.push('world.processedEventIds must be an array.')
  } else {
    const seen = new Set()
    for (const id of ids) {
      if (typeof id !== 'string' || !id.trim()) issues.push('processedEventIds contains invalid value.')
      if (seen.has(id)) issues.push(`Duplicate eventId found: ${id}`)
      seen.add(id)
    }
  }

  const agents = memory?.agents || {}
  for (const [name, agent] of Object.entries(agents)) {
    if (!agent || typeof agent !== 'object') {
      issues.push(`Invalid agent record for ${name}.`)
      continue
    }
    if (Object.prototype.hasOwnProperty.call(agent, 'profile') && agent.profile === undefined) {
      issues.push(`Agent ${name} has undefined profile.`)
      continue
    }
    if (agent.profile && typeof agent.profile === 'object') {
      if (Object.prototype.hasOwnProperty.call(agent.profile, 'trust')) {
        const trust = Number(agent.profile.trust)
        if (!Number.isFinite(trust) || trust < 0 || trust > 10) {
          issues.push(`Agent ${name} has out-of-range trust: ${agent.profile.trust}`)
        }
      }
      if (Object.prototype.hasOwnProperty.call(agent.profile, 'rep') && agent.profile.rep !== undefined) {
        if (!agent.profile.rep || typeof agent.profile.rep !== 'object' || Array.isArray(agent.profile.rep)) {
          issues.push(`Agent ${name} has invalid rep shape.`)
        } else {
          for (const [factionName, repValue] of Object.entries(agent.profile.rep)) {
            if (!asText(factionName, '', 80)) issues.push(`Agent ${name} has invalid rep faction key.`)
            if (!Number.isInteger(repValue)) issues.push(`Agent ${name} rep for ${factionName || '?'} must be integer.`)
          }
        }
      }
      if (Object.prototype.hasOwnProperty.call(agent.profile, 'traits') && agent.profile.traits !== undefined) {
        const traits = agent.profile.traits
        if (!traits || typeof traits !== 'object' || Array.isArray(traits)) {
          issues.push(`Agent ${name} has invalid traits shape.`)
        } else {
          for (const traitName of TRAIT_NAMES) {
            const value = Number(traits[traitName])
            if (!Number.isInteger(value) || value < 0 || value > 3) {
              issues.push(`Agent ${name} trait ${traitName} must be integer in [0..3].`)
            }
          }
          for (const key of Object.keys(traits)) {
            if (!TRAIT_NAME_SET.has(key)) {
              issues.push(`Agent ${name} has unknown trait key: ${key}`)
            }
          }
        }
      }
      if (Object.prototype.hasOwnProperty.call(agent.profile, 'titles') && agent.profile.titles !== undefined) {
        const titles = agent.profile.titles
        if (!Array.isArray(titles)) {
          issues.push(`Agent ${name} titles must be an array.`)
        } else {
          if (titles.length > MAX_AGENT_TITLE_COUNT) {
            issues.push(`Agent ${name} titles exceeds max count ${MAX_AGENT_TITLE_COUNT}.`)
          }
          for (const title of titles) {
            const safeTitle = asText(title, '', MAX_AGENT_TITLE_LEN)
            if (!safeTitle) issues.push(`Agent ${name} has invalid title value.`)
          }
        }
      }
      if (Object.prototype.hasOwnProperty.call(agent.profile, 'rumors_completed') && agent.profile.rumors_completed !== undefined) {
        const rumorsCompleted = Number(agent.profile.rumors_completed)
        if (!Number.isInteger(rumorsCompleted) || rumorsCompleted < 0) {
          issues.push(`Agent ${name} rumors_completed must be integer >= 0.`)
        }
      }
    }
  }

  if (typeof world.warActive !== 'boolean') issues.push('world.warActive must be boolean.')
  if (typeof world.rules?.allowLethalPolitics !== 'boolean') issues.push('world.rules.allowLethalPolitics must be boolean.')
  if (typeof world.player?.alive !== 'boolean') issues.push('world.player.alive must be boolean.')
  const legitimacy = Number(world.player?.legitimacy)
  if (!Number.isFinite(legitimacy) || legitimacy < 0 || legitimacy > 100) {
    issues.push(`world.player.legitimacy out of range: ${world.player?.legitimacy}`)
  }
  if (!world.clock || typeof world.clock !== 'object' || Array.isArray(world.clock)) {
    issues.push('world.clock must be an object.')
  } else {
    if (!Number.isInteger(world.clock.day) || world.clock.day < 1) {
      issues.push('world.clock.day must be integer >= 1.')
    }
    const phase = asText(world.clock.phase, '', 20).toLowerCase()
    if (!CLOCK_PHASES.has(phase)) {
      issues.push('world.clock.phase must be "day" or "night".')
    }
    const season = asText(world.clock.season, '', 20).toLowerCase()
    if (!CLOCK_SEASONS.has(season)) {
      issues.push('world.clock.season must be "dawn" or "long_night".')
    }
    if (!normalizeIsoDateText(world.clock.updated_at)) {
      issues.push('world.clock.updated_at must be a valid ISO datetime string.')
    }
  }
  if (!world.threat || typeof world.threat !== 'object' || Array.isArray(world.threat)) {
    issues.push('world.threat must be an object.')
  } else {
    const byTown = world.threat.byTown
    if (!byTown || typeof byTown !== 'object' || Array.isArray(byTown)) {
      issues.push('world.threat.byTown must be an object.')
    } else {
      for (const [townName, level] of Object.entries(byTown)) {
        if (!asText(townName, '', 80)) issues.push('world.threat.byTown contains invalid town name.')
        if (!Number.isInteger(level) || level < 0 || level > 100) {
          issues.push(`world.threat.byTown[${townName || '?'}] must be integer in [0..100].`)
        }
      }
    }
  }
  if (!world.moods || typeof world.moods !== 'object' || Array.isArray(world.moods)) {
    issues.push('world.moods must be an object.')
  } else {
    const byTown = world.moods.byTown
    if (!byTown || typeof byTown !== 'object' || Array.isArray(byTown)) {
      issues.push('world.moods.byTown must be an object.')
    } else {
      for (const [townName, mood] of Object.entries(byTown)) {
        if (!asText(townName, '', 80)) issues.push('world.moods.byTown contains invalid town name.')
        if (!mood || typeof mood !== 'object' || Array.isArray(mood)) {
          issues.push(`world.moods.byTown[${townName || '?'}] must be an object.`)
          continue
        }
        const fear = Number(mood.fear)
        const unrest = Number(mood.unrest)
        const prosperity = Number(mood.prosperity)
        if (!Number.isInteger(fear) || fear < 0 || fear > 100) {
          issues.push(`world.moods.byTown[${townName || '?'}].fear must be integer in [0..100].`)
        }
        if (!Number.isInteger(unrest) || unrest < 0 || unrest > 100) {
          issues.push(`world.moods.byTown[${townName || '?'}].unrest must be integer in [0..100].`)
        }
        if (!Number.isInteger(prosperity) || prosperity < 0 || prosperity > 100) {
          issues.push(`world.moods.byTown[${townName || '?'}].prosperity must be integer in [0..100].`)
        }
      }
    }
  }
  if (!world.events || typeof world.events !== 'object' || Array.isArray(world.events)) {
    issues.push('world.events must be an object.')
  } else {
    if (!Number.isInteger(world.events.seed)) {
      issues.push('world.events.seed must be an integer.')
    }
    if (!Number.isInteger(world.events.index) || world.events.index < 0) {
      issues.push('world.events.index must be integer >= 0.')
    }
    if (!Array.isArray(world.events.active)) {
      issues.push('world.events.active must be an array.')
    } else {
      for (const entry of world.events.active) {
        if (!normalizeEventShape(entry)) {
          issues.push('world.events.active contains invalid entry.')
        }
      }
    }
  }
  if (!Array.isArray(world.rumors)) {
    issues.push('world.rumors must be an array.')
  } else {
    for (const entry of world.rumors) {
      if (!normalizeRumorShape(entry)) {
        issues.push('world.rumors contains invalid rumor entry.')
      }
    }
  }
  if (!Array.isArray(world.decisions)) {
    issues.push('world.decisions must be an array.')
  } else {
    for (const entry of world.decisions) {
      if (!normalizeDecisionShape(entry)) {
        issues.push('world.decisions contains invalid decision entry.')
      }
    }
  }
  if (!world.factions || typeof world.factions !== 'object' || Array.isArray(world.factions)) {
    issues.push('world.factions must be an object.')
  } else {
    for (const factionName of STORY_FACTION_NAMES) {
      const faction = world.factions[factionName]
      if (!faction || typeof faction !== 'object' || Array.isArray(faction)) {
        issues.push(`world.factions.${factionName} must be an object.`)
        continue
      }
      if (asText(faction.name, '', 80).toLowerCase() !== factionName) {
        issues.push(`world.factions.${factionName}.name must be "${factionName}".`)
      }
      if (!Array.isArray(faction.towns)) {
        issues.push(`world.factions.${factionName}.towns must be an array.`)
      } else {
        for (const townName of faction.towns) {
          if (!asText(townName, '', 80)) issues.push(`world.factions.${factionName}.towns contains invalid town.`)
        }
      }
      if (!asText(faction.doctrine, '', 160)) {
        issues.push(`world.factions.${factionName}.doctrine must be text.`)
      }
      if (!Array.isArray(faction.rivals)) {
        issues.push(`world.factions.${factionName}.rivals must be an array.`)
      } else {
        for (const rival of faction.rivals) {
          if (!asText(rival, '', 80)) issues.push(`world.factions.${factionName}.rivals contains invalid rival.`)
        }
      }
    }
  }
  if (!Array.isArray(world.markers)) {
    issues.push('world.markers must be an array.')
  } else {
    for (const marker of world.markers) {
      if (!marker || typeof marker !== 'object') {
        issues.push('world.markers contains invalid marker.')
        continue
      }
      if (!asText(marker.name, '', 80)) issues.push('world.markers contains marker with invalid name.')
      if (!Number.isFinite(Number(marker.x))) issues.push(`world.markers[${marker.name || '?'}].x must be numeric.`)
      if (!Number.isFinite(Number(marker.y))) issues.push(`world.markers[${marker.name || '?'}].y must be numeric.`)
      if (!Number.isFinite(Number(marker.z))) issues.push(`world.markers[${marker.name || '?'}].z must be numeric.`)
    }
  }
  if (!Array.isArray(world.markets)) {
    issues.push('world.markets must be an array.')
  } else {
    for (const market of world.markets) {
      if (!market || typeof market !== 'object' || Array.isArray(market)) {
        issues.push('world.markets contains invalid market.')
        continue
      }
      const marketName = asText(market.name, '', 80)
      if (!marketName) issues.push('world.markets contains market with invalid name.')
      if (!Number.isFinite(Number(market.created_at || 0))) {
        issues.push(`world.markets[${marketName || '?'}].created_at must be numeric.`)
      }
      if (Object.prototype.hasOwnProperty.call(market, 'marker') && market.marker !== undefined) {
        if (!asText(market.marker, '', 80)) issues.push(`world.markets[${marketName || '?'}].marker must be text when present.`)
      }
      if (!Array.isArray(market.offers)) {
        issues.push(`world.markets[${marketName || '?'}].offers must be an array.`)
        continue
      }
      for (const offer of market.offers) {
        if (!offer || typeof offer !== 'object' || Array.isArray(offer)) {
          issues.push(`world.markets[${marketName || '?'}].offers contains invalid offer.`)
          continue
        }
        const offerId = asText(offer.offer_id, '', 160)
        const owner = asText(offer.owner, '', 80)
        const side = asText(offer.side, '', 20).toLowerCase()
        const amount = Number(offer.amount)
        const price = Number(offer.price)
        if (!offerId) issues.push(`world.markets[${marketName || '?'}] offer has invalid offer_id.`)
        if (!owner) issues.push(`world.markets[${marketName || '?'}] offer has invalid owner.`)
        if (side !== 'buy' && side !== 'sell') issues.push(`world.markets[${marketName || '?'}] offer has invalid side.`)
        if (!Number.isInteger(amount) || amount < 0) {
          issues.push(`world.markets[${marketName || '?'}] offer amount must be integer >= 0.`)
        }
        if (!Number.isInteger(price) || price <= 0) {
          issues.push(`world.markets[${marketName || '?'}] offer price must be integer > 0.`)
        }
        if (typeof offer.active !== 'boolean') {
          issues.push(`world.markets[${marketName || '?'}] offer active must be boolean.`)
        } else if (offer.active && amount <= 0) {
          issues.push(`world.markets[${marketName || '?'}] active offer amount must be > 0.`)
        }
        if (!Number.isFinite(Number(offer.created_at || 0))) {
          issues.push(`world.markets[${marketName || '?'}] offer created_at must be numeric.`)
        }
      }
    }
  }
  if (!Array.isArray(world.chronicle)) {
    issues.push('world.chronicle must be an array.')
  } else {
    for (const entry of world.chronicle) {
      if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
        issues.push('world.chronicle contains invalid entry.')
        continue
      }
      if (!asText(entry.id, '', 200)) issues.push('world.chronicle entry id must be text.')
      if (!asText(entry.type, '', 40)) issues.push('world.chronicle entry type must be text.')
      if (!asText(entry.msg, '', 240)) issues.push('world.chronicle entry msg must be text.')
      if (!Number.isFinite(Number(entry.at)) || Number(entry.at) < 0) {
        issues.push('world.chronicle entry at must be finite and >= 0.')
      }
      if (Object.prototype.hasOwnProperty.call(entry, 'town') && entry.town !== undefined) {
        if (!asText(entry.town, '', 80)) issues.push('world.chronicle entry town must be text when present.')
      }
      if (Object.prototype.hasOwnProperty.call(entry, 'meta') && entry.meta !== undefined) {
        if (!entry.meta || typeof entry.meta !== 'object' || Array.isArray(entry.meta)) {
          issues.push('world.chronicle entry meta must be an object when present.')
        }
      }
    }
  }
  if (!Array.isArray(world.news)) {
    issues.push('world.news must be an array.')
  } else {
    for (const entry of world.news) {
      if (!entry || typeof entry !== 'object' || Array.isArray(entry)) {
        issues.push('world.news contains invalid entry.')
        continue
      }
      if (!asText(entry.id, '', 200)) issues.push('world.news entry id must be text.')
      if (!asText(entry.topic, '', 40)) issues.push('world.news entry topic must be text.')
      if (!asText(entry.msg, '', 240)) issues.push('world.news entry msg must be text.')
      if (!Number.isFinite(Number(entry.at)) || Number(entry.at) < 0) {
        issues.push('world.news entry at must be finite and >= 0.')
      }
      if (Object.prototype.hasOwnProperty.call(entry, 'town') && entry.town !== undefined) {
        if (!asText(entry.town, '', 80)) issues.push('world.news entry town must be text when present.')
      }
      if (Object.prototype.hasOwnProperty.call(entry, 'meta') && entry.meta !== undefined) {
        if (!entry.meta || typeof entry.meta !== 'object' || Array.isArray(entry.meta)) {
          issues.push('world.news entry meta must be an object when present.')
        }
      }
    }
  }
  if (!Array.isArray(world.quests)) {
    issues.push('world.quests must be an array.')
  } else {
    const townsfolkCountByTown = new Map()
    for (const quest of world.quests) {
      const normalizedQuest = normalizeQuestShape(quest)
      if (!normalizedQuest) {
        issues.push('world.quests contains invalid quest entry.')
        continue
      }
      if (Object.prototype.hasOwnProperty.call(quest, 'origin') && !asText(quest.origin, '', 40)) {
        issues.push('world.quests origin must be text when present.')
      }
      if (Object.prototype.hasOwnProperty.call(quest, 'townId') && !asText(quest.townId, '', 80)) {
        issues.push('world.quests townId must be text when present.')
      }
      if (Object.prototype.hasOwnProperty.call(quest, 'npcKey') && !asText(quest.npcKey, '', 80)) {
        issues.push('world.quests npcKey must be text when present.')
      }
      if (Object.prototype.hasOwnProperty.call(quest, 'supportsMajorMissionId') && !asText(quest.supportsMajorMissionId, '', 200)) {
        issues.push('world.quests supportsMajorMissionId must be text when present.')
      }
      if (asText(normalizedQuest.origin, '', 40).toLowerCase() === 'townsfolk') {
        const townKey = asText(normalizedQuest.townId || normalizedQuest.town, '', 80).toLowerCase()
        if (!townKey) {
          issues.push('world.quests townsfolk entries must include town or townId.')
          continue
        }
        townsfolkCountByTown.set(townKey, Number(townsfolkCountByTown.get(townKey) || 0) + 1)
      }
    }
    for (const [townKey, count] of townsfolkCountByTown.entries()) {
      if (count > MAX_TOWNSFOLK_QUESTS_PER_TOWN) {
        issues.push(`world.quests townsfolk history exceeds max ${MAX_TOWNSFOLK_QUESTS_PER_TOWN} for town ${townKey}.`)
      }
    }
  }
  if (!world.nether || typeof world.nether !== 'object' || Array.isArray(world.nether)) {
    issues.push('world.nether must be an object.')
  } else {
    if (!Array.isArray(world.nether.eventLedger)) {
      issues.push('world.nether.eventLedger must be an array.')
    } else {
      if (world.nether.eventLedger.length > MAX_NETHER_EVENT_LEDGER_ENTRIES) {
        issues.push(`world.nether.eventLedger exceeds max entries ${MAX_NETHER_EVENT_LEDGER_ENTRIES}.`)
      }
      const seenLedgerIds = new Set()
      for (const entry of world.nether.eventLedger) {
        const normalizedEntry = normalizeNetherEventLedgerEntryShape(entry)
        if (!normalizedEntry) {
          issues.push('world.nether.eventLedger contains invalid entry.')
          continue
        }
        const key = normalizedEntry.id.toLowerCase()
        if (seenLedgerIds.has(key)) issues.push(`world.nether.eventLedger has duplicate id ${normalizedEntry.id}.`)
        seenLedgerIds.add(key)
      }
    }
    const modifiers = world.nether.modifiers
    if (!modifiers || typeof modifiers !== 'object' || Array.isArray(modifiers)) {
      issues.push('world.nether.modifiers must be an object.')
    } else {
      for (const key of ['longNight', 'omen', 'scarcity', 'threat']) {
        const value = Number(modifiers[key])
        if (!Number.isInteger(value) || value < -9 || value > 9) {
          issues.push(`world.nether.modifiers.${key} must be integer in [-9..9].`)
        }
      }
    }
    const deckState = world.nether.deckState
    if (!deckState || typeof deckState !== 'object' || Array.isArray(deckState)) {
      issues.push('world.nether.deckState must be an object.')
    } else {
      if (!Number.isInteger(deckState.seed)) issues.push('world.nether.deckState.seed must be an integer.')
      if (!Number.isInteger(deckState.cursor) || deckState.cursor < 0) {
        issues.push('world.nether.deckState.cursor must be integer >= 0.')
      }
    }
    if (!Number.isInteger(world.nether.lastTickDay) || world.nether.lastTickDay < 0) {
      issues.push('world.nether.lastTickDay must be integer >= 0.')
    }
  }
  const activeMissionByTown = new Map()
  if (!Array.isArray(world.majorMissions)) {
    issues.push('world.majorMissions must be an array.')
  } else {
    for (const mission of world.majorMissions) {
      const normalizedMission = normalizeMajorMissionShape(mission)
      if (!normalizedMission) {
        issues.push('world.majorMissions contains invalid major mission entry.')
        continue
      }
      if (normalizedMission.status === 'active') {
        const key = normalizedMission.townId.toLowerCase()
        const existing = activeMissionByTown.get(key)
        if (existing && existing !== normalizedMission.id) {
          issues.push(`world.majorMissions has multiple active missions for town ${normalizedMission.townId}.`)
        } else {
          activeMissionByTown.set(key, normalizedMission.id)
        }
      }
    }
  }
  if (!Array.isArray(world.projects)) {
    issues.push('world.projects must be an array.')
  } else {
    if (world.projects.length > MAX_PROJECT_ENTRIES) {
      issues.push(`world.projects exceeds max entries ${MAX_PROJECT_ENTRIES}.`)
    }
    const seenProjectIds = new Set()
    for (const project of world.projects) {
      const normalizedProject = normalizeProjectShape(project)
      if (!normalizedProject) {
        issues.push('world.projects contains invalid project entry.')
        continue
      }
      const key = normalizedProject.id.toLowerCase()
      if (seenProjectIds.has(key)) issues.push(`world.projects has duplicate id ${normalizedProject.id}.`)
      seenProjectIds.add(key)
    }
  }
  if (!Array.isArray(world.salvageRuns)) {
    issues.push('world.salvageRuns must be an array.')
  } else {
    if (world.salvageRuns.length > MAX_SALVAGE_RUN_ENTRIES) {
      issues.push(`world.salvageRuns exceeds max entries ${MAX_SALVAGE_RUN_ENTRIES}.`)
    }
    const seenSalvageIds = new Set()
    for (const run of world.salvageRuns) {
      const normalizedRun = normalizeSalvageRunShape(run)
      if (!normalizedRun) {
        issues.push('world.salvageRuns contains invalid salvage entry.')
        continue
      }
      const key = normalizedRun.id.toLowerCase()
      if (seenSalvageIds.has(key)) issues.push(`world.salvageRuns has duplicate id ${normalizedRun.id}.`)
      seenSalvageIds.add(key)
    }
  }
  if (!world.towns || typeof world.towns !== 'object' || Array.isArray(world.towns)) {
    issues.push('world.towns must be an object.')
  } else {
    for (const [townName, townState] of Object.entries(world.towns)) {
      const safeTownName = asText(townName, '', 80)
      if (!safeTownName) {
        issues.push('world.towns contains invalid town name.')
        continue
      }
      if (!townState || typeof townState !== 'object' || Array.isArray(townState)) {
        issues.push(`world.towns.${safeTownName} must be an object.`)
        continue
      }
      const activeMajorMissionId = asText(townState.activeMajorMissionId, '', 200) || null
      const cooldown = Number(townState.majorMissionCooldownUntilDay)
      if (townState.activeMajorMissionId !== null && activeMajorMissionId === null) {
        issues.push(`world.towns.${safeTownName}.activeMajorMissionId must be string|null.`)
      }
      if (!Number.isInteger(cooldown) || cooldown < 0) {
        issues.push(`world.towns.${safeTownName}.majorMissionCooldownUntilDay must be integer >= 0.`)
      }
      if (!Array.isArray(townState.crierQueue)) {
        issues.push(`world.towns.${safeTownName}.crierQueue must be an array.`)
      } else {
        if (townState.crierQueue.length > MAX_TOWN_CRIER_QUEUE_ENTRIES) {
          issues.push(`world.towns.${safeTownName}.crierQueue exceeds max entries ${MAX_TOWN_CRIER_QUEUE_ENTRIES}.`)
        }
        for (const entry of townState.crierQueue) {
          if (!normalizeTownCrierEntryShape(entry)) {
            issues.push(`world.towns.${safeTownName}.crierQueue contains invalid entry.`)
          }
        }
      }
      const hope = Number(townState.hope)
      const dread = Number(townState.dread)
      if (!Number.isInteger(hope) || hope < TOWN_PRESSURE_MIN || hope > TOWN_PRESSURE_MAX) {
        issues.push(`world.towns.${safeTownName}.hope must be integer in [${TOWN_PRESSURE_MIN}..${TOWN_PRESSURE_MAX}].`)
      }
      if (!Number.isInteger(dread) || dread < TOWN_PRESSURE_MIN || dread > TOWN_PRESSURE_MAX) {
        issues.push(`world.towns.${safeTownName}.dread must be integer in [${TOWN_PRESSURE_MIN}..${TOWN_PRESSURE_MAX}].`)
      }
      if (!Array.isArray(townState.recentImpacts)) {
        issues.push(`world.towns.${safeTownName}.recentImpacts must be an array.`)
      } else {
        if (townState.recentImpacts.length > MAX_TOWN_RECENT_IMPACTS) {
          issues.push(`world.towns.${safeTownName}.recentImpacts exceeds max entries ${MAX_TOWN_RECENT_IMPACTS}.`)
        }
        for (const impact of townState.recentImpacts) {
          if (!normalizeTownImpactEntryShape(impact)) {
            issues.push(`world.towns.${safeTownName}.recentImpacts contains invalid entry.`)
          }
        }
      }
      const activeMissionIdForTown = activeMissionByTown.get(safeTownName.toLowerCase()) || null
      if (activeMissionIdForTown && activeMajorMissionId !== activeMissionIdForTown) {
        issues.push(`world.towns.${safeTownName}.activeMajorMissionId must match active major mission id.`)
      }
      if (!activeMissionIdForTown && activeMajorMissionId) {
        issues.push(`world.towns.${safeTownName}.activeMajorMissionId references non-active mission.`)
      }
    }
  }
  for (const [townKey, missionId] of activeMissionByTown.entries()) {
    const hasTownRecord = Object.entries(world.towns || {}).some(([townName, townState]) => {
      return asText(townName, '', 80).toLowerCase() === townKey
        && asText(townState?.activeMajorMissionId, '', 200) === missionId
    })
    if (!hasTownRecord) {
      issues.push(`world.towns is missing activeMajorMissionId mapping for town key ${townKey}.`)
    }
  }
  if (world.economy !== undefined) {
    if (!world.economy || typeof world.economy !== 'object' || Array.isArray(world.economy)) {
      issues.push('world.economy must be an object when present.')
    } else {
      if (asText(world.economy.currency, '', 30) !== 'emerald') {
        issues.push('world.economy.currency must be "emerald".')
      }
      const ledger = world.economy.ledger
      if (!ledger || typeof ledger !== 'object' || Array.isArray(ledger)) {
        issues.push('world.economy.ledger must be an object.')
      } else {
        for (const [agentName, balanceRaw] of Object.entries(ledger)) {
          if (!asText(agentName, '', 80)) issues.push('world.economy.ledger contains invalid agent name.')
          if (typeof balanceRaw !== 'number' || !Number.isFinite(balanceRaw) || balanceRaw < 0) {
            issues.push(`world.economy.ledger[${agentName || '?'}] must be finite and >= 0.`)
          }
        }
      }
      if (Object.prototype.hasOwnProperty.call(world.economy, 'minted_total')) {
        if (typeof world.economy.minted_total !== 'number'
          || !Number.isFinite(world.economy.minted_total)
          || world.economy.minted_total < 0) {
          issues.push('world.economy.minted_total must be finite and >= 0 when present.')
        }
      }
    }
  }

  return { ok: issues.length === 0, issues }
}

/**
 * @param {{
 *   filePath?: string,
 *   fsModule?: typeof fs,
 *   logger?: ReturnType<typeof createLogger>,
 *   now?: () => number,
 *   enableTxTimers?: boolean
 * }} options
 */
function createMemoryStore(options = {}) {
  const filePath = options.filePath || path.resolve(__dirname, './memory.json')
  const fsModule = options.fsModule || fs
  const fsPromises = (fsModule.promises && typeof fsModule.promises.open === 'function')
    ? fsModule.promises
    : fs.promises
  const logger = options.logger || createLogger({ component: 'memory' })
  const now = options.now || (() => Date.now())
  const enableTxTimers = typeof options.enableTxTimers === 'boolean'
    ? options.enableTxTimers
    : process.argv.includes('--timers')
  // Cross-process lock file used to serialize writers touching memory.json.
  const lockPath = `${filePath}.lock`
  const maxLockRetries = 5
  const simulateCrash = process.argv.includes('--simulate-crash')

  /** @type {MemoryState | null} */
  let state = null
  let txQueue = Promise.resolve()

  function loadFromDisk() {
    if (!fsModule.existsSync(filePath)) {
      state = freshMemoryShape(null)
      return state
    }

    try {
      const data = JSON.parse(fsModule.readFileSync(filePath, 'utf-8'))
      state = freshMemoryShape(data)
      return state
    } catch (err) {
      logger.warn('memory_load_failed_resetting', { filePath, error: err instanceof Error ? err.message : String(err) })
      state = freshMemoryShape(null)
      return state
    }
  }

  function ensureLoaded() {
    if (state) return state
    return loadFromDisk()
  }

  /**
   * @param {number} ms
   */
  function wait(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  async function loadFromDiskUnderLock() {
    try {
      const payload = await fsPromises.readFile(filePath, 'utf-8')
      return freshMemoryShape(JSON.parse(payload))
    } catch (err) {
      if (err && typeof err === 'object' && err.code === 'ENOENT') {
        return freshMemoryShape(null)
      }
      logger.warn('memory_load_failed_resetting', { filePath, error: err instanceof Error ? err.message : String(err) })
      return freshMemoryShape(null)
    }
  }

  async function acquireLockWithRetry() {
    // Small bounded backoff avoids hot-spinning when another process holds the lock.
    const startedAt = now()
    for (let attempt = 0; attempt <= maxLockRetries; attempt += 1) {
      try {
        const handle = await fsPromises.open(lockPath, 'wx')
        const elapsedMs = now() - startedAt
        recordLockAcquisition(elapsedMs)
        if (elapsedMs > 50) {
          logger.warn('SLOW_LOCK_ACQUISITION', { lockPath, elapsedMs, retries: attempt })
        }
        return handle
      } catch (err) {
        const isEexist = err && typeof err === 'object' && err.code === 'EEXIST'
        if (!isEexist) {
          throw new AppError({
            code: 'MEMORY_LOCK_FAILED',
            message: 'Failed to acquire memory lock.',
            recoverable: false,
            metadata: { lockPath, error: err instanceof Error ? err.message : String(err) }
          })
        }
        incrementMetric('lockRetries')
        if (attempt === maxLockRetries) {
          incrementMetric('lockTimeouts')
          throw new AppError({
            code: 'MEMORY_LOCK_TIMEOUT',
            message: 'Timed out acquiring memory lock.',
            recoverable: false,
            metadata: { lockPath, retries: maxLockRetries }
          })
        }
        await wait(15 * (attempt + 1))
      }
    }
    incrementMetric('lockTimeouts')
    throw new AppError({
      code: 'MEMORY_LOCK_TIMEOUT',
      message: 'Timed out acquiring memory lock.',
      recoverable: false,
      metadata: { lockPath, retries: maxLockRetries }
    })
  }

  async function withFileLock(fn) {
    const lockWaitStartedAt = now()
    const lockHandle = await acquireLockWithRetry()
    const lockWaitMs = now() - lockWaitStartedAt
    try {
      return await fn({ lockWaitMs })
    } finally {
      try {
        await lockHandle.close()
      } finally {
        await fsPromises.unlink(lockPath).catch(() => {})
      }
    }
  }

  async function persistSnapshotAtomically(snapshot, phaseDurations) {
    const stringifyStartedAt = now()
    const payload = JSON.stringify(snapshot, null, 2)
    if (phaseDurations) phaseDurations.stringifyMs = now() - stringifyStartedAt
    const tempPath = `${filePath}.${process.pid}.${Date.now()}.tmp`
    try {
      const writeStartedAt = now()
      await fsPromises.writeFile(tempPath, payload, 'utf-8')
      if (phaseDurations) phaseDurations.writeMs = now() - writeStartedAt
      // Rename on the same filesystem is atomic, so readers never observe partial JSON.
      const renameStartedAt = now()
      await fsPromises.rename(tempPath, filePath)
      if (phaseDurations) phaseDurations.renameMs = now() - renameStartedAt
    } catch (err) {
      await fsPromises.unlink(tempPath).catch(() => {})
      throw new AppError({
        code: 'MEMORY_WRITE_FAILED',
        message: 'Failed to persist memory state.',
        recoverable: false,
        metadata: { filePath, error: err instanceof Error ? err.message : String(err) }
      })
    }
  }

  /**
   * Serialize mutating transactions and commit only after successful persist.
   * @template T
   * @param {(memory: MemoryState) => T | Promise<T>} mutator
   * @param {{eventId?: string, persist?: boolean}} [opts]
   * @returns {Promise<{skipped: boolean, result: T | null}>}
   */
  function transact(mutator, opts = {}) {
    const run = async () => {
      const eventId = opts.eventId ? asText(opts.eventId, '', 200) : ''
      const startedAt = now()
      const phaseDurations = enableTxTimers
        ? {
          lockWaitMs: 0,
          cloneMs: 0,
          stringifyMs: 0,
          writeMs: 0,
          renameMs: 0,
          totalTxMs: 0
        }
        : null
      try {
        const txResult = await withFileLock(async ({ lockWaitMs }) => {
          if (phaseDurations) phaseDurations.lockWaitMs = lockWaitMs
          // Always reload inside the lock so each writer mutates the latest committed snapshot.
          const current = await loadFromDiskUnderLock()
          state = current

          if (eventId && hasEvent(current.world.processedEventIds, eventId)) {
            incrementMetric('duplicateEventsSkipped')
            incrementMetric('transactionsAborted')
            logger.warn(`DUPLICATE_EVENT_SKIPPED: ${eventId}`)
            return { skipped: true, result: null }
          }

          if (simulateCrash && Math.random() < 0.1) {
            throw new AppError({
              code: 'SIMULATED_CRASH',
              message: 'Simulated crash after lock acquisition before commit.',
              recoverable: false,
              metadata: { eventId }
            })
          }

          const cloneStartedAt = phaseDurations ? now() : 0
          const working = cloneMemory(current)
          if (phaseDurations) phaseDurations.cloneMs = now() - cloneStartedAt
          const result = await mutator(working)

          if (eventId) markEvent(working, eventId)
          if (opts.persist !== false) await persistSnapshotAtomically(working, phaseDurations)
          state = working

          incrementMetric('transactionsCommitted')
          if (eventId) incrementMetric('eventsProcessed')
          return { skipped: false, result }
        })

        const durationMs = now() - startedAt
        if (phaseDurations) phaseDurations.totalTxMs = durationMs
        recordTransactionDuration(durationMs, { isSlow: durationMs > 75, phaseDurations })
        if (durationMs > 75) logger.warn('SLOW_TRANSACTION', { durationMs, eventId: eventId || null })
        return txResult
      } catch (err) {
        incrementMetric('transactionsAborted')
        throw err
      }
    }

    const chained = txQueue.then(run, run)
    txQueue = chained.then(() => undefined, () => undefined)
    return chained
  }

  /**
   * @returns {MemoryState}
   */
  function getSnapshot() {
    return cloneMemory(ensureLoaded())
  }

  /**
   * @param {{reload?: boolean}} [opts]
   * @returns {MemoryState}
   */
  function loadAllMemory(opts = {}) {
    if (opts.reload) loadFromDisk()
    else ensureLoaded()
    return getSnapshot()
  }

  /**
   * @param {string} eventId
   * @returns {boolean}
   */
  function hasProcessedEvent(eventId) {
    const id = asText(eventId, '', 200)
    if (!id) return false
    return hasEvent(ensureLoaded().world.processedEventIds, id)
  }

  /**
   * @param {string} agent
   * @param {string} entry
   * @param {boolean} [important]
   * @param {string | undefined} [eventId]
   */
  async function rememberAgent(agent, entry, important = false, eventId) {
    const safeAgent = asText(agent, '', 80)
    const safeEntry = asText(entry, '', 500)
    if (!safeAgent || !safeEntry) {
      throw new AppError({
        code: 'INVALID_MEMORY_INPUT',
        message: 'Agent memory write rejected due to invalid input.',
        recoverable: true
      })
    }

    await transact((memory) => {
      initAgent(memory, safeAgent)
      memory.agents[safeAgent].short.push(safeEntry)
      if (memory.agents[safeAgent].short.length > 20) memory.agents[safeAgent].short.shift()

      if (important) {
        memory.agents[safeAgent].long.push(safeEntry)
        if (memory.agents[safeAgent].long.length % 20 === 0) {
          memory.agents[safeAgent].summary = summarize(memory.agents[safeAgent].long)
        }
      }

      memory.agents[safeAgent].archive.push({ time: now(), event: safeEntry })
    }, { eventId: eventId ? `${eventId}:agent:${safeAgent}` : undefined })
  }

  /**
   * @param {string} faction
   * @param {string} entry
   * @param {boolean} [important]
   * @param {string | undefined} [eventId]
   */
  async function rememberFaction(faction, entry, important = false, eventId) {
    const safeFaction = asText(faction, '', 80)
    const safeEntry = asText(entry, '', 500)
    if (!safeFaction || !safeEntry) {
      throw new AppError({
        code: 'INVALID_MEMORY_INPUT',
        message: 'Faction memory write rejected due to invalid input.',
        recoverable: true
      })
    }

    await transact((memory) => {
      initFaction(memory, safeFaction)
      memory.factions[safeFaction].long.push(safeEntry)
      if (important || memory.factions[safeFaction].long.length % 20 === 0) {
        memory.factions[safeFaction].summary = summarize(memory.factions[safeFaction].long)
      }
      memory.factions[safeFaction].archive.push({ time: now(), event: safeEntry })
    }, { eventId: eventId ? `${eventId}:faction:${safeFaction}` : undefined })
  }

  /**
   * @param {string} entry
   * @param {boolean} [important]
   * @param {string | undefined} [eventId]
   */
  async function rememberWorld(entry, important = false, eventId) {
    const safeEntry = asText(entry, '', 500)
    if (!safeEntry) {
      throw new AppError({
        code: 'INVALID_MEMORY_INPUT',
        message: 'World memory write rejected due to invalid input.',
        recoverable: true
      })
    }

    await transact((memory) => {
      memory.world.archive.push({ time: now(), event: safeEntry, important: !!important })
      if (memory.world.archive.length > 500) memory.world.archive = memory.world.archive.slice(-500)
    }, { eventId: eventId ? `${eventId}:world` : undefined })
  }

  /**
   * @param {string} agent
   */
  function recallAgent(agent) {
    const safeAgent = asText(agent, '', 80)
    if (!safeAgent) return null
    return getSnapshot().agents[safeAgent] || null
  }

  /**
   * @param {string} faction
   */
  function recallFaction(faction) {
    const safeFaction = asText(faction, '', 80)
    if (!safeFaction) return null
    return getSnapshot().factions[safeFaction] || null
  }

  function recallWorld() {
    return getSnapshot().world
  }

  async function saveAllMemory() {
    await transact(() => {}, { persist: true, eventId: undefined })
  }

  function validateMemoryIntegrity() {
    return validateMemoryIntegritySnapshot(getSnapshot())
  }

  return {
    loadAllMemory,
    saveAllMemory,
    getSnapshot,
    getRuntimeMetrics,
    validateMemoryIntegrity,
    transact,
    hasProcessedEvent,
    rememberAgent,
    rememberFaction,
    rememberWorld,
    recallAgent,
    recallFaction,
    recallWorld
  }
}

module.exports = { createMemoryStore, freshMemoryShape, validateMemoryIntegritySnapshot }
