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
 *       rep?: Record<string, number>
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
 *       type: 'trade_n' | 'visit_town',
 *       state: 'offered' | 'accepted' | 'in_progress' | 'completed' | 'cancelled' | 'failed',
 *       town?: string,
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

const QUEST_TYPES = new Set(['trade_n', 'visit_town'])
const QUEST_STATES = new Set(['offered', 'accepted', 'in_progress', 'completed', 'cancelled', 'failed'])
const CLOCK_PHASES = new Set(['day', 'night'])
const CLOCK_SEASONS = new Set(['dawn', 'long_night'])
const STORY_FACTION_NAMES = ['iron_pact', 'veil_church']
const STORY_FACTION_NAME_SET = new Set(STORY_FACTION_NAMES)
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
    if (next.profile && typeof next.profile === 'object' && !Array.isArray(next.profile)) {
      const profile = { ...next.profile }
      profile.rep = normalizeAgentRepShape(profile.rep)
      next.profile = profile
    }
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
 * @param {'trade_n' | 'visit_town'} type
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
  const town = asText(questInput.town, '', 80)
  const offeredAt = normalizeIsoDateText(questInput.offered_at)
  const acceptedAt = normalizeIsoDateText(questInput.accepted_at)
  const owner = asText(questInput.owner, '', 80)
  const reward = Number(questInput.reward)
  const title = asText(questInput.title, '', 120)
  const desc = asText(questInput.desc, '', 120)
  const meta = normalizeFeedMetaShape(questInput.meta)

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
  if (town) quest.town = town
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
 * @param {Partial<MemoryState> | null | undefined} input
 * @returns {MemoryState}
 */
function freshMemoryShape(input) {
  const source = input || {}
  return {
    agents: normalizeAgentsShape(source.agents),
    factions: source.factions || {},
    world: {
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
      quests: normalizeQuestsShape(source.world?.quests),
      archive: Array.isArray(source.world?.archive) ? source.world.archive : [],
      processedEventIds: Array.isArray(source.world?.processedEventIds) ? source.world.processedEventIds : []
    }
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
    for (const quest of world.quests) {
      if (!normalizeQuestShape(quest)) {
        issues.push('world.quests contains invalid quest entry.')
      }
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
