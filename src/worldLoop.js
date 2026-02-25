const { createLogger } = require('./logger')
const { AppError } = require('./errors')
const { deriveOperationId } = require('./flowControl')
const { getObservabilitySnapshot } = require('./runtimeMetrics')

const INTENT_TYPES = new Set(['idle', 'wander', 'follow', 'respond'])
const JOB_ROLES = new Set(['scout', 'guard', 'builder', 'farmer', 'hauler'])
const REPETITION_BREAK_LIMIT = 10
const WANDER_DIRECTIONS = ['north', 'east', 'south', 'west']
const RESPOND_LINES = [
  'Standing by.',
  'Holding this position.',
  'Copy that.'
]
const DETERMINISTIC_TIME_EPOCH_MS = Date.parse('2026-01-01T00:00:00.000Z')

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
 * @param {unknown} value
 * @param {number} fallback
 * @param {number} min
 */
function asPositiveInt(value, fallback, min = 1) {
  const n = Number(value)
  if (!Number.isInteger(n) || n < min) return fallback
  return n
}

/**
 * @param {unknown} value
 * @param {boolean} fallback
 */
function asBooleanFlag(value, fallback) {
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase()
    if (!normalized) return false
    if (normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on') return true
    if (normalized === '0' || normalized === 'false' || normalized === 'no' || normalized === 'off') return false
  }
  return fallback
}

/**
 * @param {unknown} value
 */
function asNumber(value) {
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

/**
 * @param {string} text
 */
function stableHash(text) {
  let hash = 0
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) - hash) + text.charCodeAt(i)
    hash |= 0
  }
  return Math.abs(hash)
}

/**
 * @param {unknown} entry
 */
function normalizeMarker(entry) {
  if (!entry || typeof entry !== 'object') return null
  const name = asText(entry.name, '', 80)
  const x = asNumber(entry.x)
  const y = asNumber(entry.y)
  const z = asNumber(entry.z)
  if (!name || x === null || y === null || z === null) return null
  return { name, x, y, z }
}

/**
 * @param {any[]} markers
 */
function collectMarkers(markers) {
  return (Array.isArray(markers) ? markers : [])
    .map(normalizeMarker)
    .filter(Boolean)
    .sort((a, b) => a.name.localeCompare(b.name))
}

/**
 * @param {ReturnType<typeof collectMarkers>} markers
 * @param {string | null} markerName
 */
function findMarkerByName(markers, markerName) {
  const target = asText(markerName, '', 80).toLowerCase()
  if (!target) return null
  return markers.find(marker => marker.name.toLowerCase() === target) || null
}

/**
 * @param {any} agent
 */
function hasPendingChat(agent) {
  if (!agent || typeof agent !== 'object') return false
  const direct = [
    agent.pendingPlayerMessage,
    agent.pendingChatMessage,
    agent.pendingMessage
  ]
  if (direct.some(msg => asText(msg, '', 240))) return true
  if (Array.isArray(agent.pendingMessages)) {
    for (const msg of agent.pendingMessages) {
      if (asText(msg, '', 240)) return true
    }
  }
  return false
}

/**
 * @param {ReturnType<typeof normalizeWorldIntent>} worldIntent
 * @param {number} tickAt
 * @param {number} maxEventsPerAgentPerMin
 */
function canScheduleOnBudget(worldIntent, tickAt, maxEventsPerAgentPerMin) {
  const minuteBucket = Math.floor(tickAt / 60000)
  const eventsInMin = worldIntent.budgets.minute_bucket === minuteBucket
    ? worldIntent.budgets.events_in_min
    : 0
  return eventsInMin < maxEventsPerAgentPerMin
}

/**
 * @param {string} agentName
 * @param {number} tickNumber
 * @param {string | null} leaderName
 * @param {ReturnType<typeof normalizeWorldIntent>} worldIntent
 * @param {Record<string, unknown>} profile
 * @param {ReturnType<typeof collectMarkers>} markers
 * @param {Map<string, number>} haulerToggleByAgent
 */
function pickJobDrivenIntent(agentName, tickNumber, leaderName, worldIntent, profile, markers, haulerToggleByAgent) {
  const role = asText(profile?.job?.role, '', 20).toLowerCase()
  if (!JOB_ROLES.has(role)) return null
  const agentKey = asText(agentName, '', 80).toLowerCase()
  const homeMarkerName = asText(profile?.job?.home_marker, '', 80) || null
  const homeMarker = findMarkerByName(markers, homeMarkerName)

  if (role === 'scout') {
    if (tickNumber % 6 === 0) return { intent: 'respond', target: null, source: 'job:scout_report' }
    return { intent: 'wander', target: null, source: 'job:scout_wander' }
  }

  if (role === 'guard') {
    if (homeMarker) return { intent: 'wander', target: homeMarker.name, source: 'job:guard_patrol' }
    if (!worldIntent.is_leader && leaderName && leaderName !== agentName) {
      return { intent: 'follow', target: leaderName, source: 'job:guard_follow' }
    }
    return { intent: 'idle', target: null, source: 'job:guard_hold' }
  }

  if (role === 'builder') {
    if (homeMarker) return { intent: 'follow', target: homeMarker.name, source: 'job:builder_marker' }
    return { intent: 'wander', target: null, source: 'job:builder_roam' }
  }

  if (role === 'farmer') {
    if (homeMarker) return { intent: 'idle', target: homeMarker.name, source: 'job:farmer_marker' }
    return { intent: 'wander', target: null, source: 'job:farmer_roam' }
  }

  if (role === 'hauler') {
    if (markers.length >= 2) {
      const idx = Number(haulerToggleByAgent.get(agentKey) || 0) % 2
      haulerToggleByAgent.set(agentKey, (idx + 1) % 2)
      const targetMarker = markers[idx]
      return { intent: 'follow', target: targetMarker.name, source: 'job:hauler_route' }
    }
    return { intent: 'wander', target: null, source: 'job:hauler_roam' }
  }

  return null
}

/**
 * @param {Map<string, {key: string, count: number}>} repetitionByAgent
 * @param {string} agentName
 * @param {{intent: string, target: string | null, source: string}} planned
 */
function applyRepetitionBreaker(repetitionByAgent, agentName, planned) {
  const agentKey = asText(agentName, '', 80).toLowerCase()
  if (!agentKey) return { planned, repetitionCount: 0, broke: false }

  const trackedTarget = asText(planned.target, '', 80) || '-'
  const key = `${planned.intent}|${trackedTarget}`
  const current = repetitionByAgent.get(agentKey) || { key: '', count: 0 }
  const nextCount = current.key === key ? (current.count + 1) : 1

  const canBreak = planned.source !== 'frozen'
    && planned.source !== 'manual_override'
    && planned.source !== 'pending_chat'
    && planned.source !== 'budget_guard'

  if (canBreak && nextCount >= REPETITION_BREAK_LIMIT) {
    repetitionByAgent.set(agentKey, { key: '', count: 0 })
    const fallbackIntent = planned.intent === 'wander' ? 'idle' : 'wander'
    return {
      planned: { intent: fallbackIntent, target: null, source: 'repetition_breaker' },
      repetitionCount: 0,
      broke: true
    }
  }

  repetitionByAgent.set(agentKey, { key, count: nextCount })
  return { planned, repetitionCount: nextCount, broke: false }
}

/**
 * @param {Record<string, unknown>} profile
 */
function normalizeWorldIntent(profile) {
  const current = (profile && typeof profile.world_intent === 'object' && profile.world_intent)
    ? profile.world_intent
    : {}
  const budgets = (current && typeof current.budgets === 'object' && current.budgets)
    ? current.budgets
    : {}
  const intent = asText(current.intent, 'idle', 16)
  return {
    intent: INTENT_TYPES.has(intent) ? intent : 'idle',
    intent_target: asText(current.intent_target, '', 80) || null,
    intent_set_at: Number(current.intent_set_at || 0) || 0,
    last_action: asText(current.last_action, '', 120),
    last_action_at: Number(current.last_action_at || 0) || 0,
    manual_override: !!current.manual_override,
    frozen: !!current.frozen,
    is_leader: !!current.is_leader,
    budgets: {
      minute_bucket: asPositiveInt(budgets.minute_bucket, 0, 0),
      events_in_min: asPositiveInt(budgets.events_in_min, 0, 0)
    }
  }
}

/**
 * @param {ReturnType<import('./memory').createMemoryStore>} memoryStore
 * @param {string} agentName
 */
function ensureAgentProfile(memoryStore, agentName) {
  const memory = memoryStore
  if (!memory.agents[agentName]) {
    memory.agents[agentName] = {
      short: [],
      long: [],
      summary: '',
      archive: [],
      recentUtterances: [],
      lastProcessedTime: 0
    }
  }
  if (!memory.agents[agentName].profile || typeof memory.agents[agentName].profile !== 'object') {
    memory.agents[agentName].profile = {}
  }
  return memory.agents[agentName].profile
}

/**
 * @param {Array<{name: string}>} agents
 * @param {ReturnType<import('./memory').createMemoryStore>['getSnapshot']} getSnapshot
 */
function resolveLeaderName(agents, getSnapshot) {
  const snapshot = getSnapshot()
  const onlineNames = new Set(agents.map(agent => asText(agent?.name, '', 80)).filter(Boolean))
  const leaders = []
  for (const [name, record] of Object.entries(snapshot.agents || {})) {
    const profile = record && typeof record.profile === 'object' ? record.profile : null
    const worldIntent = profile ? normalizeWorldIntent(profile) : null
    if (worldIntent?.is_leader && onlineNames.has(name)) leaders.push(name)
  }
  leaders.sort((a, b) => a.localeCompare(b))
  return leaders[0] || null
}

/**
 * @param {string} agentName
 * @param {number} tickNumber
 * @param {string | null} leaderName
 */
function pickDeterministicIntent(agentName, tickNumber, leaderName) {
  const options = ['idle', 'wander', 'respond']
  if (leaderName && leaderName !== agentName) options.push('follow')
  const hash = stableHash(`${agentName}:${tickNumber}`)
  const intent = options[hash % options.length]
  return {
    intent,
    target: intent === 'follow' ? leaderName : null
  }
}

/**
 * @param {string} agentName
 * @param {number} tickNumber
 * @param {string} eventId
 */
function deterministicDirection(agentName, tickNumber, eventId) {
  const idx = stableHash(`${agentName}:${tickNumber}:${eventId}`) % WANDER_DIRECTIONS.length
  return WANDER_DIRECTIONS[idx]
}

/**
 * @param {string} agentName
 * @param {number} tickNumber
 * @param {string} eventId
 */
function deterministicResponse(agentName, tickNumber, eventId) {
  const idx = stableHash(`${agentName}:${tickNumber}:${eventId}`) % RESPOND_LINES.length
  return RESPOND_LINES[idx]
}

/**
 * @param {ReturnType<import('./memory').createMemoryStore>} memoryStore
 * @param {ReturnType<typeof getObservabilitySnapshot>} observability
 * @param {Record<string, number> | null} previousMetrics
 * @param {ReturnType<typeof getObservabilitySnapshot> | null} previousObservability
 */
function evaluateBackpressure(memoryStore, observability, previousMetrics, previousObservability) {
  const metrics = memoryStore.getRuntimeMetrics()
  if (metrics.lockTimeouts > 0) {
    return { active: true, reason: 'lock_timeouts_detected', metrics, observability }
  }

  const lockRetryDelta = previousMetrics ? (metrics.lockRetries - Number(previousMetrics.lockRetries || 0)) : 0
  if (lockRetryDelta >= 3) {
    return { active: true, reason: `lock_retry_spike:${lockRetryDelta}`, metrics, observability }
  }

  const avgTxMs = observability.txDurationCount > 0
    ? observability.txDurationTotalMs / observability.txDurationCount
    : 0
  if (observability.txDurationP99Ms > 250) {
    return { active: true, reason: `high_p99_tx:${observability.txDurationP99Ms.toFixed(2)}ms`, metrics, observability }
  }
  if (avgTxMs > 120) {
    return { active: true, reason: `high_avg_tx:${avgTxMs.toFixed(2)}ms`, metrics, observability }
  }

  if (previousObservability) {
    const prevAvgTx = previousObservability.txDurationCount > 0
      ? previousObservability.txDurationTotalMs / previousObservability.txDurationCount
      : 0
    const prevP99 = Number(previousObservability.txDurationP99Ms || 0)
    if (prevP99 > 0 && observability.txDurationP99Ms > prevP99 * 1.3 && observability.txDurationP99Ms > 100) {
      return { active: true, reason: 'rising_p99_tx', metrics, observability }
    }
    if (prevAvgTx > 0 && avgTxMs > prevAvgTx * 1.3 && avgTxMs > 80) {
      return { active: true, reason: 'rising_avg_tx', metrics, observability }
    }
  }

  return { active: false, reason: 'ok', metrics, observability }
}

/**
 * @param {{
 *   memoryStore: ReturnType<import('./memory').createMemoryStore>,
 *   getAgents: () => unknown[],
 *   logger?: ReturnType<typeof createLogger>,
 *   now?: () => number,
 *   setIntervalFn?: typeof setInterval,
 *   clearIntervalFn?: typeof clearInterval,
 *   maxEventsPerAgentPerMin?: number,
 *   maxEventsPerTick?: number,
 *   townCrierEnabled?: boolean,
 *   townCrierIntervalMs?: number,
 *   townCrierMaxPerTick?: number,
 *   townCrierRecentWindow?: number,
 *   townCrierDedupeWindow?: number,
 *   env?: Record<string, string | undefined>,
 *   runtimeActions?: {
 *     onWander?: (input: {agent: any, direction: string, eventId: string, tickNumber: number, target?: string | null, source?: string}) => Promise<void> | void,
 *     onFollow?: (input: {agent: any, leaderName: string | null, eventId: string, tickNumber: number}) => Promise<void> | void,
 *     onRespond?: (input: {agent: any, message: string, eventId: string, tickNumber: number}) => Promise<void> | void,
 *     onNews?: (input: {line: string, id: string, msg: string, town: string | null, eventId: string, tickNumber: number}) => Promise<void> | void
 *   },
 *   getObservabilitySnapshotFn?: () => ReturnType<typeof getObservabilitySnapshot>
 * }} deps
 */
function createWorldLoop(deps) {
  if (!deps?.memoryStore || typeof deps.getAgents !== 'function') {
    throw new AppError({
      code: 'WORLD_LOOP_CONFIG_ERROR',
      message: 'world loop requires memoryStore and getAgents dependencies.',
      recoverable: false
    })
  }

  const memoryStore = deps.memoryStore
  const getAgents = deps.getAgents
  const logger = deps.logger || createLogger({ component: 'world_loop' })
  const runtimeNow = deps.now || (() => Date.now())
  const setIntervalFn = deps.setIntervalFn || setInterval
  const clearIntervalFn = deps.clearIntervalFn || clearInterval
  const runtimeActions = deps.runtimeActions || {}
  const env = (deps.env && typeof deps.env === 'object') ? deps.env : process.env
  const observe = typeof deps.getObservabilitySnapshotFn === 'function'
    ? deps.getObservabilitySnapshotFn
    : getObservabilitySnapshot
  const townCrierEnabled = asBooleanFlag(
    deps.townCrierEnabled,
    asBooleanFlag(env.TOWN_CRIER_ENABLED, false)
  )
  const townCrierIntervalMs = asPositiveInt(
    deps.townCrierIntervalMs,
    asPositiveInt(env.TOWN_CRIER_INTERVAL_MS, 15000, 1),
    1
  )
  const townCrierMaxPerTick = asPositiveInt(
    deps.townCrierMaxPerTick,
    asPositiveInt(env.TOWN_CRIER_MAX_PER_TICK, 1, 1),
    1
  )
  const townCrierRecentWindow = asPositiveInt(
    deps.townCrierRecentWindow,
    asPositiveInt(env.TOWN_CRIER_RECENT_WINDOW, 25, 1),
    1
  )
  const townCrierDedupeWindow = asPositiveInt(
    deps.townCrierDedupeWindow,
    asPositiveInt(env.TOWN_CRIER_DEDUPE_WINDOW, 100, 1),
    1
  )

  let timer = null
  let running = false
  let tickMs = 2000
  let lastTickAt = 0
  let scheduledCount = 0
  let backpressure = false
  let reason = 'stopped'
  let tickInFlight = false
  let tickNumber = 0
  let tickDurationTotalMs = 0
  let tickDurationCount = 0
  let tickDurationMaxMs = 0
  let lastTickDurationMs = 0
  let maxEventsPerTick = asPositiveInt(deps.maxEventsPerTick, 0, 0)
  let maxEventsPerAgentPerMin = asPositiveInt(deps.maxEventsPerAgentPerMin, 10, 1)
  /** @type {Record<string, number> | null} */
  let previousMetrics = null
  /** @type {ReturnType<typeof getObservabilitySnapshot> | null} */
  let previousObservability = null
  /** @type {Map<string, number>} */
  const haulerToggleByAgent = new Map()
  /** @type {Map<string, {key: string, count: number}>} */
  const repetitionByAgent = new Map()
  /** @type {Map<string, {intent: string | null, target: string | null, repetitionCount: number}>} */
  const selectedIntentByAgent = new Map()
  let intentsSelectedTotal = 0
  let fallbackBreaksTotal = 0
  /** @type {string[]} */
  const townCrierDedupeRing = []
  const townCrierDedupeSet = new Set()
  let townCrierLastBroadcastAt = 0
  let townCrierBroadcastsTotal = 0

  function deterministicTickAtMs(currentTickNumber) {
    const safeTick = Math.max(1, Math.trunc(Number(currentTickNumber) || 1))
    const safeTickMs = Math.max(1, Math.trunc(Number(tickMs) || 1))
    return DETERMINISTIC_TIME_EPOCH_MS + (safeTick * safeTickMs)
  }

  function resetTownCrierRuntimeState() {
    townCrierDedupeRing.length = 0
    townCrierDedupeSet.clear()
    townCrierLastBroadcastAt = 0
  }

  /**
   * @param {string} newsId
   */
  function rememberTownCrierNewsId(newsId) {
    townCrierDedupeSet.add(newsId)
    townCrierDedupeRing.push(newsId)
    if (townCrierDedupeRing.length > townCrierDedupeWindow) {
      const evicted = townCrierDedupeRing.shift()
      if (evicted) townCrierDedupeSet.delete(evicted)
    }
  }

  /**
   * @param {number} tickAt
   * @param {number} tickNo
   */
  async function maybeBroadcastTownCrierNews(tickAt, tickNo) {
    if (!townCrierEnabled || !running) return 0
    if ((tickAt - townCrierLastBroadcastAt) < townCrierIntervalMs) return 0

    const snapshot = memoryStore.getSnapshot()
    const news = Array.isArray(snapshot.world?.news) ? snapshot.world.news : []
    if (news.length === 0) return 0

    const recent = news.slice(-townCrierRecentWindow)
    /** @type {Array<{id: string, msg: string, town: string | null}>} */
    const candidates = []
    for (let idx = recent.length - 1; idx >= 0 && candidates.length < townCrierMaxPerTick; idx -= 1) {
      const entry = recent[idx]
      const id = asText(entry?.id, '', 200)
      const msg = asText(entry?.msg, '', 240)
      const town = asText(entry?.town, '', 80) || null
      if (!id || !msg) continue
      if (townCrierDedupeSet.has(id)) continue
      candidates.push({ id, msg, town })
    }

    if (candidates.length === 0) return 0

    for (const candidate of candidates) {
      const line = candidate.town
        ? `[NEWS:${candidate.town}] ${candidate.msg}`
        : `[NEWS] ${candidate.msg}`
      if (typeof runtimeActions.onNews === 'function') {
        await runtimeActions.onNews({
          line,
          id: candidate.id,
          msg: candidate.msg,
          town: candidate.town,
          eventId: `town_crier:${candidate.id}`,
          tickNumber: tickNo
        })
      } else {
        logger.info('town_crier_news', { line, newsId: candidate.id, town: candidate.town })
      }
      rememberTownCrierNewsId(candidate.id)
      townCrierBroadcastsTotal += 1
    }
    townCrierLastBroadcastAt = tickAt
    return candidates.length
  }

  function getOnlineAgents() {
    const agents = Array.isArray(getAgents()) ? getAgents() : []
    return agents
      .filter(agent => !!agent && typeof agent.name === 'string')
      .sort((a, b) => asText(a.name, '', 80).localeCompare(asText(b.name, '', 80)))
  }

  /**
   * @param {any} agent
   * @param {{
   *   tickAt: number,
   *   tickNumber: number,
   *   leaderName: string | null,
   *   intent: string,
   *   target: string | null,
   *   source: string,
   *   eventId: string
   * }} input
   */
  async function persistScheduledIntent(agent, input) {
    const agentName = asText(agent?.name, '', 80)
    const eventId = asText(input.eventId, '', 200)
    if (!agentName || !eventId) {
      return { applied: false, reason: 'invalid_intent_event' }
    }

    const tx = await memoryStore.transact((memory) => {
      const profile = ensureAgentProfile(memory, agentName)
      const worldIntent = normalizeWorldIntent(profile)
      const minuteBucket = Math.floor(input.tickAt / 60000)
      const budgets = {
        minute_bucket: worldIntent.budgets.minute_bucket === minuteBucket ? minuteBucket : minuteBucket,
        events_in_min: worldIntent.budgets.minute_bucket === minuteBucket ? worldIntent.budgets.events_in_min : 0
      }
      if (budgets.events_in_min >= maxEventsPerAgentPerMin) {
        return { applied: false, reason: 'agent_budget_exceeded', agentName, eventId }
      }

      const intent = INTENT_TYPES.has(input.intent) ? input.intent : 'idle'
      const explicitTarget = asText(input.target, '', 80) || null
      const target = intent === 'follow'
        ? (explicitTarget || asText(input.leaderName, '', 80) || null)
        : explicitTarget

      budgets.events_in_min += 1
      worldIntent.intent = intent
      worldIntent.intent_target = target
      worldIntent.intent_set_at = input.tickAt
      worldIntent.last_action = `scheduled:${intent}`
      worldIntent.last_action_at = input.tickAt
      worldIntent.budgets = budgets
      profile.world_intent = worldIntent

      return { applied: true, agentName, intent, target, eventId, source: input.source }
    }, { eventId })

    if (tx.skipped) return { applied: false, reason: 'duplicate_event', agentName, eventId }
    return tx.result || { applied: false, reason: 'unknown_result', agentName, eventId }
  }

  /**
   * @param {any} agent
   * @param {{
   *   applied: boolean,
   *   agentName: string,
 *   intent: string,
 *   target: string | null,
 *   source?: string,
 *   eventId: string
 * }} committed
 * @param {{tickNumber: number, leaderName: string | null}} context
 */
  async function applyRuntimeIntent(agent, committed, context) {
    if (!committed.applied) return
    if (committed.intent === 'idle') return

    if (committed.intent === 'wander') {
      const direction = deterministicDirection(committed.agentName, context.tickNumber, committed.eventId)
      if (typeof runtimeActions.onWander === 'function') {
        await runtimeActions.onWander({
          agent,
          direction,
          eventId: committed.eventId,
          tickNumber: context.tickNumber,
          target: asText(committed.target, '', 80) || null,
          source: asText(committed.source, '', 80)
        })
      }
      return
    }

    if (committed.intent === 'follow') {
      const leaderName = asText(committed.target, '', 80) || context.leaderName || null
      if (typeof runtimeActions.onFollow === 'function') {
        await runtimeActions.onFollow({ agent, leaderName, eventId: committed.eventId, tickNumber: context.tickNumber })
      }
      return
    }

    if (committed.intent === 'respond') {
      const message = deterministicResponse(committed.agentName, context.tickNumber, committed.eventId)
      if (typeof runtimeActions.onRespond === 'function') {
        await runtimeActions.onRespond({ agent, message, eventId: committed.eventId, tickNumber: context.tickNumber })
      }
    }
  }

  /**
   * Run one deterministic tick. Exposed to tests to avoid flaky timer-based assertions.
   */
  async function runTickOnce() {
    const tickStartedAt = runtimeNow()

    if (tickInFlight) {
      backpressure = true
      reason = 'tick_in_flight'
      scheduledCount = 0
      return { scheduled: 0, backpressure, reason }
    }

    tickInFlight = true
    tickNumber += 1
    try {
      const tickAt = deterministicTickAtMs(tickNumber)
      lastTickAt = tickAt
      const observability = observe()
      const pressure = evaluateBackpressure(memoryStore, observability, previousMetrics, previousObservability)
      previousMetrics = pressure.metrics
      previousObservability = pressure.observability

      if (pressure.active) {
        backpressure = true
        reason = pressure.reason
        scheduledCount = 0
        return { scheduled: 0, backpressure, reason }
      }

      backpressure = false
      reason = 'ok'

      const onlineAgents = getOnlineAgents()
      const tickBudget = maxEventsPerTick > 0
        ? Math.min(maxEventsPerTick, onlineAgents.length)
        : onlineAgents.length

      const leaderName = resolveLeaderName(onlineAgents, memoryStore.getSnapshot)
      let scheduled = 0
      for (let i = 0; i < tickBudget; i += 1) {
        const agent = onlineAgents[i]
        const agentName = asText(agent?.name, '', 80)
        if (!agentName) continue

        const snapshot = memoryStore.getSnapshot()
        const profile = snapshot.agents?.[agentName]?.profile || {}
        const worldIntent = normalizeWorldIntent(profile)
        const markers = collectMarkers(snapshot.world?.markers)
        const budgetAllows = canScheduleOnBudget(worldIntent, tickAt, maxEventsPerAgentPerMin)

        /** @type {{intent: string, target: string | null, source: string}} */
        let planned
        if (worldIntent.frozen) {
          planned = { intent: 'idle', target: null, source: 'frozen' }
        } else if (worldIntent.manual_override && INTENT_TYPES.has(worldIntent.intent)) {
          planned = {
            intent: worldIntent.intent,
            target: worldIntent.intent === 'follow'
              ? (asText(worldIntent.intent_target, '', 80) || leaderName || null)
              : (asText(worldIntent.intent_target, '', 80) || null),
            source: 'manual_override'
          }
        } else if (hasPendingChat(agent)) {
          planned = { intent: 'respond', target: null, source: 'pending_chat' }
        } else if (!budgetAllows) {
          planned = { intent: 'idle', target: null, source: 'budget_guard' }
        } else {
          const jobPlan = pickJobDrivenIntent(
            agentName,
            tickNumber,
            leaderName,
            worldIntent,
            profile,
            markers,
            haulerToggleByAgent
          )
          if (jobPlan) {
            planned = jobPlan
          } else {
            const deterministic = pickDeterministicIntent(agentName, tickNumber, leaderName)
            planned = { intent: deterministic.intent, target: deterministic.target, source: 'deterministic' }
          }
        }

        const repetition = applyRepetitionBreaker(repetitionByAgent, agentName, planned)
        planned = repetition.planned
        if (repetition.broke) fallbackBreaksTotal += 1
        intentsSelectedTotal += 1
        selectedIntentByAgent.set(agentName.toLowerCase(), {
          intent: planned.intent,
          target: asText(planned.target, '', 80) || null,
          repetitionCount: repetition.repetitionCount
        })

        const opId = deriveOperationId(
          ['world_loop', tickNumber, agentName, planned.intent, planned.target || 'none'],
          { windowMs: 1, now: () => tickAt + i }
        )
        const eventId = `${opId}:world_loop_intent`
        const committed = await persistScheduledIntent(agent, {
          tickAt,
          tickNumber,
          leaderName,
          intent: planned.intent,
          target: planned.target,
          source: planned.source,
          eventId
        })
        if (!committed.applied) continue

        // Runtime side effects only execute after durable commit to avoid drift.
        await applyRuntimeIntent(agent, committed, { tickNumber, leaderName })
        scheduled += 1
      }

      scheduledCount = scheduled
      await maybeBroadcastTownCrierNews(tickAt, tickNumber)
      lastTickDurationMs = runtimeNow() - tickStartedAt
      tickDurationTotalMs += lastTickDurationMs
      tickDurationCount += 1
      tickDurationMaxMs = Math.max(tickDurationMaxMs, lastTickDurationMs)
      return { scheduled, backpressure, reason }
    } catch (err) {
      backpressure = true
      reason = 'tick_error'
      logger.errorWithStack('world_loop_tick_failed', err, { tickNumber })
      scheduledCount = 0
      lastTickDurationMs = runtimeNow() - tickStartedAt
      tickDurationTotalMs += lastTickDurationMs
      tickDurationCount += 1
      tickDurationMaxMs = Math.max(tickDurationMaxMs, lastTickDurationMs)
      return { scheduled: 0, backpressure, reason }
    } finally {
      tickInFlight = false
    }
  }

  /**
   * @param {{tickMs?: number, maxEventsPerTick?: number, maxEventsPerAgentPerMin?: number}} [options]
   */
  function startWorldLoop(options = {}) {
    const requestedTickMs = asPositiveInt(options.tickMs, tickMs, 100)
    const requestedMaxPerTick = asPositiveInt(options.maxEventsPerTick, maxEventsPerTick, 0)
    const requestedMaxPerAgentPerMin = asPositiveInt(options.maxEventsPerAgentPerMin, maxEventsPerAgentPerMin, 1)

    tickMs = requestedTickMs
    maxEventsPerTick = requestedMaxPerTick
    maxEventsPerAgentPerMin = requestedMaxPerAgentPerMin

    if (running) return getWorldLoopStatus()

    running = true
    resetTownCrierRuntimeState()
    reason = 'running'
    timer = setIntervalFn(() => {
      void runTickOnce()
    }, tickMs)
    if (timer && typeof timer.unref === 'function') timer.unref()
    return getWorldLoopStatus()
  }

  function stopWorldLoop() {
    if (timer) {
      clearIntervalFn(timer)
      timer = null
    }
    running = false
    resetTownCrierRuntimeState()
    backpressure = false
    reason = 'stopped'
    return getWorldLoopStatus()
  }

  function getWorldLoopStatus() {
    return {
      running,
      tickMs,
      lastTickAt,
      scheduledCount,
      intentsSelectedTotal,
      fallbackBreaksTotal,
      tickCount: tickDurationCount,
      lastTickDurationMs,
      avgTickDurationMs: tickDurationCount > 0 ? (tickDurationTotalMs / tickDurationCount) : 0,
      maxTickDurationMs: tickDurationMaxMs,
      townCrierEnabled,
      townCrierActive: townCrierEnabled && running,
      townCrierIntervalMs,
      townCrierMaxPerTick,
      townCrierRecentWindow,
      townCrierDedupeWindow,
      townCrierDedupeSize: townCrierDedupeRing.length,
      townCrierBroadcastsTotal,
      townCrierLastBroadcastAt,
      backpressure,
      reason
    }
  }

  /**
   * @param {string} agentName
   */
  function getAgentRuntimeState(agentName) {
    const key = asText(agentName, '', 80).toLowerCase()
    if (!key) return { repetitionCount: 0, selectedIntent: null, selectedTarget: null }
    const selected = selectedIntentByAgent.get(key)
    const repetition = repetitionByAgent.get(key)
    return {
      repetitionCount: Number(selected?.repetitionCount ?? repetition?.count ?? 0),
      selectedIntent: selected?.intent || null,
      selectedTarget: selected?.target || null
    }
  }

  return {
    startWorldLoop,
    stopWorldLoop,
    getWorldLoopStatus,
    getAgentRuntimeState,
    runTickOnce
  }
}

/** @type {ReturnType<typeof createWorldLoop> | null} */
let defaultWorldLoop = null

function configureWorldLoop(deps) {
  defaultWorldLoop = createWorldLoop(deps)
  return defaultWorldLoop
}

function getDefaultWorldLoop() {
  if (defaultWorldLoop) return defaultWorldLoop
  throw new AppError({
    code: 'WORLD_LOOP_NOT_CONFIGURED',
    message: 'World loop has not been configured.',
    recoverable: false
  })
}

/**
 * @param {{tickMs?: number, maxEventsPerTick?: number, maxEventsPerAgentPerMin?: number}} [options]
 */
function startWorldLoop(options) {
  return getDefaultWorldLoop().startWorldLoop(options)
}

function stopWorldLoop() {
  return getDefaultWorldLoop().stopWorldLoop()
}

function getWorldLoopStatus() {
  return getDefaultWorldLoop().getWorldLoopStatus()
}

module.exports = {
  createWorldLoop,
  configureWorldLoop,
  startWorldLoop,
  stopWorldLoop,
  getWorldLoopStatus
}
