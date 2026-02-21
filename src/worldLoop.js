const { createLogger } = require('./logger')
const { AppError } = require('./errors')
const { deriveOperationId } = require('./flowControl')
const { getObservabilitySnapshot } = require('./runtimeMetrics')

const INTENT_TYPES = new Set(['idle', 'wander', 'follow', 'respond'])
const WANDER_DIRECTIONS = ['north', 'east', 'south', 'west']
const RESPOND_LINES = [
  'Standing by.',
  'Holding this position.',
  'Copy that.'
]

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
 *   runtimeActions?: {
 *     onWander?: (input: {agent: any, direction: string, eventId: string, tickNumber: number}) => Promise<void> | void,
 *     onFollow?: (input: {agent: any, leaderName: string | null, eventId: string, tickNumber: number}) => Promise<void> | void,
 *     onRespond?: (input: {agent: any, message: string, eventId: string, tickNumber: number}) => Promise<void> | void
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
  const now = deps.now || (() => Date.now())
  const setIntervalFn = deps.setIntervalFn || setInterval
  const clearIntervalFn = deps.clearIntervalFn || clearInterval
  const runtimeActions = deps.runtimeActions || {}
  const observe = typeof deps.getObservabilitySnapshotFn === 'function'
    ? deps.getObservabilitySnapshotFn
    : getObservabilitySnapshot

  let timer = null
  let running = false
  let tickMs = 2000
  let lastTickAt = 0
  let scheduledCount = 0
  let backpressure = false
  let reason = 'stopped'
  let tickInFlight = false
  let tickNumber = 0
  let maxEventsPerTick = asPositiveInt(deps.maxEventsPerTick, 0, 0)
  let maxEventsPerAgentPerMin = asPositiveInt(deps.maxEventsPerAgentPerMin, 10, 1)
  /** @type {Record<string, number> | null} */
  let previousMetrics = null
  /** @type {ReturnType<typeof getObservabilitySnapshot> | null} */
  let previousObservability = null

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
      const target = intent === 'follow' ? (asText(input.target, '', 80) || asText(input.leaderName, '', 80) || null) : null

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
        await runtimeActions.onWander({ agent, direction, eventId: committed.eventId, tickNumber: context.tickNumber })
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
    const tickAt = now()
    lastTickAt = tickAt

    if (tickInFlight) {
      backpressure = true
      reason = 'tick_in_flight'
      scheduledCount = 0
      return { scheduled: 0, backpressure, reason }
    }

    tickInFlight = true
    tickNumber += 1
    try {
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

        /** @type {{intent: string, target: string | null, source: string}} */
        let planned
        if (worldIntent.frozen) {
          planned = { intent: 'idle', target: null, source: 'frozen' }
        } else if (worldIntent.manual_override && INTENT_TYPES.has(worldIntent.intent)) {
          planned = {
            intent: worldIntent.intent,
            target: worldIntent.intent === 'follow'
              ? (asText(worldIntent.intent_target, '', 80) || leaderName)
              : null,
            source: 'manual_override'
          }
        } else {
          const deterministic = pickDeterministicIntent(agentName, tickNumber, leaderName)
          planned = { intent: deterministic.intent, target: deterministic.target, source: 'deterministic' }
        }

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
      return { scheduled, backpressure, reason }
    } catch (err) {
      backpressure = true
      reason = 'tick_error'
      logger.errorWithStack('world_loop_tick_failed', err, { tickNumber })
      scheduledCount = 0
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
      backpressure,
      reason
    }
  }

  return {
    startWorldLoop,
    stopWorldLoop,
    getWorldLoopStatus,
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
