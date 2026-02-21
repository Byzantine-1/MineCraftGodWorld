const { createLogger } = require('./logger')
const { AppError } = require('./errors')
const { getObservabilitySnapshot } = require('./runtimeMetrics')

const SUPPORTED_GOD_COMMANDS = new Set(['declare_war', 'make_peace', 'bless_people'])
const INTENT_TYPES = new Set(['idle', 'wander', 'follow', 'respond'])

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
 * @param {unknown} item
 */
function isRuntimeAgentShape(item) {
  return !!item
    && typeof item.name === 'string'
    && typeof item.faction === 'string'
}

/**
 * @param {unknown} item
 */
function isLegacyGodAgentShape(item) {
  return isRuntimeAgentShape(item) && typeof item.applyGodCommand === 'function'
}

/**
 * @param {Record<string, unknown>} profile
 */
function normalizeWorldIntent(profile) {
  const source = (profile && typeof profile.world_intent === 'object' && profile.world_intent)
    ? profile.world_intent
    : {}
  const budgets = (source && typeof source.budgets === 'object' && source.budgets)
    ? source.budgets
    : {}
  const intent = asText(source.intent, 'idle', 16)
  return {
    intent: INTENT_TYPES.has(intent) ? intent : 'idle',
    intent_target: asText(source.intent_target, '', 80) || null,
    intent_set_at: Number(source.intent_set_at || 0) || 0,
    last_action: asText(source.last_action, '', 120),
    last_action_at: Number(source.last_action_at || 0) || 0,
    budgets: {
      minute_bucket: Number(budgets.minute_bucket || 0) || 0,
      events_in_min: Number(budgets.events_in_min || 0) || 0
    },
    manual_override: !!source.manual_override,
    frozen: !!source.frozen,
    is_leader: !!source.is_leader
  }
}

/**
 * @param {any} memory
 * @param {string} agentName
 */
function ensureAgentProfile(memory, agentName) {
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
 * @param {unknown[]} agents
 * @param {string} name
 */
function resolveRuntimeAgent(agents, name) {
  const target = asText(name, '', 80).toLowerCase()
  if (!target) return null
  return agents.find(agent => asText(agent?.name, '', 80).toLowerCase() === target) || null
}

/**
 * @param {any} memory
 */
function findCurrentLeader(memory) {
  for (const [name, agent] of Object.entries(memory.agents || {})) {
    const profile = agent && typeof agent.profile === 'object' ? agent.profile : null
    const worldIntent = profile ? normalizeWorldIntent(profile) : null
    if (worldIntent?.is_leader) return name
  }
  return null
}

/**
 * @param {string} rawCommand
 */
function parseGodCommand(rawCommand) {
  const full = asText(rawCommand, '', 240)
  if (!full) return { type: 'invalid', reason: 'No god command provided.' }
  const words = full.split(/\s+/)
  const head = words[0].toLowerCase()

  if (SUPPORTED_GOD_COMMANDS.has(head) && words.length === 1) {
    return { type: 'legacy_world', command: head }
  }

  if (head === 'loop') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'start') {
      const tickMsRaw = words[2]
      const tickMs = tickMsRaw ? Number(tickMsRaw) : undefined
      if (tickMsRaw && (!Number.isInteger(tickMs) || tickMs < 100)) {
        return { type: 'invalid', reason: 'god loop start tickMs must be an integer >= 100.' }
      }
      return { type: 'loop_start', tickMs }
    }
    if (action === 'stop') return { type: 'loop_stop' }
    if (action === 'status') return { type: 'loop_status' }
    return { type: 'invalid', reason: 'Usage: god loop start [tickMs] | god loop stop | god loop status' }
  }

  if (head === 'status') return { type: 'status' }

  if (head === 'leader') {
    const action = asText(words[1], '', 20).toLowerCase()
    if (action === 'set') {
      const name = asText(words.slice(2).join(' '), '', 80)
      if (!name) return { type: 'invalid', reason: 'Usage: god leader set <name>' }
      return { type: 'leader_set', name }
    }
    if (action === 'clear') return { type: 'leader_clear' }
    return { type: 'invalid', reason: 'Usage: god leader set <name> | god leader clear' }
  }

  if (head === 'freeze') {
    const name = asText(words.slice(1).join(' '), '', 80)
    if (!name) return { type: 'invalid', reason: 'Usage: god freeze <agent>' }
    return { type: 'freeze', name }
  }

  if (head === 'unfreeze') {
    const name = asText(words.slice(1).join(' '), '', 80)
    if (!name) return { type: 'invalid', reason: 'Usage: god unfreeze <agent>' }
    return { type: 'unfreeze', name }
  }

  if (head === 'intent') {
    const action = asText(words[1], '', 20).toLowerCase()
    const agentName = asText(words[2], '', 80)
    const intent = asText(words[3], '', 16).toLowerCase()
    const target = asText(words.slice(4).join(' '), '', 80) || null
    if (action !== 'set' || !agentName || !intent || !INTENT_TYPES.has(intent)) {
      return { type: 'invalid', reason: 'Usage: god intent set <agent> <idle|wander|follow|respond> [target]' }
    }
    return { type: 'intent_set', name: agentName, intent, target }
  }

  if (head === 'say') {
    const match = /^say\s+(\S+)\s+(.+)$/i.exec(full)
    if (!match) return { type: 'invalid', reason: 'Usage: god say <agent> <message>' }
    return {
      type: 'say',
      name: asText(match[1], '', 80),
      message: asText(match[2], '', 240)
    }
  }

  return { type: 'invalid', reason: `Unsupported god command: ${full}` }
}

/**
 * @param {ReturnType<import('./memory').createMemoryStore>} memoryStore
 * @param {unknown[]} agents
 * @param {ReturnType<import('./worldLoop').createWorldLoop> | null} worldLoop
 */
function buildDefaultStatus(memoryStore, agents, worldLoop) {
  const loopStatus = worldLoop && typeof worldLoop.getWorldLoopStatus === 'function'
    ? worldLoop.getWorldLoopStatus()
    : {
      running: false,
      tickMs: 0,
      lastTickAt: 0,
      scheduledCount: 0,
      backpressure: false,
      reason: 'not_configured'
    }
  const runtimeMetrics = memoryStore.getRuntimeMetrics()
  const observability = getObservabilitySnapshot()
  const snapshot = memoryStore.getSnapshot()
  const avgTxMs = observability.txDurationCount > 0
    ? observability.txDurationTotalMs / observability.txDurationCount
    : 0
  const slowTxRate = observability.txDurationCount > 0
    ? observability.slowTransactionCount / observability.txDurationCount
    : 0
  const heapMb = process.memoryUsage().heapUsed / (1024 * 1024)
  const memoryBytes = Buffer.byteLength(JSON.stringify(snapshot), 'utf-8')
  const integrity = memoryStore.validateMemoryIntegrity()
  const guardrailFlags = []
  if (!integrity.ok) guardrailFlags.push('CRITICAL:integrity_failed')
  if (runtimeMetrics.lockTimeouts > 0) guardrailFlags.push('CRITICAL:lock_timeouts')
  if (observability.txDurationP99Ms >= 500) guardrailFlags.push('CRITICAL:p99_tx_ge_500')
  if (slowTxRate > 0.10) guardrailFlags.push('WARN:slow_tx_rate_gt_0_10')
  if (loopStatus.backpressure) guardrailFlags.push(`WARN:backpressure:${loopStatus.reason}`)

  return {
    loopStatus,
    agentsOnline: agents.length,
    avgTxMs,
    p95TxMs: observability.txDurationP95Ms,
    p99TxMs: observability.txDurationP99Ms,
    lockWaitP95Ms: observability.txPhaseP95Ms.lockWaitMs,
    lockWaitP99Ms: observability.txPhaseP99Ms.lockWaitMs,
    memoryBytes,
    heapMb,
    guardrailFlags
  }
}

/**
 * @param {{
 *   memoryStore: ReturnType<import('./memory').createMemoryStore>,
 *   logger?: ReturnType<typeof createLogger>,
 *   worldLoop?: ReturnType<import('./worldLoop').createWorldLoop> | null,
 *   getStatusSnapshot?: (input: {agents: unknown[]}) => Promise<any> | any,
 *   runtimeSay?: (input: {agent: any, message: string}) => Promise<void> | void,
 *   now?: () => number
 * }} deps
 */
function createGodCommandService(deps) {
  if (!deps?.memoryStore) {
    throw new AppError({
      code: 'GOD_SERVICE_CONFIG_ERROR',
      message: 'memoryStore dependency is required for god command service.',
      recoverable: false
    })
  }

  const memoryStore = deps.memoryStore
  const logger = deps.logger || createLogger({ component: 'god_commands' })
  const worldLoop = deps.worldLoop || null
  const runtimeSay = typeof deps.runtimeSay === 'function' ? deps.runtimeSay : null
  const getStatusSnapshot = typeof deps.getStatusSnapshot === 'function' ? deps.getStatusSnapshot : null
  const now = deps.now || (() => Date.now())

  /**
   * @param {{agents: unknown[], command: string, operationId: string}} input
   */
  async function applyGodCommand(input) {
    const command = asText(input?.command, '', 240)
    const operationId = asText(input?.operationId, '', 200)
    const runtimeAgents = Array.isArray(input?.agents) ? input.agents.filter(isRuntimeAgentShape) : []
    const legacyAgents = runtimeAgents.filter(isLegacyGodAgentShape)
    const parsed = parseGodCommand(command)

    if (parsed.type === 'invalid') {
      throw new AppError({
        code: 'INVALID_GOD_COMMAND',
        message: parsed.reason,
        recoverable: true
      })
    }
    if (!operationId) {
      throw new AppError({
        code: 'INVALID_GOD_OPERATION',
        message: 'God command requires operationId for idempotency.',
        recoverable: true
      })
    }

    if (parsed.type === 'legacy_world') {
      const tx = await memoryStore.transact((memory) => {
        const world = memory.world
        if (parsed.command === 'declare_war') world.warActive = true
        if (parsed.command === 'make_peace') world.warActive = false
        if (parsed.command === 'declare_war') world.player.legitimacy = Math.max(0, world.player.legitimacy - 8)
        if (parsed.command === 'bless_people') world.player.legitimacy = Math.min(100, world.player.legitimacy + 5)
      }, { eventId: `${operationId}:god_command` })

      if (tx.skipped) {
        logger.info('god_command_duplicate_ignored', { operationId, command: parsed.command })
        return { applied: false, command, reason: 'Duplicate operation ignored.' }
      }

      // Persist world state before applying runtime side effects to prevent drift.
      legacyAgents.forEach(agent => agent.applyGodCommand(parsed.command))
      logger.info('god_command_applied', { operationId, command: parsed.command, affectedAgents: legacyAgents.length })
      return { applied: true, command, audit: true }
    }

    if (parsed.type === 'loop_start') {
      if (!worldLoop || typeof worldLoop.startWorldLoop !== 'function') {
        throw new AppError({
          code: 'WORLD_LOOP_UNAVAILABLE',
          message: 'World loop is not configured.',
          recoverable: true
        })
      }
      const status = worldLoop.startWorldLoop({ tickMs: parsed.tickMs })
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `WORLD LOOP STARTED: tickMs=${status.tickMs}`,
          `WORLD LOOP STATUS: running=${status.running} backpressure=${status.backpressure} reason=${status.reason}`
        ]
      }
    }

    if (parsed.type === 'loop_stop') {
      if (!worldLoop || typeof worldLoop.stopWorldLoop !== 'function') {
        throw new AppError({
          code: 'WORLD_LOOP_UNAVAILABLE',
          message: 'World loop is not configured.',
          recoverable: true
        })
      }
      const status = worldLoop.stopWorldLoop()
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `WORLD LOOP STOPPED: running=${status.running}`,
          `WORLD LOOP STATUS: running=${status.running} backpressure=${status.backpressure} reason=${status.reason}`
        ]
      }
    }

    if (parsed.type === 'loop_status') {
      const status = worldLoop && typeof worldLoop.getWorldLoopStatus === 'function'
        ? worldLoop.getWorldLoopStatus()
        : {
          running: false,
          tickMs: 0,
          lastTickAt: 0,
          scheduledCount: 0,
          backpressure: false,
          reason: 'not_configured'
        }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [
          `WORLD LOOP STATUS: running=${status.running} tickMs=${status.tickMs} lastTickAt=${status.lastTickAt || 0}`,
          `WORLD LOOP LAST_TICK: scheduled=${status.scheduledCount} backpressure=${status.backpressure} reason=${status.reason}`
        ]
      }
    }

    if (parsed.type === 'status') {
      const status = getStatusSnapshot
        ? await getStatusSnapshot({ agents: runtimeAgents })
        : buildDefaultStatus(memoryStore, runtimeAgents, worldLoop)
      const lines = [
        `GOD STATUS: loop_running=${!!status.loopStatus?.running} agents_online=${Number(status.agentsOnline || 0)} last_tick_at=${Number(status.loopStatus?.lastTickAt || 0)}`,
        `GOD STATUS TX: avg=${Number(status.avgTxMs || 0).toFixed(2)}ms p95=${Number(status.p95TxMs || 0).toFixed(2)}ms p99=${Number(status.p99TxMs || 0).toFixed(2)}ms`,
        `GOD STATUS LOCK: lock_wait_p95=${Number(status.lockWaitP95Ms || 0).toFixed(2)}ms lock_wait_p99=${Number(status.lockWaitP99Ms || 0).toFixed(2)}ms`,
        `GOD STATUS MEMORY: memory_bytes=${Number(status.memoryBytes || 0)} heap_mb=${Number(status.heapMb || 0).toFixed(2)}`,
        `GOD STATUS FLAGS: ${(Array.isArray(status.guardrailFlags) && status.guardrailFlags.length) ? status.guardrailFlags.join(' | ') : 'CLEAN'}`
      ]
      return { applied: true, command, audit: false, outputLines: lines }
    }

    if (parsed.type === 'leader_set') {
      const runtimeAgent = resolveRuntimeAgent(runtimeAgents, parsed.name)
      if (!runtimeAgent) {
        throw new AppError({
          code: 'UNKNOWN_AGENT',
          message: `Unknown agent for leader set: ${parsed.name}`,
          recoverable: true
        })
      }
      const leaderName = runtimeAgent.name
      const tx = await memoryStore.transact((memory) => {
        for (const [name, record] of Object.entries(memory.agents || {})) {
          const profile = ensureAgentProfile(memory, name)
          const worldIntent = normalizeWorldIntent(profile)
          worldIntent.is_leader = false
          profile.world_intent = worldIntent
          if (record && typeof record === 'object') {
            memory.agents[name] = { ...record, profile }
          }
        }
        const profile = ensureAgentProfile(memory, leaderName)
        const worldIntent = normalizeWorldIntent(profile)
        worldIntent.is_leader = true
        worldIntent.last_action = 'leader_set'
        worldIntent.last_action_at = now()
        profile.world_intent = worldIntent
      }, { eventId: `${operationId}:leader_set:${leaderName.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      runtimeAgents.forEach(agent => {
        agent.worldLeader = leaderName
      })
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`LEADER SET: ${leaderName}`]
      }
    }

    if (parsed.type === 'leader_clear') {
      const tx = await memoryStore.transact((memory) => {
        for (const name of Object.keys(memory.agents || {})) {
          const profile = ensureAgentProfile(memory, name)
          const worldIntent = normalizeWorldIntent(profile)
          worldIntent.is_leader = false
          worldIntent.last_action = 'leader_cleared'
          worldIntent.last_action_at = now()
          profile.world_intent = worldIntent
        }
      }, { eventId: `${operationId}:leader_clear` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      runtimeAgents.forEach(agent => {
        agent.worldLeader = null
      })
      return {
        applied: true,
        command,
        audit: true,
        outputLines: ['LEADER CLEARED']
      }
    }

    if (parsed.type === 'freeze' || parsed.type === 'unfreeze') {
      const runtimeAgent = resolveRuntimeAgent(runtimeAgents, parsed.name)
      if (!runtimeAgent) {
        throw new AppError({
          code: 'UNKNOWN_AGENT',
          message: `Unknown agent: ${parsed.name}`,
          recoverable: true
        })
      }
      const freeze = parsed.type === 'freeze'
      const tx = await memoryStore.transact((memory) => {
        const profile = ensureAgentProfile(memory, runtimeAgent.name)
        const worldIntent = normalizeWorldIntent(profile)
        worldIntent.frozen = freeze
        if (freeze) {
          worldIntent.intent = 'idle'
          worldIntent.intent_target = null
        }
        worldIntent.last_action = freeze ? 'freeze' : 'unfreeze'
        worldIntent.last_action_at = now()
        profile.world_intent = worldIntent
      }, { eventId: `${operationId}:${freeze ? 'freeze' : 'unfreeze'}:${runtimeAgent.name.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      runtimeAgent.worldFrozen = freeze
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [`${freeze ? 'FROZEN' : 'UNFROZEN'}: ${runtimeAgent.name}`]
      }
    }

    if (parsed.type === 'intent_set') {
      const runtimeAgent = resolveRuntimeAgent(runtimeAgents, parsed.name)
      if (!runtimeAgent) {
        throw new AppError({
          code: 'UNKNOWN_AGENT',
          message: `Unknown agent: ${parsed.name}`,
          recoverable: true
        })
      }
      const tx = await memoryStore.transact((memory) => {
        const profile = ensureAgentProfile(memory, runtimeAgent.name)
        const worldIntent = normalizeWorldIntent(profile)
        const fallbackLeader = findCurrentLeader(memory)
        const target = parsed.intent === 'follow'
          ? (asText(parsed.target, '', 80) || fallbackLeader || null)
          : null
        worldIntent.intent = parsed.intent
        worldIntent.intent_target = target
        worldIntent.intent_set_at = now()
        worldIntent.manual_override = true
        worldIntent.last_action = 'intent_set'
        worldIntent.last_action_at = now()
        profile.world_intent = worldIntent
      }, { eventId: `${operationId}:intent_set:${runtimeAgent.name.toLowerCase()}` })

      if (tx.skipped) return { applied: false, command, reason: 'Duplicate operation ignored.' }
      return {
        applied: true,
        command,
        audit: true,
        outputLines: [
          `INTENT SET: ${runtimeAgent.name} -> ${parsed.intent}${parsed.target ? ` (${parsed.target})` : ''}`
        ]
      }
    }

    if (parsed.type === 'say') {
      const runtimeAgent = resolveRuntimeAgent(runtimeAgents, parsed.name)
      if (!runtimeAgent) {
        throw new AppError({
          code: 'UNKNOWN_AGENT',
          message: `Unknown agent: ${parsed.name}`,
          recoverable: true
        })
      }
      if (!runtimeSay) {
        logger.info('god_say_no_runtime_hook', { agent: runtimeAgent.name })
      } else {
        await runtimeSay({ agent: runtimeAgent, message: parsed.message })
      }
      return {
        applied: true,
        command,
        audit: false,
        outputLines: [`GOD SAY: ${runtimeAgent.name} <- ${parsed.message}`]
      }
    }

    throw new AppError({
      code: 'INVALID_GOD_COMMAND',
      message: `Unsupported god command: ${command || '(empty)'}`,
      recoverable: true
    })
  }

  return {
    applyGodCommand,
    SUPPORTED_GOD_COMMANDS
  }
}

module.exports = { createGodCommandService, SUPPORTED_GOD_COMMANDS, parseGodCommand }
