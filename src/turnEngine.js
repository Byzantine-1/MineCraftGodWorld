const { sanitizeTurn } = require('./turnGuard')
const { createLogger } = require('./logger')
const { AppError } = require('./errors')

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

/**
 * @param {{
 *   memoryStore: ReturnType<import('./memory').createMemoryStore>,
 *   actionEngine: ReturnType<import('./actionEngine').createActionEngine>,
 *   logger?: ReturnType<typeof createLogger>
 * }} deps
 */
function createTurnEngine(deps) {
  if (!deps?.memoryStore || !deps?.actionEngine) {
    throw new AppError({
      code: 'TURN_ENGINE_CONFIG_ERROR',
      message: 'turn engine requires memoryStore and actionEngine dependencies.',
      recoverable: false
    })
  }

  const memoryStore = deps.memoryStore
  const actionEngine = deps.actionEngine
  const logger = deps.logger || createLogger({ component: 'turn_engine' })

  /**
   * @param {{agent: {name: string, faction: string, combatState: string}, playerName?: string | null, message: string, operationId: string}} input
   */
  async function recordIncoming(input) {
    const agentName = asText(input?.agent?.name, '', 80)
    const faction = asText(input?.agent?.faction, '', 80)
    const combatState = asText(input?.agent?.combatState, 'peace', 20)
    const message = asText(input?.message, '', 500)
    const operationId = asText(input?.operationId, '', 200)
    const playerName = asText(input?.playerName, 'Player', 80)

    if (!agentName || !faction || !message || !operationId) {
      throw new AppError({
        code: 'INVALID_RECORD_INCOMING',
        message: 'recordIncoming received invalid input.',
        recoverable: true
      })
    }

    const prefix = playerName === 'Player' ? 'Player' : `Player(${playerName})`
    await memoryStore.rememberAgent(
      agentName,
      `${prefix} said "${message}" during ${combatState}.`,
      combatState === 'war',
      `${operationId}:incoming`
    )
    await memoryStore.rememberFaction(
      faction,
      `${prefix} interaction during ${combatState}: "${message}".`,
      false,
      `${operationId}:incoming`
    )
    await memoryStore.rememberWorld(
      `${prefix} -> ${agentName}: "${message}"`,
      false,
      `${operationId}:incoming`
    )
  }

  /**
   * @param {{
   *   agent: {name: string, faction: string, trust?: number, mood?: string, flags?: {rebellious?: boolean}, applyNpcTurn?: (turn: any) => void},
   *   rawTurn: unknown,
   *   fallbackTurn: {say: string, tone: string, trust_delta: number, memory_writes: any[], proposed_actions: any[]},
   *   operationId: string
   * }} input
   */
  async function applyTurn(input) {
    const operationId = asText(input?.operationId, '', 200)
    if (!operationId) {
      throw new AppError({
        code: 'INVALID_TURN_INPUT',
        message: 'applyTurn requires operationId for idempotency.',
        recoverable: true
      })
    }

    if (memoryStore.hasProcessedEvent(`${operationId}:turn_applied`)) {
      logger.info('apply_turn_duplicate_ignored', { operationId, agent: input.agent?.name })
      const world = memoryStore.recallWorld()
      return {
        skipped: true,
        turn: input.fallbackTurn,
        outcomes: [],
        playerAlive: world.player.alive !== false
      }
    }

    const agentName = asText(input?.agent?.name, '', 80)
    const faction = asText(input?.agent?.faction, '', 80)
    if (!agentName || !faction || !input.agent || typeof input.agent.applyNpcTurn !== 'function') {
      throw new AppError({
        code: 'INVALID_TURN_AGENT',
        message: 'applyTurn requires valid agent identity and mutation handler.',
        recoverable: true
      })
    }

    const turn = sanitizeTurn(input.rawTurn, input.fallbackTurn)
    const stateTx = await memoryStore.transact((memory) => {
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
      const profile = memory.agents[agentName].profile || {
        trust: clamp(Number(input.agent.trust ?? 3), 0, 10),
        mood: asText(input.agent.mood, 'calm', 16),
        flags: { rebellious: !!input.agent.flags?.rebellious }
      }
      const carrier = {
        trust: clamp(Number(profile.trust ?? 3), 0, 10),
        mood: asText(profile.mood, 'calm', 16),
        flags: { rebellious: !!profile.flags?.rebellious }
      }

      // Apply trust/mood mutations inside the durable transaction to prevent runtime-only drift.
      input.agent.applyNpcTurn.call(carrier, turn)

      const nextProfile = {
        trust: clamp(Number(carrier.trust ?? 3), 0, 10),
        mood: asText(carrier.mood, 'calm', 16),
        flags: { rebellious: !!carrier.flags?.rebellious }
      }
      memory.agents[agentName].profile = nextProfile
      return nextProfile
    }, { eventId: `${operationId}:agent_state` })

    if (!stateTx.skipped && stateTx.result) {
      input.agent.trust = stateTx.result.trust
      input.agent.mood = stateTx.result.mood
      input.agent.flags = { ...(input.agent.flags || {}), rebellious: !!stateTx.result.flags?.rebellious }
    }

    for (let i = 0; i < turn.memory_writes.length; i += 1) {
      const write = turn.memory_writes[i]
      const important = Number(write.importance || 0) >= 7
      const eventId = `${operationId}:memory_write:${i}`
      if (write.scope === 'agent') await memoryStore.rememberAgent(agentName, write.text, important, eventId)
      if (write.scope === 'faction') await memoryStore.rememberFaction(faction, write.text, important, eventId)
      if (write.scope === 'world') await memoryStore.rememberWorld(write.text, important, eventId)
    }

    const outcomes = await actionEngine.applyProposedActions({
      agent: { name: agentName, faction },
      proposedActions: turn.proposed_actions,
      operationId: `${operationId}:actions`
    })

    for (let i = 0; i < outcomes.length; i += 1) {
      const outcome = outcomes[i]
      if (!outcome.accepted) continue
      await memoryStore.rememberFaction(
        faction,
        `[ACTION] ${agentName} -> ${outcome.type} (${outcome.outcome})`,
        true,
        `${operationId}:outcome:${i}`
      )
    }

    const applyTx = await memoryStore.transact(() => {}, { eventId: `${operationId}:turn_applied` })
    if (applyTx.skipped) {
      const world = memoryStore.recallWorld()
      return {
        skipped: true,
        turn: input.fallbackTurn,
        outcomes: [],
        playerAlive: world.player.alive !== false
      }
    }

    const playerAlive = memoryStore.recallWorld().player?.alive !== false
    return { skipped: false, turn, outcomes, playerAlive }
  }

  return {
    recordIncoming,
    applyTurn
  }
}

module.exports = { createTurnEngine }
