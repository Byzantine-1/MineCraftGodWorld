const { createLogger } = require('./logger')
const { AppError } = require('./errors')

const ACTION_TYPES = new Set([
  'none',
  'spread_rumor',
  'recruit',
  'call_meeting',
  'desert_faction',
  'attack_player'
])
const DETERMINISTIC_TIME_EPOCH_MS = Date.parse('2026-01-01T00:00:00.000Z')

/**
 * @param {number} n
 * @param {number} min
 * @param {number} max
 */
function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n))
}

/**
 * @param {string} text
 */
function stableHashNumber(text) {
  let hash = 0
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) - hash) + text.charCodeAt(i)
    hash |= 0
  }
  return Math.abs(hash)
}

/**
 * @param {any} world
 * @param {string} scopeKey
 * @param {number} [offset]
 */
function deterministicArchiveTime(world, scopeKey, offset = 0) {
  const dayRaw = Number(world?.clock?.day)
  const day = Number.isInteger(dayRaw) && dayRaw > 0 ? dayRaw : 1
  const processedCount = Array.isArray(world?.processedEventIds) ? world.processedEventIds.length : 0
  const orderedOffset = Math.min(86000, Math.max(0, Math.trunc(processedCount))) * 1000
  const salt = stableHashNumber(scopeKey) % 997
  return DETERMINISTIC_TIME_EPOCH_MS + ((day - 1) * 86400000) + orderedOffset + salt + Math.max(0, Math.trunc(Number(offset) || 0))
}

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
 * @param {unknown} actions
 */
function sanitizeActions(actions) {
  if (!Array.isArray(actions)) return [{ type: 'none', reason: 'invalid_actions' }]
  const safe = actions.slice(0, 3).map((action) => {
    const type = asText(action?.type, 'none', 32)
    return {
      type: ACTION_TYPES.has(type) ? type : 'none',
      reason: asText(action?.reason, 'none', 220)
    }
  })
  return safe.length ? safe : [{ type: 'none', reason: 'empty_actions' }]
}

/**
 * @param {Record<string, {hostilityToPlayer: number, stability: number}>} factions
 * @param {string} faction
 */
function ensureFactionStats(factions, faction) {
  factions[faction] = factions[faction] || { hostilityToPlayer: 10, stability: 70 }
  return factions[faction]
}

/**
 * @param {{
 *   memoryStore: ReturnType<import('./memory').createMemoryStore>,
 *   logger?: ReturnType<typeof createLogger>,
 *   now?: () => number
 * }} deps
 */
function createActionEngine(deps) {
  if (!deps?.memoryStore) {
    throw new AppError({
      code: 'ACTION_ENGINE_CONFIG_ERROR',
      message: 'memoryStore dependency is required for action engine.',
      recoverable: false
    })
  }

  const memoryStore = deps.memoryStore
  const logger = deps.logger || createLogger({ component: 'action_engine' })
  const providedNow = typeof deps.now === 'function' ? deps.now : null

  /**
   * Deterministic action application with persistent idempotency.
   * @param {{
   *   agent: {name: string, faction: string},
   *   proposedActions: unknown,
   *   operationId: string
   * }} input
   */
  async function applyProposedActions(input) {
    const agentName = asText(input?.agent?.name, '', 80)
    const faction = asText(input?.agent?.faction, '', 80)
    const operationId = asText(input?.operationId, '', 200)

    if (!agentName || !faction) {
      throw new AppError({
        code: 'INVALID_AGENT',
        message: 'Action engine requires valid agent identity.',
        recoverable: true
      })
    }
    if (!operationId) {
      throw new AppError({
        code: 'INVALID_ACTION_OPERATION',
        message: 'Action engine requires operationId for idempotency.',
        recoverable: true
      })
    }

    const actions = sanitizeActions(input?.proposedActions)
    const outcomes = []

    const tx = await memoryStore.transact((memory) => {
      const world = memory.world
      const factionStats = ensureFactionStats(world.factions, faction)
      let archiveSeq = 0

      function nextArchiveTime(scopeTag) {
        if (providedNow) {
          const candidate = Number(providedNow())
          if (Number.isFinite(candidate) && candidate >= 0) return candidate
        }
        const safeTag = asText(scopeTag, 'event', 80)
        return deterministicArchiveTime(world, `${operationId}:${agentName}:${safeTag}`, archiveSeq++)
      }

      function appendWorldEvent(event, important = false, scopeTag = 'event') {
        world.archive.push({ time: nextArchiveTime(scopeTag), event, important: !!important })
        if (world.archive.length > 500) world.archive = world.archive.slice(-500)
      }

      for (const action of actions) {
        const type = action.type

        if (type === 'none') {
          outcomes.push({ type, accepted: false, reason: 'No action proposed.' })
          continue
        }

        if (type === 'spread_rumor') {
          world.player.legitimacy = clamp(world.player.legitimacy - 2, 0, 100)
          factionStats.hostilityToPlayer = clamp(factionStats.hostilityToPlayer + 3, 0, 100)
          appendWorldEvent(`[RUMOR] ${agentName} spreads a rumor: ${action.reason || '...'}`, false, 'spread_rumor')
          outcomes.push({ type, accepted: true, outcome: 'rumor_spread' })
          continue
        }

        if (type === 'call_meeting') {
          factionStats.stability = clamp(factionStats.stability - 2, 0, 100)
          appendWorldEvent(`[MEETING] ${agentName} calls a meeting in ${faction}.`, false, 'call_meeting')
          outcomes.push({ type, accepted: true, outcome: 'meeting_called' })
          continue
        }

        if (type === 'recruit') {
          factionStats.stability = clamp(factionStats.stability + 1, 0, 100)
          outcomes.push({ type, accepted: true, outcome: 'recruit_attempted' })
          continue
        }

        if (type === 'desert_faction') {
          factionStats.stability = clamp(factionStats.stability - 6, 0, 100)
          appendWorldEvent(`[SPLINTER] ${agentName} deserts ${faction}.`, false, 'desert_faction')
          outcomes.push({ type, accepted: true, outcome: 'deserted' })
          continue
        }

        if (type === 'attack_player') {
          const lethal = !!world.rules?.allowLethalPolitics
          const hostileEnough = factionStats.hostilityToPlayer >= 75
          const legitimacyLow = world.player.legitimacy <= 25
          const warOrChaos = !!world.warActive || factionStats.stability <= 35

          if (!lethal) {
            outcomes.push({ type, accepted: false, reason: 'Lethal politics disabled.' })
            continue
          }
          if (!(hostileEnough && legitimacyLow && warOrChaos)) {
            outcomes.push({
              type,
              accepted: false,
              reason: `Blocked (hostility:${factionStats.hostilityToPlayer}, legitimacy:${world.player.legitimacy}, war:${world.warActive}, stability:${factionStats.stability}).`
            })
            continue
          }

          world.player.alive = false
          appendWorldEvent(`[ASSASSINATION] ${agentName} leads an attack. The player is killed.`, true, 'attack_player')
          outcomes.push({ type, accepted: true, outcome: 'player_killed' })
          continue
        }

        outcomes.push({ type, accepted: false, reason: 'Unknown action type.' })
      }
    }, { eventId: `${operationId}:apply_actions` })

    if (tx.skipped) {
      logger.info('apply_actions_skipped_duplicate', { operationId, agent: agentName })
      return actions.map(action => ({
        type: action.type,
        accepted: false,
        reason: 'Duplicate operation ignored.'
      }))
    }

    logger.debug('apply_actions_complete', { operationId, agent: agentName, outcomes })
    return outcomes
  }

  return { applyProposedActions }
}

module.exports = { createActionEngine, sanitizeActions }
