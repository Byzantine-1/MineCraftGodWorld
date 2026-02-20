const { createLogger } = require('./logger')
const { AppError } = require('./errors')

const SUPPORTED_GOD_COMMANDS = new Set(['declare_war', 'make_peace', 'bless_people'])

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
function isAgentShape(item) {
  return !!item
    && typeof item.name === 'string'
    && typeof item.faction === 'string'
    && typeof item.applyGodCommand === 'function'
}

/**
 * @param {{
 *   memoryStore: ReturnType<import('./memory').createMemoryStore>,
 *   logger?: ReturnType<typeof createLogger>
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

  /**
   * @param {{agents: unknown[], command: string, operationId: string}} input
   */
  async function applyGodCommand(input) {
    const command = asText(input?.command, '', 40)
    const operationId = asText(input?.operationId, '', 200)
    const agents = Array.isArray(input?.agents) ? input.agents.filter(isAgentShape) : []

    if (!SUPPORTED_GOD_COMMANDS.has(command)) {
      throw new AppError({
        code: 'INVALID_GOD_COMMAND',
        message: `Unsupported god command: ${command || '(empty)'}`,
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

    const tx = await memoryStore.transact((memory) => {
      const world = memory.world

      if (command === 'declare_war') world.warActive = true
      if (command === 'make_peace') world.warActive = false
      if (command === 'declare_war') world.player.legitimacy = Math.max(0, world.player.legitimacy - 8)
      if (command === 'bless_people') world.player.legitimacy = Math.min(100, world.player.legitimacy + 5)
    }, { eventId: `${operationId}:god_command` })

    if (tx.skipped) {
      logger.info('god_command_duplicate_ignored', { operationId, command })
      return { applied: false, command, reason: 'Duplicate operation ignored.' }
    }

    // Persist world state before applying runtime side effects to prevent drift.
    agents.forEach(agent => agent.applyGodCommand(command))

    logger.info('god_command_applied', { operationId, command, affectedAgents: agents.length })
    return { applied: true, command }
  }

  return {
    applyGodCommand,
    SUPPORTED_GOD_COMMANDS
  }
}

module.exports = { createGodCommandService, SUPPORTED_GOD_COMMANDS }
