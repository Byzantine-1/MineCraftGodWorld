const OpenAI = require('openai')
const { sanitizeTurn } = require('./turnGuard')
const { createLogger } = require('./logger')
const { AppError } = require('./errors')
const { createSemaphore, withTimeout } = require('./flowControl')
const { incrementMetric } = require('./runtimeMetrics')

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
 * @param {string[] | null | undefined} arr
 * @param {number} max
 */
function safeJoin(arr, max = 12) {
  if (!Array.isArray(arr)) return 'None'
  return arr.slice(-max).join('\n- ') || 'None'
}

/**
 * @param {string} text
 */
function sanitizePromptText(text) {
  return asText(text, '', 600).replace(/[\u0000-\u001f\u007f]/g, '')
}

/**
 * @param {{combatState: string}} agent
 */
function fallbackTurn(agent) {
  const war = agent?.combatState === 'war'
  return {
    say: war ? 'Watch your words. The world is on edge.' : 'Speak.',
    tone: war ? 'hostile' : 'wary',
    trust_delta: 0,
    memory_writes: [],
    proposed_actions: [{ type: 'none', target: 'none', confidence: 0, reason: 'fallback' }]
  }
}

/**
 * @param {{
 *   memoryStore: ReturnType<import('./memory').createMemoryStore>,
 *   logger?: ReturnType<typeof createLogger>,
 *   openaiClient?: OpenAI | null,
 *   model?: string,
 *   maxConcurrentTurns?: number,
 *   requestTimeoutMs?: number
 * }} deps
 */
function createDialogueService(deps) {
  const logger = deps?.logger || createLogger({ component: 'dialogue' })
  const memoryStore = deps?.memoryStore
  if (!memoryStore) {
    throw new AppError({
      code: 'DIALOGUE_CONFIG_ERROR',
      message: 'createDialogueService requires memoryStore dependency.',
      recoverable: false
    })
  }

  const model = asText(deps?.model, process.env.OPENAI_MODEL || 'gpt-5-mini', 80)
  const requestTimeoutMs = Number(deps?.requestTimeoutMs || process.env.OPENAI_TIMEOUT_MS || 15000)
  const maxConcurrentTurns = Number(deps?.maxConcurrentTurns || process.env.MAX_CONCURRENT_TURNS || 4)
  const withDialogueSlot = createSemaphore(maxConcurrentTurns)

  const client = deps?.openaiClient !== undefined
    ? deps.openaiClient
    : (process.env.OPENAI_API_KEY
      ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
      : null)

  const jsonSchema = {
    name: 'NpcTurn',
    schema: {
      type: 'object',
      additionalProperties: false,
      required: ['say', 'tone', 'trust_delta', 'memory_writes', 'proposed_actions'],
      properties: {
        say: { type: 'string', minLength: 1, maxLength: 300 },
        tone: { type: 'string', enum: ['calm', 'wary', 'hostile', 'fearful', 'proud', 'sad', 'joyful'] },
        trust_delta: { type: 'integer', minimum: -2, maximum: 2 },
        memory_writes: {
          type: 'array',
          maxItems: 5,
          items: {
            type: 'object',
            additionalProperties: false,
            required: ['scope', 'text', 'importance'],
            properties: {
              scope: { type: 'string', enum: ['agent', 'faction', 'world'] },
              text: { type: 'string', minLength: 1, maxLength: 220 },
              importance: { type: 'integer', minimum: 1, maximum: 10 }
            }
          }
        },
        proposed_actions: {
          type: 'array',
          maxItems: 3,
          items: {
            type: 'object',
            additionalProperties: false,
            required: ['type', 'target', 'confidence', 'reason'],
            properties: {
              type: {
                type: 'string',
                enum: ['none', 'spread_rumor', 'recruit', 'call_meeting', 'desert_faction', 'attack_player']
              },
              target: { type: 'string', minLength: 1, maxLength: 80 },
              confidence: { type: 'number', minimum: 0, maximum: 1 },
              reason: { type: 'string', minLength: 1, maxLength: 220 }
            }
          }
        }
      }
    }
  }

  const systemPrompt = [
    'You are an NPC in a living world. You are NOT an assistant.',
    'You must stay in character and never mention AI or game mechanics.',
    '',
    'Hard rules:',
    '- Respond with valid JSON that matches the schema exactly.',
    '- Keep "say" to 1-2 sentences.',
    '- You can propose actions, but you cannot assume they succeed.',
    '- If you are unsure, propose "none".'
  ].join('\n')

  /**
   * @param {{name: string, role: string, faction: string, mood: string, trust: number, combatState: string}} agent
   * @param {string} context
   */
  async function generateDialogue(agent, context = '') {
    const safeContext = sanitizePromptText(context)
    const fallback = fallbackTurn(agent)

    const agentMemory = memoryStore.recallAgent(agent.name)
    const factionMemory = memoryStore.recallFaction(agent.faction)
    const world = memoryStore.recallWorld()

    const shortMemory = safeJoin(agentMemory?.short, 20)
    const agentSummary = asText(agentMemory?.summary, 'No personal history.', 500)
    const factionSummary = asText(factionMemory?.summary, 'No faction history.', 500)

    if (!client) {
      logger.info('dialogue_fallback_no_client', { agent: agent.name })
      return fallback
    }

    const userPrompt = [
      `Name: ${agent.name}`,
      `Role: ${agent.role}`,
      `Faction: ${agent.faction}`,
      `Mood: ${agent.mood}`,
      `Trust (0-10): ${agent.trust}`,
      `Combat State: ${agent.combatState}`,
      '',
      'WORLD:',
      `- warActive: ${world.warActive}`,
      `- playerAlive: ${world.player.alive}`,
      `- playerLegitimacy: ${world.player.legitimacy}`,
      `- lethalPoliticsAllowed: ${world.rules.allowLethalPolitics}`,
      '',
      'Personal memories:',
      `- ${shortMemory}`,
      '',
      'Personal worldview:',
      agentSummary,
      '',
      'Faction worldview:',
      factionSummary,
      '',
      'Player just said:',
      `"${safeContext}"`
    ].join('\n')

    return withDialogueSlot(async () => {
      try {
        incrementMetric('openAiRequests')
        const response = await withTimeout(client.responses.create({
          model,
          input: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: userPrompt }
          ],
          text: {
            format: {
              type: 'json_schema',
              json_schema: jsonSchema
            }
          }
        }), requestTimeoutMs, 'dialogue_request_timeout')

        const turn = JSON.parse(response.output_text)
        const safe = sanitizeTurn(turn, fallback)
        safe.trust_delta = clamp(Number(safe.trust_delta || 0), -2, 2)
        return safe
      } catch (err) {
        if (err instanceof Error && err.message === 'dialogue_request_timeout') {
          incrementMetric('openAiTimeouts')
        }
        logger.errorWithStack('dialogue_generation_failed', err, { agent: agent.name })
        return fallback
      }
    })
  }

  return {
    generateDialogue,
    fallbackTurn
  }
}

module.exports = { createDialogueService, fallbackTurn }
