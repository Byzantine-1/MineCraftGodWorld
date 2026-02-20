/**
 * @param {number} n
 * @param {number} min
 * @param {number} max
 */
function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n))
}

const ALLOWED_TONES = new Set(['calm', 'wary', 'hostile', 'fearful', 'proud', 'sad', 'joyful'])
const ALLOWED_ACTIONS = new Set([
  'none',
  'spread_rumor',
  'recruit',
  'call_meeting',
  'desert_faction',
  'attack_player'
])
const ALLOWED_SCOPES = new Set(['agent', 'faction', 'world'])

/**
 * @param {unknown} value
 * @param {string} fallback
 * @param {number} maxLen
 */
function sanitizeString(value, fallback, maxLen) {
  if (typeof value !== 'string') return fallback
  const trimmed = value.trim()
  if (!trimmed) return fallback
  return trimmed.slice(0, maxLen)
}

/**
 * @param {any} fallback
 */
function cloneFallback(fallback) {
  return {
    say: sanitizeString(fallback?.say, 'Speak.', 300),
    tone: ALLOWED_TONES.has(fallback?.tone) ? fallback.tone : 'wary',
    trust_delta: clamp(Number(fallback?.trust_delta || 0), -2, 2),
    memory_writes: Array.isArray(fallback?.memory_writes) ? fallback.memory_writes : [],
    proposed_actions: Array.isArray(fallback?.proposed_actions)
      ? fallback.proposed_actions
      : [{ type: 'none', target: 'none', confidence: 0, reason: 'fallback' }]
  }
}

/**
 * @param {unknown} memoryWrites
 */
function sanitizeMemoryWrites(memoryWrites) {
  if (!Array.isArray(memoryWrites)) return []
  return memoryWrites.slice(0, 5).reduce((acc, item) => {
    const scope = sanitizeString(item?.scope, '', 16)
    if (!ALLOWED_SCOPES.has(scope)) return acc
    const text = sanitizeString(item?.text, '', 220)
    if (!text) return acc
    const importance = clamp(Number(item?.importance || 1), 1, 10)
    acc.push({ scope, text, importance })
    return acc
  }, [])
}

/**
 * @param {unknown} actions
 */
function sanitizeActions(actions) {
  if (!Array.isArray(actions)) {
    return [{ type: 'none', target: 'none', confidence: 0, reason: 'invalid_actions' }]
  }
  const safe = actions.slice(0, 3).reduce((acc, item) => {
    const type = sanitizeString(item?.type, 'none', 32)
    if (!ALLOWED_ACTIONS.has(type)) return acc
    const target = sanitizeString(item?.target, 'none', 80)
    const confidence = clamp(Number(item?.confidence || 0), 0, 1)
    const reason = sanitizeString(item?.reason, 'no_reason', 220)
    acc.push({ type, target, confidence, reason })
    return acc
  }, [])
  return safe.length ? safe : [{ type: 'none', target: 'none', confidence: 0, reason: 'no_valid_actions' }]
}

/**
 * Validate and sanitize model turn payload.
 * @param {unknown} turn
 * @param {{say: string, tone: string, trust_delta: number, memory_writes: any[], proposed_actions: any[]}} fallback
 */
function sanitizeTurn(turn, fallback) {
  const base = cloneFallback(fallback)
  if (!turn || typeof turn !== 'object') return base

  const say = sanitizeString(turn.say, base.say, 300)
  const toneRaw = sanitizeString(turn.tone, base.tone, 16)
  const tone = ALLOWED_TONES.has(toneRaw) ? toneRaw : base.tone
  const trustDelta = clamp(Number(turn.trust_delta || 0), -2, 2)

  return {
    say,
    tone,
    trust_delta: trustDelta,
    memory_writes: sanitizeMemoryWrites(turn.memory_writes),
    proposed_actions: sanitizeActions(turn.proposed_actions)
  }
}

module.exports = { sanitizeTurn, sanitizeActions }
