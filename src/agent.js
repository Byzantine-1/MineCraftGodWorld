/**
 * Clamp number to a bounded range.
 * @param {number} n
 * @param {number} min
 * @param {number} max
 */
function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n))
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
 * Domain model for an NPC.
 * Invariants:
 * - trust is always between 0 and 10.
 * - combatState is either "peace" or "war".
 */
class Agent {
  /**
   * @param {{name: string, role: string, faction: string}} input
   */
  constructor(input) {
    this.name = asText(input?.name, 'Unknown', 80)
    this.role = asText(input?.role, 'Unknown', 80)
    this.faction = asText(input?.faction, 'Neutral', 80)
    this.mood = 'calm'
    this.trust = 3
    this.combatState = 'peace'
    this.flags = { rebellious: false }
  }

  /**
   * @param {string} command
   */
  applyGodCommand(command) {
    if (command === 'declare_war') {
      this.combatState = 'war'
      this.mood = 'angry'
      return
    }

    if (command === 'make_peace') {
      this.combatState = 'peace'
      if (this.mood === 'angry') this.mood = 'calm'
      return
    }

    if (command === 'bless_people') {
      if (this.combatState !== 'war') this.mood = 'happy'
      this.trust = clamp(this.trust + 1, 0, 10)
    }
  }

  /**
   * Apply sanitized NPC turn output.
   * @param {{trust_delta: number, tone: string}} turn
   */
  applyNpcTurn(turn) {
    if (!turn || typeof turn !== 'object') return

    const delta = clamp(Number(turn.trust_delta || 0), -2, 2)
    this.trust = clamp(this.trust + delta, 0, 10)

    const tone = asText(turn.tone, 'calm', 16)
    if (tone === 'hostile') this.mood = 'angry'
    else if (tone === 'joyful') this.mood = 'happy'
    else if (tone === 'fearful') this.mood = 'fearful'
    else this.mood = 'calm'

    if (this.trust <= 1) this.flags.rebellious = true
  }

  /**
   * Reserved for future event-based updates.
   */
  updateFromMemory() {
    return
  }

  toSnapshot() {
    return {
      name: this.name,
      role: this.role,
      faction: this.faction,
      mood: this.mood,
      trust: this.trust,
      combatState: this.combatState,
      flags: { ...this.flags }
    }
  }
}

module.exports = Agent
