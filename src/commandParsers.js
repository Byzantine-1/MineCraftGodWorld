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
 * @param {unknown} raw
 */
function sanitizeMinecraftName(raw) {
  const value = asText(raw, '', 32)
  if (!value) return null
  return /^[A-Za-z0-9_]{3,16}$/.test(value) ? value : null
}

/**
 * @param {unknown} text
 */
function sanitizeChatText(text) {
  return asText(text, '', 800).replace(/[\u0000-\u001f\u007f]/g, '')
}

/**
 * @param {string} input
 */
function parseCliInput(input) {
  const trimmed = sanitizeChatText(input)
  if (!trimmed) return { type: 'noop' }

  const [command, targetRaw, ...rest] = trimmed.split(' ')
  const commandName = asText(command, '', 30).toLowerCase()
  const target = asText(targetRaw, '', 80)
  const message = asText(rest.join(' '), '', 600)

  if (commandName === 'talk') {
    if (!target) return { type: 'error', message: 'No agent target provided.' }
    if (!message) return { type: 'error', message: 'No message provided.' }
    return { type: 'talk', target: target.toLowerCase(), message }
  }

  if (commandName === 'god') {
    const payload = asText(trimmed.substring(command.length), '', 240)
    if (!payload) return { type: 'error', message: 'No god command provided.' }
    return { type: 'god', command: payload }
  }

  if (commandName === 'exit') return { type: 'exit' }
  return { type: 'unknown', command: commandName }
}

/**
 * @param {string} text
 */
function parseBridgeChat(text) {
  const message = sanitizeChatText(text)
  if (!message) return { type: 'noop' }
  const lower = message.toLowerCase()

  if (lower === 'party on') return { type: 'party_on' }
  if (lower === 'party off') return { type: 'party_off' }
  if (lower.startsWith('party leader ')) {
    const candidate = message.substring('party leader '.length).trim()
    return { type: 'party_leader', leaderName: sanitizeMinecraftName(candidate) }
  }
  if (lower.startsWith('god ')) {
    return { type: 'god', command: asText(message.substring(4), '', 240) }
  }

  const match = /^([a-zA-Z0-9_]{3,16})\s*:\s*(.+)$/.exec(message)
  if (!match) return { type: 'noop' }
  return {
    type: 'npc_talk',
    target: asText(match[1], '', 80).toLowerCase(),
    message: asText(match[2], '', 600)
  }
}

module.exports = {
  parseCliInput,
  parseBridgeChat,
  sanitizeMinecraftName,
  sanitizeChatText
}
