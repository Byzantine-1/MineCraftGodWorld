const { createHash } = require('crypto')

const MAX_WORLD_PLAYER_ENTRIES = 256
const DEFAULT_SPAWN_DIMENSION = 'overworld'
const DEFAULT_SPAWN_RADIUS = 2
const DEFAULT_SPAWN_KIND = 'town_hub'
const DEFAULT_GLOBAL_FALLBACK_SPAWN = Object.freeze({
  dimension: DEFAULT_SPAWN_DIMENSION,
  x: 0,
  y: 80,
  z: 0,
  yaw: 0,
  pitch: 0,
  radius: DEFAULT_SPAWN_RADIUS,
  kind: 'global_fallback'
})
const PLAYER_SPAWN_POLICIES = new Set([
  'deterministic_starter_town',
  'explicit_town'
])

function isPlainObject(value) {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value))
}

function asText(value, fallback = '', maxLen = 80) {
  if (typeof value !== 'string') return fallback
  const trimmed = value.trim()
  return trimmed ? trimmed.slice(0, maxLen) : fallback
}

function asFiniteNumber(value) {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function canonicalizeTownId(world, townId) {
  const safeTownId = asText(townId, '', 80).toLowerCase()
  if (!safeTownId) return ''
  for (const key of Object.keys(world?.towns || {})) {
    const normalized = asText(key, '', 80)
    if (normalized.toLowerCase() === safeTownId) {
      return normalized
    }
  }
  return ''
}

function normalizeTownSpawn(input, { defaultKind = DEFAULT_SPAWN_KIND } = {}) {
  if (!isPlainObject(input)) return null
  const dimension = asText(input.dimension, '', 40)
  const x = asFiniteNumber(input.x)
  const y = asFiniteNumber(input.y)
  const z = asFiniteNumber(input.z)
  if (!dimension || x === null || y === null || z === null) return null

  const spawn = {
    dimension,
    x,
    y,
    z,
    radius: DEFAULT_SPAWN_RADIUS,
    kind: asText(input.kind, defaultKind, 40) || defaultKind
  }

  const yaw = asFiniteNumber(input.yaw)
  const pitch = asFiniteNumber(input.pitch)
  const radius = Number(input.radius)
  if (yaw !== null) spawn.yaw = yaw
  if (pitch !== null) spawn.pitch = pitch
  if (Number.isInteger(radius) && radius >= 0) spawn.radius = radius

  return spawn
}

function normalizeSpawnPolicy(value, fallback = 'explicit_town') {
  const policy = asText(value, '', 40).toLowerCase()
  return PLAYER_SPAWN_POLICIES.has(policy) ? policy : fallback
}

function normalizePlayerAssignment(input, playerIdHint = '') {
  if (!isPlainObject(input)) return null
  const playerId = asText(playerIdHint || input.playerId, '', 120)
  const townId = asText(input.townId, '', 80)
  const assignedAtDay = Number(input.assignedAtDay)
  if (!playerId || !townId) return null
  return {
    playerId,
    townId,
    assignedAtDay: Number.isInteger(assignedAtDay) && assignedAtDay >= 0 ? assignedAtDay : 0,
    spawnPolicy: normalizeSpawnPolicy(input.spawnPolicy)
  }
}

function listStarterTownIds(world) {
  return Object.keys(world?.towns || {})
    .map((entry) => asText(entry, '', 80))
    .filter(Boolean)
    .sort((left, right) => left.localeCompare(right))
}

function selectStarterTownId(world, playerId) {
  const towns = listStarterTownIds(world)
  const safePlayerId = asText(playerId, '', 120)
  if (!towns.length || !safePlayerId) return ''
  const digest = createHash('sha256').update(safePlayerId.toLowerCase()).digest()
  const index = digest.readUInt32BE(0) % towns.length
  return towns[index]
}

function getPlayerAssignment(world, playerId) {
  const safePlayerId = asText(playerId, '', 120).toLowerCase()
  if (!safePlayerId) return null
  for (const [playerKey, entry] of Object.entries(world?.players || {})) {
    const normalized = normalizePlayerAssignment(entry, playerKey)
    if (!normalized) continue
    if (normalized.playerId.toLowerCase() === safePlayerId) {
      return normalized
    }
  }
  return null
}

function resolvePlayerTownId(world, playerId, preferredTownId = '') {
  const explicitTownId = canonicalizeTownId(world, preferredTownId)
  if (explicitTownId) return explicitTownId

  const assignment = getPlayerAssignment(world, playerId)
  const assignedTownId = canonicalizeTownId(world, assignment?.townId)
  if (assignedTownId) return assignedTownId

  return selectStarterTownId(world, playerId)
}

function resolveTownSpawn(world, townId, { fallbackSpawn = DEFAULT_GLOBAL_FALLBACK_SPAWN } = {}) {
  const safeTownId = canonicalizeTownId(world, townId) || asText(townId, '', 80)
  const configuredSpawn = normalizeTownSpawn(world?.towns?.[safeTownId]?.spawn)
  if (configuredSpawn) {
    return {
      townId: safeTownId,
      spawn: configuredSpawn,
      source: 'configured',
      usedFallback: false
    }
  }

  const normalizedFallback = normalizeTownSpawn(fallbackSpawn, { defaultKind: 'global_fallback' })
    || { ...DEFAULT_GLOBAL_FALLBACK_SPAWN }
  return {
    townId: safeTownId,
    spawn: normalizedFallback,
    source: 'global_fallback',
    usedFallback: true
  }
}

function resolvePlayerSpawn(world, {
  playerId,
  preferredTownId = '',
  fallbackSpawn = DEFAULT_GLOBAL_FALLBACK_SPAWN
} = {}) {
  const safePlayerId = asText(playerId, '', 120)
  const assignment = getPlayerAssignment(world, safePlayerId)
  const townId = resolvePlayerTownId(world, safePlayerId, preferredTownId || assignment?.townId || '')
  const townSpawn = resolveTownSpawn(world, townId, { fallbackSpawn })
  return {
    playerId: safePlayerId,
    townId: townSpawn.townId,
    assigned: Boolean(assignment),
    assignment,
    spawn: townSpawn.spawn,
    source: townSpawn.source,
    usedFallback: townSpawn.usedFallback
  }
}

module.exports = {
  DEFAULT_GLOBAL_FALLBACK_SPAWN,
  DEFAULT_SPAWN_DIMENSION,
  DEFAULT_SPAWN_KIND,
  DEFAULT_SPAWN_RADIUS,
  MAX_WORLD_PLAYER_ENTRIES,
  canonicalizeTownId,
  getPlayerAssignment,
  listStarterTownIds,
  normalizePlayerAssignment,
  normalizeSpawnPolicy,
  normalizeTownSpawn,
  resolvePlayerSpawn,
  resolvePlayerTownId,
  resolveTownSpawn,
  selectStarterTownId
}
