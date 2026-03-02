const OFFICEHOLDER_ROLE_ORDER = Object.freeze(['mayor', 'captain', 'warden'])
const OFFICEHOLDER_ROLE_SET = new Set(OFFICEHOLDER_ROLE_ORDER)

function asText(value, fallback = '', maxLen = 80) {
  if (typeof value !== 'string') return fallback
  const trimmed = value.trim()
  return trimmed ? trimmed.slice(0, maxLen) : fallback
}

function normalizeTownId(value) {
  return asText(value, '', 80)
}

function normalizeActorId(value) {
  return asText(value, '', 120)
}

function normalizeRole(value) {
  return asText(value, '', 40).toLowerCase()
}

function normalizeStatus(value, fallback) {
  const normalized = asText(value, '', 24).toLowerCase()
  return normalized || fallback
}

function titleCaseIdentifier(value) {
  return String(value || '')
    .split(/[_\-. ]+/)
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

function defaultTownNameFromId(townId) {
  const safeTownId = normalizeTownId(townId)
  return safeTownId ? titleCaseIdentifier(safeTownId) : ''
}

function defaultActorName({ role, townName }) {
  const safeRole = normalizeRole(role)
  const safeTownName = asText(townName, '', 80)
  if (!safeRole || !safeTownName) return ''
  if (safeRole === 'townsfolk') return `Townsfolk of ${safeTownName}`
  return `${titleCaseIdentifier(safeRole)} of ${safeTownName}`
}

function projectTownRecord(townId, input) {
  const safeTownId = normalizeTownId(townId || input?.townId)
  if (!safeTownId) return null
  return {
    townId: safeTownId,
    name: asText(input?.name, defaultTownNameFromId(safeTownId), 80),
    status: normalizeStatus(input?.status, 'active'),
    region: asText(input?.region, '', 80) || null,
    tags: (Array.isArray(input?.tags) ? input.tags : [])
      .map((value) => asText(value, '', 80))
      .filter(Boolean)
      .slice(0, 12)
      .sort((left, right) => left.localeCompare(right))
  }
}

function projectActorRecord(actorId, input) {
  const safeActorId = normalizeActorId(actorId || input?.actorId)
  const safeTownId = normalizeTownId(input?.townId)
  if (!safeActorId || !safeTownId) return null
  return {
    actorId: safeActorId,
    townId: safeTownId,
    name: asText(input?.name, '', 80),
    role: normalizeRole(input?.role),
    status: normalizeStatus(input?.status, 'active')
  }
}

function compareTownRecords(left, right) {
  return left.townId.localeCompare(right.townId)
}

function compareActorRecords(left, right) {
  if (left.townId !== right.townId) {
    return left.townId.localeCompare(right.townId)
  }
  if (left.role !== right.role) {
    return left.role.localeCompare(right.role)
  }
  return left.actorId.localeCompare(right.actorId)
}

function compareOfficeholders(left, right) {
  const leftIndex = OFFICEHOLDER_ROLE_ORDER.indexOf(left.role)
  const rightIndex = OFFICEHOLDER_ROLE_ORDER.indexOf(right.role)
  if (leftIndex !== rightIndex) {
    return leftIndex - rightIndex
  }
  return compareActorRecords(left, right)
}

function listTownRecords(world) {
  return Object.entries(world?.towns || {})
    .map(([townId, value]) => projectTownRecord(townId, value))
    .filter(Boolean)
    .sort(compareTownRecords)
}

function getTownRecord(world, townId) {
  const safeTownId = normalizeTownId(townId).toLowerCase()
  if (!safeTownId) return null
  return listTownRecords(world).find((record) => record.townId.toLowerCase() === safeTownId) || null
}

function listActorRecords(world, { townId, actorId, role, status } = {}) {
  const safeTownId = normalizeTownId(townId).toLowerCase()
  const safeActorId = normalizeActorId(actorId).toLowerCase()
  const safeRole = normalizeRole(role)
  const safeStatus = normalizeStatus(status, '')
  return Object.entries(world?.actors || {})
    .map(([entryActorId, value]) => projectActorRecord(entryActorId, value))
    .filter(Boolean)
    .filter((record) => (!safeTownId || record.townId.toLowerCase() === safeTownId))
    .filter((record) => (!safeActorId || record.actorId.toLowerCase() === safeActorId))
    .filter((record) => (!safeRole || record.role === safeRole))
    .filter((record) => (!safeStatus || record.status === safeStatus))
    .sort(compareActorRecords)
}

function getActorRecord(world, actorId) {
  const safeActorId = normalizeActorId(actorId)
  if (!safeActorId) return null
  return listActorRecords(world, { actorId: safeActorId })[0] || null
}

function listTownOfficeholders(world, townId) {
  return listActorRecords(world, { townId, status: 'active' })
    .filter((record) => OFFICEHOLDER_ROLE_SET.has(record.role))
    .sort(compareOfficeholders)
}

function createWorldRegistryStore({ memoryStore } = {}) {
  if (!memoryStore || typeof memoryStore.recallWorld !== 'function') {
    throw new Error('memoryStore dependency is required.')
  }

  return {
    listTowns() {
      return listTownRecords(memoryStore.recallWorld())
    },
    getTown(townId) {
      return getTownRecord(memoryStore.recallWorld(), townId)
    },
    listActors(query = {}) {
      return listActorRecords(memoryStore.recallWorld(), query)
    },
    getActor(actorId) {
      return getActorRecord(memoryStore.recallWorld(), actorId)
    },
    listTownOfficeholders(townId) {
      return listTownOfficeholders(memoryStore.recallWorld(), townId)
    }
  }
}

module.exports = {
  OFFICEHOLDER_ROLE_ORDER,
  defaultActorName,
  defaultTownNameFromId,
  createWorldRegistryStore,
  getActorRecord,
  getTownRecord,
  listActorRecords,
  listTownOfficeholders,
  listTownRecords,
  projectActorRecord,
  projectTownRecord
}
