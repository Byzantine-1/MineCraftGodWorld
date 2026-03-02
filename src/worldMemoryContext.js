const WORLD_MEMORY_REQUEST_TYPE = 'world-memory-request.v1'
const WORLD_MEMORY_REQUEST_SCHEMA_VERSION = 1
const WORLD_MEMORY_CONTEXT_TYPE = 'world-memory-context.v1'
const WORLD_MEMORY_CONTEXT_SCHEMA_VERSION = 1
const MAX_CONTEXT_CHRONICLE_RECORDS = 5
const MAX_CONTEXT_HISTORY_RECORDS = 5

function isPlainObject(value) {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value))
}

function asText(value, maxLen = 240) {
  if (typeof value !== 'string') return ''
  const trimmed = value.trim()
  if (!trimmed) return ''
  return trimmed.slice(0, maxLen)
}

function asNullableInteger(value) {
  const parsed = Number(value)
  return Number.isInteger(parsed) ? parsed : null
}

function cloneValue(value) {
  if (typeof structuredClone === 'function') return structuredClone(value)
  return JSON.parse(JSON.stringify(value))
}

function normalizeLimit(value, defaultValue, maxValue) {
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed <= 0) return defaultValue
  return Math.min(parsed, maxValue)
}

function normalizeStringList(values, maxEntries = 12, maxLen = 80) {
  return (Array.isArray(values) ? values : [])
    .map((value) => asText(value, maxLen))
    .filter(Boolean)
    .slice(0, maxEntries)
}

function isValidScopeField(value) {
  return value === null || (typeof value === 'string' && value.trim().length > 0)
}

function isValidLimit(value, maxValue) {
  return Number.isInteger(value) && value >= 1 && value <= maxValue
}

function hasOnlyKeys(value, expectedKeys) {
  return Object.keys(value).every((key) => expectedKeys.includes(key))
}

function normalizeWorldMemoryScope({
  townId,
  factionId,
  chronicleLimit,
  historyLimit
} = {}) {
  return {
    townId: asText(townId, 80) || null,
    factionId: asText(factionId, 80) || null,
    chronicleLimit: normalizeLimit(chronicleLimit, 3, MAX_CONTEXT_CHRONICLE_RECORDS),
    historyLimit: normalizeLimit(historyLimit, 4, MAX_CONTEXT_HISTORY_RECORDS)
  }
}

function isValidWorldMemoryScope(scope) {
  return Boolean(
    isPlainObject(scope) &&
    hasOnlyKeys(scope, ['townId', 'factionId', 'chronicleLimit', 'historyLimit']) &&
    isValidScopeField(scope.townId) &&
    isValidScopeField(scope.factionId) &&
    isValidLimit(scope.chronicleLimit, MAX_CONTEXT_CHRONICLE_RECORDS) &&
    isValidLimit(scope.historyLimit, MAX_CONTEXT_HISTORY_RECORDS)
  )
}

function normalizeExecutionCounts(value) {
  const source = isPlainObject(value) ? value : {}
  return {
    executed: Number(source.executed) || 0,
    rejected: Number(source.rejected) || 0,
    stale: Number(source.stale) || 0,
    duplicate: Number(source.duplicate) || 0,
    failed: Number(source.failed) || 0
  }
}

function createWorldMemoryRequest({
  townId,
  factionId,
  chronicleLimit,
  historyLimit
} = {}) {
  return {
    type: WORLD_MEMORY_REQUEST_TYPE,
    schemaVersion: WORLD_MEMORY_REQUEST_SCHEMA_VERSION,
    scope: normalizeWorldMemoryScope({
      townId,
      factionId,
      chronicleLimit,
      historyLimit
    })
  }
}

function isValidWorldMemoryRequest(request) {
  return Boolean(
    isPlainObject(request) &&
    hasOnlyKeys(request, ['type', 'schemaVersion', 'scope']) &&
    request.type === WORLD_MEMORY_REQUEST_TYPE &&
    request.schemaVersion === WORLD_MEMORY_REQUEST_SCHEMA_VERSION &&
    isValidWorldMemoryScope(request.scope)
  )
}

function parseWorldMemoryRequestLine(line) {
  if (typeof line !== 'string') {
    return null
  }

  const trimmed = line.trim()
  if (!trimmed.startsWith('{')) {
    return null
  }

  let parsed
  try {
    parsed = JSON.parse(trimmed)
  } catch {
    return null
  }

  if (parsed?.type !== WORLD_MEMORY_REQUEST_TYPE) {
    return null
  }

  if (!isValidWorldMemoryRequest(parsed)) {
    return null
  }

  return createWorldMemoryRequest(parsed.scope)
}

function projectChronicleRecord(record) {
  return {
    sourceRecordId: asText(record?.recordId),
    entryType: asText(record?.entryType, 80),
    message: asText(record?.message, 240),
    at: asNullableInteger(record?.at) ?? 0,
    townId: asText(record?.townId, 80) || null,
    factionId: asText(record?.factionId, 80) || null,
    sourceRefId: asText(record?.sourceRefId, 200) || null,
    tags: normalizeStringList(record?.tags, 12, 80)
  }
}

function projectHistoryRecord(record) {
  return {
    sourceType: asText(record?.sourceType, 40),
    handoffId: asText(record?.handoffId, 200) || null,
    proposalType: asText(record?.proposalType, 80) || null,
    command: asText(record?.command, 240) || null,
    authorityCommands: normalizeStringList(record?.authorityCommands, 8, 240),
    status: asText(record?.status, 40),
    reasonCode: asText(record?.reasonCode, 80),
    kind: asText(record?.kind, 80),
    at: asNullableInteger(record?.at) ?? 0,
    townId: asText(record?.townId, 80) || null,
    summary: asText(record?.summary, 320)
  }
}

function projectTownSummary(summary) {
  if (!isPlainObject(summary)) return null
  return {
    type: asText(summary.type, 80),
    schemaVersion: Number(summary.schemaVersion) || 0,
    townId: asText(summary.townId, 80),
    chronicleCount: Number(summary.chronicleCount) || 0,
    historyCount: Number(summary.historyCount) || 0,
    lastChronicleAt: asNullableInteger(summary.lastChronicleAt),
    lastHistoryAt: asNullableInteger(summary.lastHistoryAt),
    hope: Number.isFinite(Number(summary.hope)) ? Number(summary.hope) : null,
    dread: Number.isFinite(Number(summary.dread)) ? Number(summary.dread) : null,
    activeMajorMissionId: asText(summary.activeMajorMissionId, 200) || null,
    recentImpactCount: Number(summary.recentImpactCount) || 0,
    crierQueueDepth: Number(summary.crierQueueDepth) || 0,
    activeProjectCount: Number(summary.activeProjectCount) || 0,
    factions: normalizeStringList(summary.factions, 12, 80),
    executionCounts: normalizeExecutionCounts(summary.executionCounts)
  }
}

function projectFactionSummary(summary) {
  if (!isPlainObject(summary)) return null
  return {
    type: asText(summary.type, 80),
    schemaVersion: Number(summary.schemaVersion) || 0,
    factionId: asText(summary.factionId, 80),
    towns: normalizeStringList(summary.towns, 12, 80),
    chronicleCount: Number(summary.chronicleCount) || 0,
    historyCount: Number(summary.historyCount) || 0,
    lastChronicleAt: asNullableInteger(summary.lastChronicleAt),
    lastHistoryAt: asNullableInteger(summary.lastHistoryAt),
    hostilityToPlayer: Number.isFinite(Number(summary.hostilityToPlayer)) ? Number(summary.hostilityToPlayer) : null,
    stability: Number.isFinite(Number(summary.stability)) ? Number(summary.stability) : null,
    doctrine: asText(summary.doctrine, 240) || null,
    rivals: normalizeStringList(summary.rivals, 12, 80)
  }
}

function normalizeChronicleSelection(records, limit) {
  return (Array.isArray(records) ? records : [])
    .map((record) => projectChronicleRecord(record))
    .slice(0, limit)
}

function normalizeHistorySelection(records, limit) {
  return (Array.isArray(records) ? records : [])
    .map((record) => projectHistoryRecord(record))
    .sort((left, right) => {
      if (left.at !== right.at) return right.at - left.at
      return [
        right.sourceType,
        right.kind,
        right.proposalType || '',
        right.status,
        right.handoffId || ''
      ].join(':').localeCompare([
        left.sourceType,
        left.kind,
        left.proposalType || '',
        left.status,
        left.handoffId || ''
      ].join(':'))
    })
    .slice(0, limit)
}

function createScopedSelection(executionStore, {
  townId,
  factionId,
  chronicleLimit,
  historyLimit
}) {
  const scope = normalizeWorldMemoryScope({
    townId,
    factionId,
    chronicleLimit,
    historyLimit
  })
  const safeTownId = scope.townId
  const safeFactionId = scope.factionId
  const normalizedChronicleLimit = scope.chronicleLimit
  const normalizedHistoryLimit = scope.historyLimit

  const townSummary = safeTownId
    ? executionStore.getTownHistorySummary({
      townId: safeTownId,
      chronicleLimit: normalizedChronicleLimit,
      historyLimit: normalizedHistoryLimit
    })
    : null
  const factionSummary = safeFactionId
    ? executionStore.getFactionHistorySummary({
      factionId: safeFactionId,
      chronicleLimit: normalizedChronicleLimit,
      historyLimit: normalizedHistoryLimit
    })
    : null

  let chronicleRecords
  let historyRecords
  const selectionHistoryWindow = Math.max(normalizedHistoryLimit * 4, MAX_CONTEXT_HISTORY_RECORDS)

  if (safeTownId) {
    chronicleRecords = executionStore.listChronicleRecords({
      townId: safeTownId,
      ...(safeFactionId ? { factionId: safeFactionId } : {}),
      limit: normalizedChronicleLimit
    })
    historyRecords = executionStore.listHistoryRecords({
      townId: safeTownId,
      limit: selectionHistoryWindow
    })
  } else if (factionSummary) {
    chronicleRecords = Array.isArray(factionSummary.recentChronicle) ? factionSummary.recentChronicle : []
    historyRecords = Array.isArray(factionSummary.recentHistory) ? factionSummary.recentHistory : []
  } else {
    chronicleRecords = executionStore.listChronicleRecords({ limit: normalizedChronicleLimit })
    historyRecords = executionStore.listHistoryRecords({ limit: normalizedHistoryLimit })
  }

  return {
    scope,
    recentChronicle: normalizeChronicleSelection(chronicleRecords, normalizedChronicleLimit),
    recentHistory: normalizeHistorySelection(historyRecords, normalizedHistoryLimit),
    townSummary: projectTownSummary(townSummary),
    factionSummary: projectFactionSummary(factionSummary)
  }
}

function createWorldMemoryContext({
  executionStore,
  townId,
  factionId,
  chronicleLimit,
  historyLimit
} = {}) {
  if (
    !executionStore
    || typeof executionStore.listChronicleRecords !== 'function'
    || typeof executionStore.listHistoryRecords !== 'function'
    || typeof executionStore.getTownHistorySummary !== 'function'
    || typeof executionStore.getFactionHistorySummary !== 'function'
  ) {
    throw new Error('executionStore world-memory query surface is required')
  }

  const selection = createScopedSelection(executionStore, {
    townId,
    factionId,
    chronicleLimit,
    historyLimit
  })

  return cloneValue({
    type: WORLD_MEMORY_CONTEXT_TYPE,
    schemaVersion: WORLD_MEMORY_CONTEXT_SCHEMA_VERSION,
    scope: selection.scope,
    recentChronicle: selection.recentChronicle,
    recentHistory: selection.recentHistory,
    ...(selection.townSummary ? { townSummary: selection.townSummary } : {}),
    ...(selection.factionSummary ? { factionSummary: selection.factionSummary } : {})
  })
}

function createWorldMemoryContextForRequest({
  executionStore,
  request
} = {}) {
  if (!isValidWorldMemoryRequest(request)) {
    throw new Error('Invalid world memory request')
  }

  return createWorldMemoryContext({
    executionStore,
    ...request.scope
  })
}

module.exports = {
  MAX_CONTEXT_CHRONICLE_RECORDS,
  MAX_CONTEXT_HISTORY_RECORDS,
  WORLD_MEMORY_CONTEXT_SCHEMA_VERSION,
  WORLD_MEMORY_CONTEXT_TYPE,
  WORLD_MEMORY_REQUEST_SCHEMA_VERSION,
  WORLD_MEMORY_REQUEST_TYPE,
  createWorldMemoryContext,
  createWorldMemoryContextForRequest,
  createWorldMemoryRequest,
  isValidWorldMemoryRequest,
  parseWorldMemoryRequestLine
}
