const fs = require('fs')
const path = require('path')
const { execFileSync } = require('child_process')

const { AppError } = require('./errors')

const MAX_EXECUTION_HISTORY_ENTRIES = 512
const MAX_EXECUTION_EVENT_LEDGER_ENTRIES = 1024
const MAX_PENDING_EXECUTION_ENTRIES = 128
const MAX_WORLD_MEMORY_QUERY_LIMIT = 200
const CHRONICLE_RECORD_TYPE = 'chronicle-record.v1'
const HISTORY_RECORD_TYPE = 'history-record.v1'
const HISTORY_SUMMARY_SCHEMA_VERSION = 1

function isPlainObject(value) {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value))
}

function asText(value, maxLen = 200) {
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

function appendBounded(list, entry, maxEntries) {
  list.push(entry)
  if (list.length > maxEntries) {
    list.splice(0, list.length - maxEntries)
  }
}

function normalizeQueryLimit(value, defaultLimit = 50, maxLimit = MAX_WORLD_MEMORY_QUERY_LIMIT) {
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed <= 0) {
    return defaultLimit
  }
  return Math.min(parsed, maxLimit)
}

function normalizeScalarRecord(value, maxEntries = 24) {
  if (!isPlainObject(value)) return {}
  const out = {}
  let count = 0
  for (const key of Object.keys(value).sort()) {
    if (count >= maxEntries) break
    const safeKey = asText(key, 80)
    if (!safeKey) continue
    const entryValue = value[key]
    if (entryValue === null) {
      out[safeKey] = null
      count += 1
      continue
    }
    if (typeof entryValue === 'boolean') {
      out[safeKey] = entryValue
      count += 1
      continue
    }
    if (typeof entryValue === 'number' && Number.isFinite(entryValue)) {
      out[safeKey] = Math.trunc(entryValue)
      count += 1
      continue
    }
    if (typeof entryValue === 'string') {
      const safeValue = asText(entryValue, 240)
      if (!safeValue) continue
      out[safeKey] = safeValue
      count += 1
    }
  }
  return out
}

function compareRecordsDesc(left, right) {
  const leftAt = Number(left?.at) || 0
  const rightAt = Number(right?.at) || 0
  if (leftAt !== rightAt) {
    return rightAt - leftAt
  }
  return String(right?.recordId || '').localeCompare(String(left?.recordId || ''))
}

function buildChronicleTags({ entryType, townId, factionId } = {}) {
  const tags = new Set(['chronicle'])
  const safeEntryType = asText(entryType, 80)
  const safeTownId = asText(townId, 80)
  const safeFactionId = asText(factionId, 80)
  if (safeEntryType) tags.add(`type:${safeEntryType.toLowerCase()}`)
  if (safeTownId) tags.add(`town:${safeTownId.toLowerCase()}`)
  if (safeFactionId) tags.add(`faction:${safeFactionId.toLowerCase()}`)
  return Array.from(tags).sort((left, right) => left.localeCompare(right))
}

function createChronicleRecord(entry) {
  const sourceId = asText(entry?.id)
  if (!sourceId) return null
  const meta = normalizeScalarRecord(entry?.meta)
  const entryType = asText(entry?.type, 80)
  const townId = asText(entry?.town, 80) || null
  const factionId = asText(meta.factionId ?? meta.faction, 80) || null
  const at = asNullableInteger(entry?.at) ?? 0
  return {
    type: CHRONICLE_RECORD_TYPE,
    schemaVersion: HISTORY_SUMMARY_SCHEMA_VERSION,
    recordId: `chronicle:${sourceId}`,
    sourceId,
    entryType,
    message: asText(entry?.msg, 240),
    at,
    townId,
    factionId,
    sourceKind: asText(meta.sourceKind ?? meta.origin, 80) || null,
    sourceRefId: asText(
      meta.sourceId
      ?? meta.executionId
      ?? meta.questId
      ?? meta.missionId
      ?? meta.projectId
      ?? meta.salvageRunId,
      200
    ) || null,
    tags: buildChronicleTags({ entryType, townId, factionId }),
    meta
  }
}

function buildHistorySummary({ sourceType, kind, proposalType, status, reasonCode, townId }) {
  return [
    asText(sourceType, 40),
    asText(kind, 80),
    asText(proposalType, 80),
    asText(status, 40),
    asText(reasonCode, 80),
    asText(townId, 80)
  ].filter(Boolean).join(' ')
}

function createReceiptIndex(receipts) {
  const byExecutionId = new Map()
  const byHandoffId = new Map()
  const byIdempotencyKey = new Map()
  for (const receipt of Array.isArray(receipts) ? receipts : []) {
    if (!isPlainObject(receipt)) continue
    if (receipt.executionId) byExecutionId.set(receipt.executionId, receipt)
    if (receipt.handoffId) byHandoffId.set(receipt.handoffId, receipt)
    if (receipt.idempotencyKey) byIdempotencyKey.set(receipt.idempotencyKey, receipt)
  }
  return {
    byExecutionId,
    byHandoffId,
    byIdempotencyKey
  }
}

function resolveReceiptForHistoryEntry(entry, receiptIndex) {
  if (!receiptIndex) return null
  if (entry?.executionId && receiptIndex.byExecutionId.has(entry.executionId)) {
    return receiptIndex.byExecutionId.get(entry.executionId)
  }
  if (entry?.handoffId && receiptIndex.byHandoffId.has(entry.handoffId)) {
    return receiptIndex.byHandoffId.get(entry.handoffId)
  }
  if (entry?.idempotencyKey && receiptIndex.byIdempotencyKey.has(entry.idempotencyKey)) {
    return receiptIndex.byIdempotencyKey.get(entry.idempotencyKey)
  }
  return null
}

function createHistoryRecordFromReceipt(receipt) {
  const executionId = asText(receipt?.executionId)
  if (!executionId) return null
  const townId = asText(receipt?.townId, 80) || null
  const proposalType = asText(receipt?.proposalType, 80) || null
  const status = asText(receipt?.status, 40) || null
  const reasonCode = asText(receipt?.reasonCode, 80) || null
  const at = asNullableInteger(receipt?.postExecutionDecisionEpoch)
    ?? asNullableInteger(receipt?.actualDecisionEpoch)
    ?? 0
  return {
    type: HISTORY_RECORD_TYPE,
    schemaVersion: HISTORY_SUMMARY_SCHEMA_VERSION,
    recordId: `history:receipt:${executionId}`,
    sourceType: 'execution_receipt',
    sourceId: executionId,
    handoffId: asText(receipt?.handoffId),
    idempotencyKey: asText(receipt?.idempotencyKey),
    executionId,
    actorId: asText(receipt?.actorId, 80) || null,
    townId,
    proposalType,
    command: asText(receipt?.command, 240) || null,
    authorityCommands: normalizeAuthorityCommands(receipt?.authorityCommands),
    status,
    reasonCode,
    kind: 'terminal_receipt',
    at,
    snapshotHash: asText(receipt?.postExecutionSnapshotHash, 64)
      || asText(receipt?.actualSnapshotHash, 64)
      || null,
    summary: buildHistorySummary({
      sourceType: 'execution_receipt',
      kind: 'terminal_receipt',
      proposalType,
      status,
      reasonCode,
      townId
    })
  }
}

function createHistoryRecordFromLedgerEntry(entry, receiptIndex) {
  const sourceId = asText(entry?.id, 240)
  if (!sourceId) return null
  const receipt = resolveReceiptForHistoryEntry(entry, receiptIndex)
  const townId = asText(receipt?.townId ?? null, 80) || null
  const proposalType = asText(receipt?.proposalType ?? null, 80) || null
  const status = asText(entry?.status, 40) || asText(receipt?.status, 40) || null
  const reasonCode = asText(entry?.reasonCode, 80) || asText(receipt?.reasonCode, 80) || null
  const kind = asText(entry?.kind, 80) || 'execution_event'
  const at = asNullableInteger(entry?.day) ?? 0
  return {
    type: HISTORY_RECORD_TYPE,
    schemaVersion: HISTORY_SUMMARY_SCHEMA_VERSION,
    recordId: `history:event:${sourceId}`,
    sourceType: 'execution_event',
    sourceId,
    handoffId: asText(entry?.handoffId),
    idempotencyKey: asText(entry?.idempotencyKey),
    executionId: asText(entry?.executionId, 200) || asText(receipt?.executionId, 200) || null,
    actorId: asText(receipt?.actorId, 80) || null,
    townId,
    proposalType,
    command: asText(receipt?.command, 240) || null,
    authorityCommands: normalizeAuthorityCommands(receipt?.authorityCommands),
    status,
    reasonCode,
    kind,
    at,
    snapshotHash: asText(entry?.postExecutionSnapshotHash, 64)
      || asText(entry?.actualSnapshotHash, 64)
      || asText(receipt?.postExecutionSnapshotHash, 64)
      || asText(receipt?.actualSnapshotHash, 64)
      || null,
    summary: buildHistorySummary({
      sourceType: 'execution_event',
      kind,
      proposalType,
      status,
      reasonCode,
      townId
    })
  }
}

function matchesChronicleRecord(record, {
  townId,
  factionId,
  entryType,
  search
} = {}) {
  const safeTownId = asText(townId, 80)
  if (safeTownId && record.townId !== safeTownId) return false
  const safeFactionId = asText(factionId, 80)
  if (safeFactionId && record.factionId !== safeFactionId) return false
  const safeEntryType = asText(entryType, 80)
  if (safeEntryType && record.entryType !== safeEntryType) return false
  const safeSearch = asText(search, 120).toLowerCase()
  if (safeSearch) {
    const haystack = [
      record.message,
      record.entryType,
      record.townId,
      record.factionId,
      record.summary
    ].filter(Boolean).join(' ').toLowerCase()
    if (!haystack.includes(safeSearch)) return false
  }
  return true
}

function matchesHistoryRecord(record, {
  townId,
  status,
  proposalType,
  sourceType,
  kind
} = {}) {
  const safeTownId = asText(townId, 80)
  if (safeTownId && record.townId !== safeTownId) return false
  const safeStatus = asText(status, 40)
  if (safeStatus && record.status !== safeStatus) return false
  const safeProposalType = asText(proposalType, 80)
  if (safeProposalType && record.proposalType !== safeProposalType) return false
  const safeSourceType = asText(sourceType, 40)
  if (safeSourceType && record.sourceType !== safeSourceType) return false
  const safeKind = asText(kind, 80)
  if (safeKind && record.kind !== safeKind) return false
  return true
}

function buildChronicleRecordsFromWorld(world) {
  return (Array.isArray(world?.chronicle) ? world.chronicle : [])
    .map((entry) => createChronicleRecord(entry))
    .filter(Boolean)
    .sort(compareRecordsDesc)
}

function buildHistoryRecords(receipts, ledgerEntries, query = {}) {
  const receiptIndex = createReceiptIndex(receipts)
  const records = []
  for (const receipt of Array.isArray(receipts) ? receipts : []) {
    const record = createHistoryRecordFromReceipt(receipt)
    if (record) records.push(record)
  }
  for (const entry of Array.isArray(ledgerEntries) ? ledgerEntries : []) {
    const record = createHistoryRecordFromLedgerEntry(entry, receiptIndex)
    if (record) records.push(record)
  }
  return records
    .filter((record) => matchesHistoryRecord(record, query))
    .sort(compareRecordsDesc)
    .slice(0, normalizeQueryLimit(query.limit, 50, MAX_EXECUTION_HISTORY_ENTRIES + MAX_EXECUTION_EVENT_LEDGER_ENTRIES))
}

function buildTownHistorySummary(world, chronicleRecords, historyRecords, townId) {
  const townState = isPlainObject(world?.towns?.[townId]) ? world.towns[townId] : null
  const factionIds = Object.entries(world?.factions || {})
    .filter(([, faction]) => Array.isArray(faction?.towns) && faction.towns.includes(townId))
    .map(([factionId]) => factionId)
    .sort((left, right) => left.localeCompare(right))
  const executionCounts = {
    executed: 0,
    rejected: 0,
    stale: 0,
    duplicate: 0,
    failed: 0
  }
  for (const record of historyRecords) {
    if (record.sourceType !== 'execution_receipt') continue
    if (!Object.prototype.hasOwnProperty.call(executionCounts, record.status)) continue
    executionCounts[record.status] += 1
  }
  return {
    type: 'town-history-summary.v1',
    schemaVersion: HISTORY_SUMMARY_SCHEMA_VERSION,
    townId,
    chronicleCount: chronicleRecords.length,
    historyCount: historyRecords.length,
    lastChronicleAt: chronicleRecords[0]?.at ?? null,
    lastHistoryAt: historyRecords[0]?.at ?? null,
    hope: Number.isFinite(Number(townState?.hope)) ? Number(townState.hope) : null,
    dread: Number.isFinite(Number(townState?.dread)) ? Number(townState.dread) : null,
    activeMajorMissionId: asText(townState?.activeMajorMissionId, 200) || null,
    recentImpactCount: Array.isArray(townState?.recentImpacts) ? townState.recentImpacts.length : 0,
    crierQueueDepth: Array.isArray(townState?.crierQueue) ? townState.crierQueue.length : 0,
    activeProjectCount: (Array.isArray(world?.projects) ? world.projects : [])
      .filter((project) => project?.townId === townId && project?.status === 'active')
      .length,
    factions: factionIds,
    executionCounts,
    recentChronicle: chronicleRecords.slice(0, 5),
    recentHistory: historyRecords.slice(0, 5)
  }
}

function buildFactionHistorySummary(world, chronicleRecords, historyRecords, factionId) {
  const factionState = isPlainObject(world?.factions?.[factionId]) ? world.factions[factionId] : null
  const towns = Array.isArray(factionState?.towns)
    ? factionState.towns
      .map((entry) => asText(entry, 80))
      .filter(Boolean)
      .sort((left, right) => left.localeCompare(right))
    : []
  return {
    type: 'faction-history-summary.v1',
    schemaVersion: HISTORY_SUMMARY_SCHEMA_VERSION,
    factionId,
    towns,
    chronicleCount: chronicleRecords.length,
    historyCount: historyRecords.length,
    lastChronicleAt: chronicleRecords[0]?.at ?? null,
    lastHistoryAt: historyRecords[0]?.at ?? null,
    hostilityToPlayer: Number.isFinite(Number(factionState?.hostilityToPlayer))
      ? Number(factionState.hostilityToPlayer)
      : null,
    stability: Number.isFinite(Number(factionState?.stability))
      ? Number(factionState.stability)
      : null,
    doctrine: asText(factionState?.doctrine, 240) || null,
    rivals: Array.isArray(factionState?.rivals)
      ? factionState.rivals.map((entry) => asText(entry, 80)).filter(Boolean).sort((left, right) => left.localeCompare(right))
      : [],
    recentChronicle: chronicleRecords.slice(0, 5),
    recentHistory: historyRecords.slice(0, 5)
  }
}

function ensureExecutionState(world) {
  if (!isPlainObject(world.execution)) {
    world.execution = {
      history: [],
      eventLedger: [],
      pending: []
    }
  }

  if (!Array.isArray(world.execution.history)) {
    world.execution.history = []
  }

  if (!Array.isArray(world.execution.eventLedger)) {
    world.execution.eventLedger = []
  }

  if (!Array.isArray(world.execution.pending)) {
    world.execution.pending = []
  }

  return world.execution
}

function normalizeAuthorityCommands(value) {
  return (Array.isArray(value) ? value : [])
    .map((entry) => asText(entry))
    .filter(Boolean)
}

function matchesExecutionIdentity(entry, { handoffId, idempotencyKey }) {
  const safeHandoffId = asText(handoffId)
  const safeIdempotencyKey = asText(idempotencyKey)
  if (!safeHandoffId && !safeIdempotencyKey) return false

  if (safeHandoffId && entry?.handoffId === safeHandoffId) {
    return true
  }

  if (safeIdempotencyKey && entry?.idempotencyKey === safeIdempotencyKey) {
    return true
  }

  return false
}

function removeMatchingPending(pendingEntries, identity) {
  if (!Array.isArray(pendingEntries) || pendingEntries.length === 0) return 0
  let removed = 0
  for (let index = pendingEntries.length - 1; index >= 0; index -= 1) {
    if (!matchesExecutionIdentity(pendingEntries[index], identity)) continue
    pendingEntries.splice(index, 1)
    removed += 1
  }
  return removed
}

function findMatchingReceipt(history, { handoffId, idempotencyKey }) {
  const safeHandoffId = asText(handoffId)
  const safeIdempotencyKey = asText(idempotencyKey)
  if (!safeHandoffId && !safeIdempotencyKey) return null

  for (let index = history.length - 1; index >= 0; index -= 1) {
    const receipt = history[index]
    if (!isPlainObject(receipt)) continue
    if (safeHandoffId && receipt.handoffId === safeHandoffId) {
      return cloneValue(receipt)
    }
    if (safeIdempotencyKey && receipt.idempotencyKey === safeIdempotencyKey) {
      return cloneValue(receipt)
    }
  }

  return null
}

function findMatchingPending(pendingEntries, identity) {
  if (!Array.isArray(pendingEntries)) return null
  for (let index = pendingEntries.length - 1; index >= 0; index -= 1) {
    const pending = pendingEntries[index]
    if (!isPlainObject(pending)) continue
    if (matchesExecutionIdentity(pending, identity)) {
      return cloneValue(pending)
    }
  }
  return null
}

function createReceiptFromResult(result) {
  return {
    type: asText(result?.type, 80),
    schemaVersion: Number(result?.schemaVersion) || 0,
    executionId: asText(result?.executionId),
    resultId: asText(result?.resultId),
    handoffId: asText(result?.handoffId),
    proposalId: asText(result?.proposalId),
    idempotencyKey: asText(result?.idempotencyKey),
    actorId: asText(result?.actorId, 80),
    townId: asText(result?.townId, 80),
    proposalType: asText(result?.proposalType, 80),
    command: asText(result?.command, 240),
    authorityCommands: normalizeAuthorityCommands(result?.authorityCommands),
    status: asText(result?.status, 40),
    accepted: result?.accepted === true,
    executed: result?.executed === true,
    reasonCode: asText(result?.reasonCode, 80),
    actualSnapshotHash: asText(result?.evaluation?.staleCheck?.actualSnapshotHash, 64) || null,
    actualDecisionEpoch: asNullableInteger(result?.evaluation?.staleCheck?.actualDecisionEpoch),
    postExecutionSnapshotHash: asText(result?.worldState?.postExecutionSnapshotHash, 64) || null,
    postExecutionDecisionEpoch: asNullableInteger(result?.worldState?.postExecutionDecisionEpoch)
  }
}

function createLedgerEntryFromResult(result, kind) {
  return {
    id: `${asText(result?.executionId)}:${kind}`,
    kind,
    handoffId: asText(result?.handoffId),
    idempotencyKey: asText(result?.idempotencyKey),
    executionId: asText(result?.executionId) || null,
    status: asText(result?.status, 40),
    reasonCode: asText(result?.reasonCode, 80),
    day: asNullableInteger(result?.worldState?.postExecutionDecisionEpoch)
      ?? asNullableInteger(result?.evaluation?.staleCheck?.actualDecisionEpoch)
      ?? 0,
    actualSnapshotHash: asText(result?.evaluation?.staleCheck?.actualSnapshotHash, 64) || null,
    postExecutionSnapshotHash: asText(result?.worldState?.postExecutionSnapshotHash, 64) || null
  }
}

function createPendingExecutionRecord({
  handoff,
  proposalType,
  actorId,
  townId,
  authorityCommands,
  beforeProjection
}) {
  return {
    pendingId: asText(handoff?.handoffId),
    handoffId: asText(handoff?.handoffId),
    proposalId: asText(handoff?.proposalId),
    idempotencyKey: asText(handoff?.idempotencyKey),
    actorId: asText(actorId, 80),
    townId: asText(townId, 80),
    proposalType: asText(proposalType, 80),
    command: asText(handoff?.command, 240),
    authorityCommands: normalizeAuthorityCommands(authorityCommands),
    status: 'pending',
    preparedSnapshotHash: asText(beforeProjection?.snapshotHash, 64) || null,
    preparedDecisionEpoch: asNullableInteger(beforeProjection?.decisionEpoch),
    lastKnownSnapshotHash: asText(beforeProjection?.snapshotHash, 64) || null,
    lastKnownDecisionEpoch: asNullableInteger(beforeProjection?.decisionEpoch),
    totalCommandCount: normalizeAuthorityCommands(authorityCommands).length,
    completedCommandCount: 0,
    lastAppliedCommand: null
  }
}

function createPendingIdentity(entry) {
  return {
    handoffId: entry?.handoffId,
    idempotencyKey: entry?.idempotencyKey
  }
}

function validatePersistenceBackend(backend) {
  if (!backend || typeof backend !== 'object') return false
  return (
    typeof backend.findReceipt === 'function'
    && typeof backend.findPendingExecution === 'function'
    && typeof backend.listPendingExecutions === 'function'
    && typeof backend.stagePendingExecution === 'function'
    && typeof backend.markPendingExecutionProgress === 'function'
    && typeof backend.clearPendingExecution === 'function'
    && typeof backend.recordResult === 'function'
    && typeof backend.syncWorldMemoryFromSnapshot === 'function'
    && typeof backend.listChronicleRecords === 'function'
    && typeof backend.listHistoryRecords === 'function'
  )
}

function createMemoryExecutionPersistence({ memoryStore, logger } = {}) {
  if (!memoryStore || typeof memoryStore.recallWorld !== 'function' || typeof memoryStore.transact !== 'function') {
    throw new AppError({
      code: 'EXECUTION_STORE_CONFIG_ERROR',
      message: 'memoryStore dependency is required.',
      recoverable: false
    })
  }

  const safeLogger = logger || { info: () => {}, warn: () => {} }

  function findReceipt(input) {
    const world = memoryStore.recallWorld()
    const execution = ensureExecutionState(world)
    return findMatchingReceipt(execution.history, input || {})
  }

  function findPendingExecution(input) {
    const world = memoryStore.recallWorld()
    const execution = ensureExecutionState(world)
    return findMatchingPending(execution.pending, input || {})
  }

  function listPendingExecutions() {
    const world = memoryStore.recallWorld()
    const execution = ensureExecutionState(world)
    return execution.pending.map((entry) => cloneValue(entry))
  }

  async function stagePendingExecution(input) {
    const pendingEntry = createPendingExecutionRecord(input || {})
    if (!pendingEntry.pendingId || !pendingEntry.handoffId || !pendingEntry.idempotencyKey) {
      throw new AppError({
        code: 'EXECUTION_STORE_INVALID_PENDING',
        message: 'Pending execution identity is required.',
        recoverable: false
      })
    }

    const tx = await memoryStore.transact((memory) => {
      const execution = ensureExecutionState(memory.world)
      removeMatchingPending(execution.pending, createPendingIdentity(pendingEntry))
      appendBounded(execution.pending, pendingEntry, MAX_PENDING_EXECUTION_ENTRIES)
      return {
        pendingSize: execution.pending.length
      }
    }, { eventId: `execution-store:pending:stage:${pendingEntry.handoffId}` })

    if (!tx.skipped) {
      safeLogger.info('execution_store_pending_staged', {
        backend: 'memory',
        handoffId: pendingEntry.handoffId,
        idempotencyKey: pendingEntry.idempotencyKey,
        pendingSize: tx.result?.pendingSize
      })
    }

    return pendingEntry
  }

  async function markPendingExecutionProgress({
    handoffId,
    idempotencyKey,
    completedCommandCount,
    lastAppliedCommand,
    lastKnownSnapshotHash,
    lastKnownDecisionEpoch
  } = {}) {
    const identity = { handoffId, idempotencyKey }
    const safeHandoffId = asText(handoffId)
    if (!safeHandoffId) {
      throw new AppError({
        code: 'EXECUTION_STORE_INVALID_PENDING',
        message: 'handoffId is required to update pending execution progress.',
        recoverable: false
      })
    }

    const tx = await memoryStore.transact((memory) => {
      const execution = ensureExecutionState(memory.world)
      let updated = null
      for (const pendingEntry of execution.pending) {
        if (!matchesExecutionIdentity(pendingEntry, identity)) continue
        pendingEntry.completedCommandCount = Math.max(
          0,
          Math.min(
            Number.isInteger(completedCommandCount) ? completedCommandCount : 0,
            Number.isInteger(pendingEntry.totalCommandCount) ? pendingEntry.totalCommandCount : 0
          )
        )
        pendingEntry.lastAppliedCommand = asText(lastAppliedCommand, 240) || null
        pendingEntry.lastKnownSnapshotHash = asText(lastKnownSnapshotHash, 64) || pendingEntry.lastKnownSnapshotHash || null
        pendingEntry.lastKnownDecisionEpoch = asNullableInteger(lastKnownDecisionEpoch)
          ?? pendingEntry.lastKnownDecisionEpoch
        updated = cloneValue(pendingEntry)
        break
      }
      return updated
    }, { eventId: `execution-store:pending:progress:${safeHandoffId}:${Number(completedCommandCount) || 0}` })

    if (!tx.skipped && tx.result) {
      safeLogger.info('execution_store_pending_progress', {
        backend: 'memory',
        handoffId: safeHandoffId,
        completedCommandCount: tx.result.completedCommandCount,
        totalCommandCount: tx.result.totalCommandCount
      })
    }

    return tx.result || findPendingExecution(identity)
  }

  async function clearPendingExecution(identity, { kind = 'pending_clear' } = {}) {
    const safeHandoffId = asText(identity?.handoffId)
    if (!safeHandoffId && !asText(identity?.idempotencyKey)) {
      return 0
    }

    const tx = await memoryStore.transact((memory) => {
      const execution = ensureExecutionState(memory.world)
      return removeMatchingPending(execution.pending, identity)
    }, { eventId: `execution-store:${kind}:${safeHandoffId || asText(identity?.idempotencyKey)}` })

    if (!tx.skipped && Number(tx.result) > 0) {
      safeLogger.info('execution_store_pending_cleared', {
        backend: 'memory',
        handoffId: safeHandoffId || null,
        idempotencyKey: asText(identity?.idempotencyKey) || null,
        kind,
        removed: tx.result
      })
    }

    return Number(tx.result) || 0
  }

  async function recordResult(
    result,
    { kind = `result:${asText(result?.status, 40)}`, persistReceipt = true, clearPending = true } = {}
  ) {
    const safeExecutionId = asText(result?.executionId)
    if (!safeExecutionId) {
      throw new AppError({
        code: 'EXECUTION_STORE_INVALID_RESULT',
        message: 'executionId is required to persist a result.',
        recoverable: false
      })
    }

    const receipt = createReceiptFromResult(result)
    const ledgerEntry = createLedgerEntryFromResult(result, kind)

    const tx = await memoryStore.transact((memory) => {
      const execution = ensureExecutionState(memory.world)
      if (persistReceipt) {
        appendBounded(execution.history, receipt, MAX_EXECUTION_HISTORY_ENTRIES)
      }
      appendBounded(execution.eventLedger, ledgerEntry, MAX_EXECUTION_EVENT_LEDGER_ENTRIES)
      const clearedPendingCount = clearPending
        ? removeMatchingPending(execution.pending, {
          handoffId: receipt.handoffId,
          idempotencyKey: receipt.idempotencyKey
        })
        : 0
      return {
        historySize: execution.history.length,
        eventLedgerSize: execution.eventLedger.length,
        clearedPendingCount
      }
    }, { eventId: `execution-store:${kind}:${safeExecutionId}` })

    if (!tx.skipped) {
      safeLogger.info('execution_store_result_recorded', {
        backend: 'memory',
        executionId: safeExecutionId,
        handoffId: receipt.handoffId,
        kind,
        persistReceipt,
        clearPending,
        historySize: tx.result?.historySize,
        eventLedgerSize: tx.result?.eventLedgerSize,
        clearedPendingCount: tx.result?.clearedPendingCount
      })
    }

    return receipt
  }

  function syncWorldMemoryFromSnapshot(world) {
    return {
      backend: 'memory',
      chronicleCount: buildChronicleRecordsFromWorld(world).length
    }
  }

  function listChronicleRecords(query = {}) {
    const world = memoryStore.recallWorld()
    return buildChronicleRecordsFromWorld(world)
      .filter((record) => matchesChronicleRecord(record, query))
      .slice(0, normalizeQueryLimit(query.limit))
  }

  function listHistoryRecords(query = {}) {
    const world = memoryStore.recallWorld()
    const execution = ensureExecutionState(world)
    return buildHistoryRecords(execution.history, execution.eventLedger, query)
  }

  return {
    backendName: 'memory',
    findPendingExecution,
    findReceipt,
    listPendingExecutions,
    listChronicleRecords,
    listHistoryRecords,
    markPendingExecutionProgress,
    recordResult,
    clearPendingExecution,
    stagePendingExecution,
    syncWorldMemoryFromSnapshot
  }
}

function createSqliteExecutionPersistence(options = {}) {
  const dbPath = path.resolve(String(options.dbPath || ''))
  const sqliteCommand = asText(options.sqliteCommand || 'sqlite3', 200) || 'sqlite3'
  const safeLogger = options.logger || { info: () => {}, warn: () => {} }
  const fsModule = options.fsModule || fs
  const now = typeof options.now === 'function' ? options.now : () => Date.now()

  if (!dbPath) {
    throw new AppError({
      code: 'EXECUTION_STORE_CONFIG_ERROR',
      message: 'SQLite dbPath is required.',
      recoverable: false
    })
  }

  let initialized = false

  function sqlValue(value) {
    if (value === null || value === undefined) return 'NULL'
    if (typeof value === 'boolean') return value ? '1' : '0'
    if (typeof value === 'number') return Number.isFinite(value) ? String(Math.trunc(value)) : 'NULL'
    const text = String(value)
    return `'${text.replace(/'/g, "''")}'`
  }

  function runSql(sql, { json = false } = {}) {
    try {
      const dirPath = path.dirname(dbPath)
      if (dirPath && dirPath !== '.') {
        fsModule.mkdirSync(dirPath, { recursive: true })
      }

      const args = ['-bail', '-cmd', '.timeout 5000']
      if (json) args.push('-json')
      args.push(dbPath, sql)
      const stdout = execFileSync(sqliteCommand, args, {
        encoding: 'utf8',
        windowsHide: true
      })
      if (!json) {
        return stdout
      }

      if (!stdout.trim()) {
        return []
      }

      const parsed = JSON.parse(stdout)
      if (Array.isArray(parsed)) {
        return parsed
      }
      if (parsed && typeof parsed === 'object') {
        return [parsed]
      }
      return []
    } catch (error) {
      throw new AppError({
        code: 'EXECUTION_STORE_SQLITE_ERROR',
        message: 'SQLite execution persistence command failed.',
        recoverable: false,
        metadata: {
          dbPath,
          sqliteCommand,
          error: error instanceof Error ? error.message : String(error)
        }
      })
    }
  }

  function ensureInitialized() {
    if (initialized) return

    runSql(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      CREATE TABLE IF NOT EXISTS execution_receipts (
        execution_id TEXT PRIMARY KEY,
        handoff_id TEXT NOT NULL UNIQUE,
        idempotency_key TEXT NOT NULL UNIQUE,
        proposal_id TEXT NOT NULL,
        actor_id TEXT NOT NULL,
        town_id TEXT NOT NULL,
        proposal_type TEXT NOT NULL,
        status TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_execution_receipts_status ON execution_receipts(status, created_at DESC);
      CREATE TABLE IF NOT EXISTS execution_pending (
        pending_id TEXT PRIMARY KEY,
        handoff_id TEXT NOT NULL UNIQUE,
        idempotency_key TEXT NOT NULL UNIQUE,
        proposal_id TEXT NOT NULL,
        status TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_execution_pending_updated_at ON execution_pending(updated_at DESC);
      CREATE TABLE IF NOT EXISTS execution_event_ledger (
        event_id TEXT PRIMARY KEY,
        handoff_id TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        execution_id TEXT,
        kind TEXT NOT NULL,
        status TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_execution_event_ledger_handoff ON execution_event_ledger(handoff_id, created_at DESC);
      CREATE TABLE IF NOT EXISTS world_chronicle_records (
        record_id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL UNIQUE,
        entry_type TEXT NOT NULL,
        town_id TEXT,
        faction_id TEXT,
        at INTEGER NOT NULL,
        message TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_world_chronicle_records_at ON world_chronicle_records(at DESC, record_id DESC);
      CREATE INDEX IF NOT EXISTS idx_world_chronicle_records_town ON world_chronicle_records(town_id, at DESC);
      CREATE INDEX IF NOT EXISTS idx_world_chronicle_records_faction ON world_chronicle_records(faction_id, at DESC);
    `)

    initialized = true
    safeLogger.info('execution_store_sqlite_initialized', {
      backend: 'sqlite',
      dbPath
    })
  }

  function findReceipt(input) {
    ensureInitialized()
    const safeHandoffId = asText(input?.handoffId)
    const safeIdempotencyKey = asText(input?.idempotencyKey)
    if (!safeHandoffId && !safeIdempotencyKey) return null

    const rows = runSql(`
      SELECT payload_json
      FROM execution_receipts
      WHERE handoff_id = ${sqlValue(safeHandoffId)} OR idempotency_key = ${sqlValue(safeIdempotencyKey)}
      ORDER BY created_at DESC
      LIMIT 1;
    `, { json: true })

    if (!Array.isArray(rows) || rows.length === 0) return null
    return cloneValue(JSON.parse(rows[0].payload_json))
  }

  function findPendingExecution(input) {
    ensureInitialized()
    const safeHandoffId = asText(input?.handoffId)
    const safeIdempotencyKey = asText(input?.idempotencyKey)
    if (!safeHandoffId && !safeIdempotencyKey) return null

    const rows = runSql(`
      SELECT payload_json
      FROM execution_pending
      WHERE handoff_id = ${sqlValue(safeHandoffId)} OR idempotency_key = ${sqlValue(safeIdempotencyKey)}
      ORDER BY updated_at DESC
      LIMIT 1;
    `, { json: true })

    if (!Array.isArray(rows) || rows.length === 0) return null
    return cloneValue(JSON.parse(rows[0].payload_json))
  }

  function listPendingExecutions() {
    ensureInitialized()
    const rows = runSql(`
      SELECT payload_json
      FROM execution_pending
      ORDER BY updated_at ASC, pending_id ASC;
    `, { json: true })
    return rows.map((row) => cloneValue(JSON.parse(row.payload_json)))
  }

  async function stagePendingExecution(input) {
    ensureInitialized()
    const pendingEntry = createPendingExecutionRecord(input || {})
    if (!pendingEntry.pendingId || !pendingEntry.handoffId || !pendingEntry.idempotencyKey) {
      throw new AppError({
        code: 'EXECUTION_STORE_INVALID_PENDING',
        message: 'Pending execution identity is required.',
        recoverable: false
      })
    }

    const createdAt = Math.trunc(now())
    runSql(`
      BEGIN IMMEDIATE;
      DELETE FROM execution_pending
      WHERE handoff_id = ${sqlValue(pendingEntry.handoffId)} OR idempotency_key = ${sqlValue(pendingEntry.idempotencyKey)};
      INSERT INTO execution_pending (
        pending_id,
        handoff_id,
        idempotency_key,
        proposal_id,
        status,
        payload_json,
        created_at,
        updated_at
      ) VALUES (
        ${sqlValue(pendingEntry.pendingId)},
        ${sqlValue(pendingEntry.handoffId)},
        ${sqlValue(pendingEntry.idempotencyKey)},
        ${sqlValue(pendingEntry.proposalId)},
        ${sqlValue(pendingEntry.status)},
        ${sqlValue(JSON.stringify(pendingEntry))},
        ${sqlValue(createdAt)},
        ${sqlValue(createdAt)}
      );
      COMMIT;
    `)

    safeLogger.info('execution_store_pending_staged', {
      backend: 'sqlite',
      handoffId: pendingEntry.handoffId,
      dbPath
    })
    return pendingEntry
  }

  async function markPendingExecutionProgress({
    handoffId,
    idempotencyKey,
    completedCommandCount,
    lastAppliedCommand,
    lastKnownSnapshotHash,
    lastKnownDecisionEpoch
  } = {}) {
    ensureInitialized()
    const identity = { handoffId, idempotencyKey }
    const current = findPendingExecution(identity)
    if (!current) return null

    current.completedCommandCount = Math.max(
      0,
      Math.min(
        Number.isInteger(completedCommandCount) ? completedCommandCount : 0,
        Number.isInteger(current.totalCommandCount) ? current.totalCommandCount : 0
      )
    )
    current.lastAppliedCommand = asText(lastAppliedCommand, 240) || null
    current.lastKnownSnapshotHash = asText(lastKnownSnapshotHash, 64) || current.lastKnownSnapshotHash || null
    current.lastKnownDecisionEpoch = asNullableInteger(lastKnownDecisionEpoch) ?? current.lastKnownDecisionEpoch

    const updatedAt = Math.trunc(now())
    runSql(`
      UPDATE execution_pending
      SET payload_json = ${sqlValue(JSON.stringify(current))},
          updated_at = ${sqlValue(updatedAt)}
      WHERE handoff_id = ${sqlValue(asText(handoffId))} OR idempotency_key = ${sqlValue(asText(idempotencyKey))};
    `)

    safeLogger.info('execution_store_pending_progress', {
      backend: 'sqlite',
      handoffId: asText(handoffId),
      completedCommandCount: current.completedCommandCount,
      totalCommandCount: current.totalCommandCount,
      dbPath
    })

    return current
  }

  async function clearPendingExecution(identity, { kind = 'pending_clear' } = {}) {
    ensureInitialized()
    const current = findPendingExecution(identity)
    if (!current) return 0

    runSql(`
      DELETE FROM execution_pending
      WHERE handoff_id = ${sqlValue(asText(identity?.handoffId))} OR idempotency_key = ${sqlValue(asText(identity?.idempotencyKey))};
    `)

    safeLogger.info('execution_store_pending_cleared', {
      backend: 'sqlite',
      handoffId: current.handoffId,
      kind,
      dbPath
    })
    return 1
  }

  async function recordResult(
    result,
    { kind = `result:${asText(result?.status, 40)}`, persistReceipt = true, clearPending = true } = {}
  ) {
    ensureInitialized()
    const receipt = createReceiptFromResult(result)
    const ledgerEntry = createLedgerEntryFromResult(result, kind)
    const createdAt = Math.trunc(now())

    const statements = [
      'BEGIN IMMEDIATE;'
    ]
    if (persistReceipt) {
      statements.push(`
        INSERT OR REPLACE INTO execution_receipts (
          execution_id,
          handoff_id,
          idempotency_key,
          proposal_id,
          actor_id,
          town_id,
          proposal_type,
          status,
          reason_code,
          payload_json,
          created_at
        ) VALUES (
          ${sqlValue(receipt.executionId)},
          ${sqlValue(receipt.handoffId)},
          ${sqlValue(receipt.idempotencyKey)},
          ${sqlValue(receipt.proposalId)},
          ${sqlValue(receipt.actorId)},
          ${sqlValue(receipt.townId)},
          ${sqlValue(receipt.proposalType)},
          ${sqlValue(receipt.status)},
          ${sqlValue(receipt.reasonCode)},
          ${sqlValue(JSON.stringify(receipt))},
          ${sqlValue(createdAt)}
        );
      `)
    }
    statements.push(`
      INSERT OR REPLACE INTO execution_event_ledger (
        event_id,
        handoff_id,
        idempotency_key,
        execution_id,
        kind,
        status,
        reason_code,
        payload_json,
        created_at
      ) VALUES (
        ${sqlValue(ledgerEntry.id)},
        ${sqlValue(ledgerEntry.handoffId)},
        ${sqlValue(ledgerEntry.idempotencyKey)},
        ${sqlValue(ledgerEntry.executionId)},
        ${sqlValue(ledgerEntry.kind)},
        ${sqlValue(ledgerEntry.status)},
        ${sqlValue(ledgerEntry.reasonCode)},
        ${sqlValue(JSON.stringify(ledgerEntry))},
        ${sqlValue(createdAt)}
      );
    `)
    if (clearPending) {
      statements.push(`
        DELETE FROM execution_pending
        WHERE handoff_id = ${sqlValue(receipt.handoffId)} OR idempotency_key = ${sqlValue(receipt.idempotencyKey)};
      `)
    }
    statements.push('COMMIT;')

    runSql(statements.join('\n'))

    safeLogger.info('execution_store_result_recorded', {
      backend: 'sqlite',
      executionId: receipt.executionId,
      handoffId: receipt.handoffId,
      kind,
      persistReceipt,
      clearPending,
      dbPath
    })
    return receipt
  }

  function syncWorldMemoryFromSnapshot(world) {
    ensureInitialized()
    const records = buildChronicleRecordsFromWorld(world)
    const timestamp = Math.trunc(now())
    const statements = [
      'BEGIN IMMEDIATE;',
      'DELETE FROM world_chronicle_records;'
    ]
    for (const record of records) {
      statements.push(`
        INSERT OR REPLACE INTO world_chronicle_records (
          record_id,
          source_id,
          entry_type,
          town_id,
          faction_id,
          at,
          message,
          payload_json,
          created_at,
          updated_at
        ) VALUES (
          ${sqlValue(record.recordId)},
          ${sqlValue(record.sourceId)},
          ${sqlValue(record.entryType)},
          ${sqlValue(record.townId)},
          ${sqlValue(record.factionId)},
          ${sqlValue(record.at)},
          ${sqlValue(record.message)},
          ${sqlValue(JSON.stringify(record))},
          ${sqlValue(timestamp)},
          ${sqlValue(timestamp)}
        );
      `)
    }
    statements.push('COMMIT;')
    runSql(statements.join('\n'))

    safeLogger.info('execution_store_world_memory_synced', {
      backend: 'sqlite',
      chronicleCount: records.length,
      dbPath
    })

    return {
      backend: 'sqlite',
      chronicleCount: records.length
    }
  }

  function listChronicleRecords(query = {}) {
    ensureInitialized()
    const safeTownId = asText(query?.townId, 80)
    const safeFactionId = asText(query?.factionId, 80)
    const safeEntryType = asText(query?.entryType, 80)
    const safeSearch = asText(query?.search, 120).toLowerCase()
    const clauses = []
    if (safeTownId) clauses.push(`town_id = ${sqlValue(safeTownId)}`)
    if (safeFactionId) clauses.push(`faction_id = ${sqlValue(safeFactionId)}`)
    if (safeEntryType) clauses.push(`entry_type = ${sqlValue(safeEntryType)}`)
    if (safeSearch) {
      const likeValue = `%${safeSearch.replace(/[%_]/g, '')}%`
      clauses.push(`(
        LOWER(message) LIKE ${sqlValue(likeValue)}
        OR LOWER(entry_type) LIKE ${sqlValue(likeValue)}
        OR LOWER(COALESCE(town_id, '')) LIKE ${sqlValue(likeValue)}
        OR LOWER(COALESCE(faction_id, '')) LIKE ${sqlValue(likeValue)}
      )`)
    }
    const rows = runSql(`
      SELECT payload_json
      FROM world_chronicle_records
      ${clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : ''}
      ORDER BY at DESC, record_id DESC
      LIMIT ${sqlValue(normalizeQueryLimit(query.limit))};
    `, { json: true })

    return rows
      .map((row) => cloneValue(JSON.parse(row.payload_json)))
      .filter((record) => matchesChronicleRecord(record, query))
      .sort(compareRecordsDesc)
      .slice(0, normalizeQueryLimit(query.limit))
  }

  function listHistoryRecords(query = {}) {
    ensureInitialized()
    const receiptRows = runSql(`
      SELECT payload_json
      FROM execution_receipts
      ORDER BY created_at DESC;
    `, { json: true })
    const ledgerRows = runSql(`
      SELECT payload_json
      FROM execution_event_ledger
      ORDER BY created_at DESC;
    `, { json: true })
    const receipts = receiptRows.map((row) => cloneValue(JSON.parse(row.payload_json)))
    const ledgerEntries = ledgerRows.map((row) => cloneValue(JSON.parse(row.payload_json)))
    return buildHistoryRecords(receipts, ledgerEntries, query)
  }

  return {
    backendName: 'sqlite',
    clearPendingExecution,
    findPendingExecution,
    findReceipt,
    initialize: ensureInitialized,
    listChronicleRecords,
    listHistoryRecords,
    listPendingExecutions,
    markPendingExecutionProgress,
    recordResult,
    stagePendingExecution,
    syncWorldMemoryFromSnapshot
  }
}

function createExecutionPersistenceBackend({
  backend = 'memory',
  memoryStore,
  sqliteDbPath,
  sqliteCommand,
  logger,
  now
} = {}) {
  const normalizedBackend = asText(String(backend || 'memory').toLowerCase(), 40) || 'memory'
  if (normalizedBackend === 'sqlite') {
    return createSqliteExecutionPersistence({
      dbPath: sqliteDbPath,
      sqliteCommand,
      logger,
      now
    })
  }

  return createMemoryExecutionPersistence({
    memoryStore,
    logger
  })
}

function createExecutionStore({
  memoryStore,
  logger,
  persistenceBackend,
  backend = 'memory',
  sqliteDbPath,
  sqliteCommand,
  now
} = {}) {
  if (!memoryStore || typeof memoryStore.recallWorld !== 'function') {
    throw new AppError({
      code: 'EXECUTION_STORE_CONFIG_ERROR',
      message: 'memoryStore dependency is required.',
      recoverable: false
    })
  }

  const safeLogger = logger || { info: () => {}, warn: () => {} }
  const resolvedBackend = persistenceBackend || createExecutionPersistenceBackend({
    backend,
    memoryStore,
    sqliteDbPath,
    sqliteCommand,
    logger: safeLogger,
    now
  })

  if (!validatePersistenceBackend(resolvedBackend)) {
    throw new AppError({
      code: 'EXECUTION_STORE_CONFIG_ERROR',
      message: 'Execution persistence backend is invalid.',
      recoverable: false
    })
  }

  if (typeof resolvedBackend.initialize === 'function') {
    resolvedBackend.initialize()
  }

  function syncWorldMemory() {
    return resolvedBackend.syncWorldMemoryFromSnapshot(memoryStore.recallWorld())
  }

  function listChronicleRecords(query = {}) {
    syncWorldMemory()
    return resolvedBackend.listChronicleRecords(query || {})
  }

  function listHistoryRecords(query = {}) {
    return resolvedBackend.listHistoryRecords(query || {})
  }

  function getTownHistorySummary({ townId, chronicleLimit = 5, historyLimit = 5 } = {}) {
    const safeTownId = asText(townId, 80)
    if (!safeTownId) {
      throw new AppError({
        code: 'EXECUTION_STORE_INVALID_WORLD_MEMORY_QUERY',
        message: 'townId is required.',
        recoverable: false
      })
    }
    const world = memoryStore.recallWorld()
    syncWorldMemory()
    const chronicleRecords = resolvedBackend.listChronicleRecords({
      townId: safeTownId,
      limit: MAX_WORLD_MEMORY_QUERY_LIMIT
    })
    const historyRecords = resolvedBackend.listHistoryRecords({
      townId: safeTownId,
      limit: MAX_EXECUTION_HISTORY_ENTRIES + MAX_EXECUTION_EVENT_LEDGER_ENTRIES
    })
    const summary = buildTownHistorySummary(world, chronicleRecords, historyRecords, safeTownId)
    summary.recentChronicle = chronicleRecords.slice(0, normalizeQueryLimit(chronicleLimit, 5, 20))
    summary.recentHistory = historyRecords.slice(0, normalizeQueryLimit(historyLimit, 5, 20))
    return summary
  }

  function getFactionHistorySummary({ factionId, chronicleLimit = 5, historyLimit = 5 } = {}) {
    const safeFactionId = asText(factionId, 80)
    if (!safeFactionId) {
      throw new AppError({
        code: 'EXECUTION_STORE_INVALID_WORLD_MEMORY_QUERY',
        message: 'factionId is required.',
        recoverable: false
      })
    }
    const world = memoryStore.recallWorld()
    const faction = isPlainObject(world?.factions?.[safeFactionId]) ? world.factions[safeFactionId] : null
    const towns = Array.isArray(faction?.towns)
      ? faction.towns.map((entry) => asText(entry, 80)).filter(Boolean)
      : []
    syncWorldMemory()
    const chronicleRecords = resolvedBackend.listChronicleRecords({
      factionId: safeFactionId,
      limit: MAX_WORLD_MEMORY_QUERY_LIMIT
    })
    const townChronicleRecords = towns.flatMap((townId) => resolvedBackend.listChronicleRecords({
      townId,
      limit: MAX_WORLD_MEMORY_QUERY_LIMIT
    }))
    const mergedChronicleRecords = Array.from(new Map(
      [...chronicleRecords, ...townChronicleRecords].map((record) => [record.recordId, record])
    ).values()).sort(compareRecordsDesc)
    const townHistoryRecords = towns.flatMap((townId) => resolvedBackend.listHistoryRecords({
      townId,
      limit: MAX_EXECUTION_HISTORY_ENTRIES + MAX_EXECUTION_EVENT_LEDGER_ENTRIES
    }))
    const mergedHistoryRecords = Array.from(new Map(
      townHistoryRecords.map((record) => [record.recordId, record])
    ).values()).sort(compareRecordsDesc)
    const summary = buildFactionHistorySummary(world, mergedChronicleRecords, mergedHistoryRecords, safeFactionId)
    summary.recentChronicle = mergedChronicleRecords.slice(0, normalizeQueryLimit(chronicleLimit, 5, 20))
    summary.recentHistory = mergedHistoryRecords.slice(0, normalizeQueryLimit(historyLimit, 5, 20))
    return summary
  }

  return {
    backendName: resolvedBackend.backendName || asText(backend, 40) || 'memory',
    findPendingExecution(input) {
      return resolvedBackend.findPendingExecution(input || {})
    },
    findReceipt(input) {
      return resolvedBackend.findReceipt(input || {})
    },
    listPendingExecutions() {
      return resolvedBackend.listPendingExecutions()
    },
    listChronicleRecords,
    listHistoryRecords,
    readSnapshotSource() {
      return memoryStore.recallWorld()
    },
    syncWorldMemory,
    getTownHistorySummary,
    getFactionHistorySummary,
    async stagePendingExecution(input) {
      return resolvedBackend.stagePendingExecution(input || {})
    },
    async markPendingExecutionProgress(input) {
      return resolvedBackend.markPendingExecutionProgress(input || {})
    },
    async clearPendingExecution(identity, opts) {
      return resolvedBackend.clearPendingExecution(identity || {}, opts || {})
    },
    async recordResult(result, opts) {
      return resolvedBackend.recordResult(result, opts || {})
    }
  }
}

module.exports = {
  MAX_EXECUTION_EVENT_LEDGER_ENTRIES,
  MAX_EXECUTION_HISTORY_ENTRIES,
  MAX_PENDING_EXECUTION_ENTRIES,
  createExecutionPersistenceBackend,
  createExecutionStore,
  createMemoryExecutionPersistence,
  createSqliteExecutionPersistence
}
