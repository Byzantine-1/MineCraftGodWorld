const { AppError } = require('./errors')

const MAX_EXECUTION_HISTORY_ENTRIES = 512
const MAX_EXECUTION_EVENT_LEDGER_ENTRIES = 1024
const MAX_PENDING_EXECUTION_ENTRIES = 128

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

function createExecutionStore({ memoryStore, logger } = {}) {
  if (!memoryStore || typeof memoryStore.recallWorld !== 'function' || typeof memoryStore.transact !== 'function') {
    throw new AppError({
      code: 'EXECUTION_STORE_CONFIG_ERROR',
      message: 'memoryStore dependency is required.',
      recoverable: false
    })
  }

  const safeLogger = logger || { info: () => {}, warn: () => {} }

  function readSnapshotSource() {
    return memoryStore.recallWorld()
  }

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

  return {
    clearPendingExecution,
    findPendingExecution,
    findReceipt,
    listPendingExecutions,
    markPendingExecutionProgress,
    readSnapshotSource,
    recordResult,
    stagePendingExecution
  }
}

module.exports = {
  MAX_EXECUTION_EVENT_LEDGER_ENTRIES,
  MAX_EXECUTION_HISTORY_ENTRIES,
  MAX_PENDING_EXECUTION_ENTRIES,
  createExecutionStore
}
