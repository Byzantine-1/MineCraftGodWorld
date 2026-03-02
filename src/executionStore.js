const { AppError } = require('./errors')

const MAX_EXECUTION_HISTORY_ENTRIES = 512
const MAX_EXECUTION_EVENT_LEDGER_ENTRIES = 1024

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
      eventLedger: []
    }
  }

  if (!Array.isArray(world.execution.history)) {
    world.execution.history = []
  }

  if (!Array.isArray(world.execution.eventLedger)) {
    world.execution.eventLedger = []
  }

  return world.execution
}

function normalizeAuthorityCommands(value) {
  return (Array.isArray(value) ? value : [])
    .map((entry) => asText(entry))
    .filter(Boolean)
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

  async function recordResult(result, { kind = `result:${asText(result?.status, 40)}`, persistReceipt = true } = {}) {
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
      return {
        historySize: execution.history.length,
        eventLedgerSize: execution.eventLedger.length
      }
    }, { eventId: `execution-store:${kind}:${safeExecutionId}` })

    if (!tx.skipped) {
      safeLogger.info('execution_store_result_recorded', {
        executionId: safeExecutionId,
        handoffId: receipt.handoffId,
        kind,
        persistReceipt,
        historySize: tx.result?.historySize,
        eventLedgerSize: tx.result?.eventLedgerSize
      })
    }

    return receipt
  }

  return {
    findReceipt,
    readSnapshotSource,
    recordResult
  }
}

module.exports = {
  MAX_EXECUTION_EVENT_LEDGER_ENTRIES,
  MAX_EXECUTION_HISTORY_ENTRIES,
  createExecutionStore
}
