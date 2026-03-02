const fs = require('fs')
const path = require('path')
const { execFileSync } = require('child_process')

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

  return {
    backendName: 'memory',
    findPendingExecution,
    findReceipt,
    listPendingExecutions,
    markPendingExecutionProgress,
    recordResult,
    clearPendingExecution,
    stagePendingExecution
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

  return {
    backendName: 'sqlite',
    clearPendingExecution,
    findPendingExecution,
    findReceipt,
    initialize: ensureInitialized,
    listPendingExecutions,
    markPendingExecutionProgress,
    recordResult,
    stagePendingExecution
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
    readSnapshotSource() {
      return memoryStore.recallWorld()
    },
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
