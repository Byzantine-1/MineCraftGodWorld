const fs = require('fs')
const path = require('path')
const { execFileSync } = require('child_process')

const { AppError } = require('./errors')
const { createAuthoritativeSnapshotProjection } = require('./worldSnapshotProjection')
const {
  getActorRecord,
  getTownRecord,
  listActorRecords,
  listTownOfficeholders,
  listTownRecords,
  projectActorRecord,
  projectTownRecord
} = require('./worldRegistry')

const WORLD_STATE_SCHEMA_VERSION = 1
const WORLD_STATE_MIGRATION_META_KEY = 'world_state.migration.v1'
const OFFICEHOLDER_ROLES = Object.freeze(['mayor', 'captain', 'warden'])

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

function sqlValue(value) {
  if (value === null || value === undefined) return 'NULL'
  if (typeof value === 'number') return Number.isFinite(value) ? String(Math.trunc(value)) : 'NULL'
  if (typeof value === 'boolean') return value ? '1' : '0'
  return `'${String(value).replace(/'/g, "''")}'`
}

function resolveNow(now) {
  return typeof now === 'function' ? now : () => Date.now()
}

function normalizeTownTags(tags) {
  return (Array.isArray(tags) ? tags : [])
    .map((entry) => asText(entry, 80))
    .filter(Boolean)
    .slice(0, 12)
    .sort((left, right) => left.localeCompare(right))
}

function normalizeActorMetadata(actor) {
  if (!isPlainObject(actor)) return {}
  const metadata = {}
  for (const [key, value] of Object.entries(actor)) {
    if (['actorId', 'townId', 'name', 'role', 'status'].includes(key)) continue
    if (value === null) {
      metadata[key] = null
      continue
    }
    if (typeof value === 'boolean') {
      metadata[key] = value
      continue
    }
    if (typeof value === 'number' && Number.isFinite(value)) {
      metadata[key] = Math.trunc(value)
      continue
    }
    if (typeof value === 'string') {
      const safeValue = asText(value, 240)
      if (!safeValue) continue
      metadata[key] = safeValue
      continue
    }
  }
  return metadata
}

function buildTownRowsFromWorld(world) {
  const sourceWorld = isPlainObject(world) ? world : {}
  return listTownRecords(sourceWorld).map((townRecord) => {
    const townState = isPlainObject(sourceWorld.towns?.[townRecord.townId]) ? sourceWorld.towns[townRecord.townId] : {}
    return {
      townId: townRecord.townId,
      name: townRecord.name,
      status: townRecord.status,
      region: townRecord.region || null,
      tags: normalizeTownTags(townRecord.tags),
      state: cloneValue(townState)
    }
  })
}

function buildActorRowsFromWorld(world) {
  const sourceWorld = isPlainObject(world) ? world : {}
  return listActorRecords(sourceWorld).map((actorRecord) => {
    const rawActor = isPlainObject(sourceWorld.actors?.[actorRecord.actorId]) ? sourceWorld.actors[actorRecord.actorId] : {}
    return {
      actorId: actorRecord.actorId,
      townId: actorRecord.townId,
      name: actorRecord.name,
      role: actorRecord.role,
      status: actorRecord.status,
      metadata: normalizeActorMetadata(rawActor)
    }
  })
}

function mergeActorRowIntoState(row) {
  const projected = projectActorRecord(row?.actorId, row)
  if (!projected) return null
  return {
    ...projected,
    ...normalizeActorMetadata(isPlainObject(row?.metadata) ? row.metadata : {})
  }
}

function mergeTownRowIntoState(row) {
  const projected = projectTownRecord(row?.townId, row)
  if (!projected) return null
  const rawState = isPlainObject(row?.state) ? row.state : {}
  return {
    ...rawState,
    ...projected,
    tags: normalizeTownTags(projected.tags)
  }
}

function createMemoryWorldStateStore({ memoryStore } = {}) {
  if (!memoryStore || typeof memoryStore.recallWorld !== 'function' || typeof memoryStore.transact !== 'function') {
    throw new AppError({
      code: 'WORLD_STATE_STORE_CONFIG_ERROR',
      message: 'memoryStore dependency with recallWorld/transact is required.',
      recoverable: false
    })
  }

  function loadWorldSnapshot() {
    return cloneValue(memoryStore.recallWorld())
  }

  function listTownStateRecords() {
    return buildTownRowsFromWorld(loadWorldSnapshot())
      .map((row) => mergeTownRowIntoState(row))
      .filter(Boolean)
  }

  async function replaceWorldSnapshot(world, { persist = true } = {}) {
    const sourceWorld = isPlainObject(world) ? world : {}
    await memoryStore.transact((memory) => {
      memory.world = cloneValue(sourceWorld)
    }, { persist })
    return loadWorldSnapshot()
  }

  return {
    backendName: 'memory',
    initialize() {},
    hasWorldSnapshot() {
      return true
    },
    loadWorldSnapshot,
    readWorldSnapshot: loadWorldSnapshot,
    async replaceWorldSnapshot(world, options = {}) {
      return replaceWorldSnapshot(world, options)
    },
    listTowns() {
      return listTownStateRecords()
    },
    getTown(townId) {
      const safeTownId = asText(townId, 80)
      if (!safeTownId) return null
      return listTownStateRecords().find((entry) => asText(entry?.townId, 80).toLowerCase() === safeTownId.toLowerCase()) || null
    },
    listActorsByTown(townId) {
      return listActorRecords(loadWorldSnapshot(), { townId })
    },
    getActor(actorId) {
      return getActorRecord(loadWorldSnapshot(), actorId)
    },
    listOfficeholders(townId) {
      return listTownOfficeholders(loadWorldSnapshot(), townId)
    },
    async upsertTown(townRecord, { persist = true } = {}) {
      const projected = projectTownRecord(townRecord?.townId, townRecord)
      if (!projected) return null
      await memoryStore.transact((memory) => {
        memory.world.towns = isPlainObject(memory.world.towns) ? memory.world.towns : {}
        const existing = isPlainObject(memory.world.towns[projected.townId]) ? memory.world.towns[projected.townId] : {}
        memory.world.towns[projected.townId] = {
          ...existing,
          ...projected
        }
      }, { persist })
      return getTownRecord(loadWorldSnapshot(), projected.townId)
    },
    async upsertActor(actorRecord, { persist = true } = {}) {
      const projected = projectActorRecord(actorRecord?.actorId, actorRecord)
      if (!projected) return null
      await memoryStore.transact((memory) => {
        memory.world.actors = isPlainObject(memory.world.actors) ? memory.world.actors : {}
        const existing = isPlainObject(memory.world.actors[projected.actorId]) ? memory.world.actors[projected.actorId] : {}
        memory.world.actors[projected.actorId] = {
          ...existing,
          ...projected
        }
      }, { persist })
      return getActorRecord(loadWorldSnapshot(), projected.actorId)
    },
    async applyWorldPatch(patch = {}, { persist = true } = {}) {
      const sourcePatch = isPlainObject(patch) ? patch : {}
      await memoryStore.transact((memory) => {
        for (const [key, value] of Object.entries(sourcePatch)) {
          memory.world[key] = cloneValue(value)
        }
      }, { persist })
      return loadWorldSnapshot()
    },
    getMeta() {
      return null
    },
    setMeta() {
      return null
    },
    buildReplaceWorldStatements() {
      return []
    }
  }
}

function createSqliteWorldStateStore({
  dbPath,
  sqliteCommand = 'sqlite3',
  logger,
  now
} = {}) {
  const safeLogger = logger || { info: () => {}, warn: () => {} }
  const safeNow = resolveNow(now)
  const resolvedDbPath = asText(dbPath, 400) || path.resolve(__dirname, './execution.sqlite3')
  const resolvedSqliteCommand = asText(sqliteCommand, 200) || 'sqlite3'

  let initialized = false

  function ensureParentDirectory() {
    const dir = path.dirname(resolvedDbPath)
    fs.mkdirSync(dir, { recursive: true })
  }

  function runSql(sql, { json = false } = {}) {
    ensureParentDirectory()
    const args = []
    if (json) args.push('-json')
    args.push(resolvedDbPath)
    try {
      const stdout = execFileSync(resolvedSqliteCommand, args, {
        encoding: 'utf8',
        windowsHide: true,
        input: `${String(sql || '')}\n`,
        maxBuffer: 10 * 1024 * 1024
      })
      if (!json) return stdout
      if (!stdout || !stdout.trim()) return []
      return JSON.parse(stdout)
    } catch (error) {
      throw new AppError({
        code: 'WORLD_STATE_SQLITE_ERROR',
        message: 'SQLite world-state operation failed.',
        recoverable: false,
        metadata: {
          dbPath: resolvedDbPath,
          sqliteCommand: resolvedSqliteCommand,
          error: error instanceof Error ? error.message : String(error)
        }
      })
    }
  }

  function buildReplaceWorldStatements(world, { timestamp } = {}) {
    const sourceWorld = isPlainObject(world) ? world : {}
    const ts = Number.isInteger(timestamp) ? timestamp : Math.trunc(safeNow())
    const projection = createAuthoritativeSnapshotProjection(sourceWorld)
    const towns = buildTownRowsFromWorld(sourceWorld)
    const actors = buildActorRowsFromWorld(sourceWorld)
    const statements = [
      `INSERT OR REPLACE INTO world_state_snapshots (
        snapshot_id,
        payload_json,
        snapshot_hash,
        decision_epoch,
        created_at,
        updated_at
      ) VALUES (
        1,
        ${sqlValue(JSON.stringify(sourceWorld))},
        ${sqlValue(projection.snapshotHash)},
        ${sqlValue(asNullableInteger(projection.decisionEpoch))},
        ${sqlValue(ts)},
        ${sqlValue(ts)}
      );`,
      'DELETE FROM world_towns;',
      'DELETE FROM world_actors;'
    ]

    for (const town of towns) {
      statements.push(`INSERT OR REPLACE INTO world_towns (
        town_id,
        name,
        status,
        region,
        tags_json,
        state_json,
        created_at,
        updated_at
      ) VALUES (
        ${sqlValue(town.townId)},
        ${sqlValue(town.name)},
        ${sqlValue(town.status)},
        ${sqlValue(town.region)},
        ${sqlValue(JSON.stringify(town.tags))},
        ${sqlValue(JSON.stringify(town.state))},
        ${sqlValue(ts)},
        ${sqlValue(ts)}
      );`)
    }

    for (const actor of actors) {
      statements.push(`INSERT OR REPLACE INTO world_actors (
        actor_id,
        town_id,
        name,
        role,
        status,
        metadata_json,
        created_at,
        updated_at
      ) VALUES (
        ${sqlValue(actor.actorId)},
        ${sqlValue(actor.townId)},
        ${sqlValue(actor.name)},
        ${sqlValue(actor.role)},
        ${sqlValue(actor.status)},
        ${sqlValue(JSON.stringify(actor.metadata))},
        ${sqlValue(ts)},
        ${sqlValue(ts)}
      );`)
    }

    statements.push(`INSERT OR REPLACE INTO world_meta (
      meta_key,
      meta_value,
      updated_at
    ) VALUES (
      'world_state.schema_version',
      ${sqlValue(String(WORLD_STATE_SCHEMA_VERSION))},
      ${sqlValue(ts)}
    );`)

    return statements
  }

  function ensureInitialized() {
    if (initialized) return

    runSql(`
      PRAGMA journal_mode=WAL;
      PRAGMA foreign_keys=ON;
      CREATE TABLE IF NOT EXISTS world_state_snapshots (
        snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
        payload_json TEXT NOT NULL,
        snapshot_hash TEXT,
        decision_epoch INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS world_towns (
        town_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        status TEXT NOT NULL,
        region TEXT,
        tags_json TEXT NOT NULL,
        state_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS world_actors (
        actor_id TEXT PRIMARY KEY,
        town_id TEXT NOT NULL,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        status TEXT NOT NULL,
        metadata_json TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_world_actors_town_role ON world_actors(town_id, role, actor_id);
      CREATE INDEX IF NOT EXISTS idx_world_actors_status ON world_actors(status, role, actor_id);
      CREATE TABLE IF NOT EXISTS world_meta (
        meta_key TEXT PRIMARY KEY,
        meta_value TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      );
    `)

    initialized = true
    safeLogger.info('world_state_store_sqlite_initialized', {
      backend: 'sqlite',
      dbPath: resolvedDbPath
    })
  }

  function hasWorldSnapshot() {
    ensureInitialized()
    const rows = runSql(`
      SELECT COUNT(*) AS count
      FROM world_state_snapshots
      WHERE snapshot_id = 1;
    `, { json: true })
    return Number(rows?.[0]?.count || 0) > 0
  }

  function loadWorldSnapshot() {
    ensureInitialized()
    const rows = runSql(`
      SELECT payload_json
      FROM world_state_snapshots
      WHERE snapshot_id = 1
      LIMIT 1;
    `, { json: true })
    if (!Array.isArray(rows) || rows.length === 0) return null
    try {
      return cloneValue(JSON.parse(rows[0].payload_json))
    } catch {
      return null
    }
  }

  function setMeta(key, value) {
    ensureInitialized()
    const safeKey = asText(key, 120)
    if (!safeKey) return null
    const ts = Math.trunc(safeNow())
    runSql(`
      INSERT OR REPLACE INTO world_meta (
        meta_key,
        meta_value,
        updated_at
      ) VALUES (
        ${sqlValue(safeKey)},
        ${sqlValue(String(value ?? ''))},
        ${sqlValue(ts)}
      );
    `)
    return {
      key: safeKey,
      value: String(value ?? '')
    }
  }

  function getMeta(key) {
    ensureInitialized()
    const safeKey = asText(key, 120)
    if (!safeKey) return null
    const rows = runSql(`
      SELECT meta_key, meta_value
      FROM world_meta
      WHERE meta_key = ${sqlValue(safeKey)}
      LIMIT 1;
    `, { json: true })
    if (!Array.isArray(rows) || rows.length === 0) return null
    return {
      key: rows[0].meta_key,
      value: rows[0].meta_value
    }
  }

  function parseTownRow(row) {
    if (!isPlainObject(row)) return null
    let tags = []
    let state = {}
    try {
      tags = normalizeTownTags(JSON.parse(row.tags_json || '[]'))
    } catch {
      tags = []
    }
    try {
      const parsedState = JSON.parse(row.state_json || '{}')
      state = isPlainObject(parsedState) ? parsedState : {}
    } catch {
      state = {}
    }
    return mergeTownRowIntoState({
      townId: row.town_id,
      name: row.name,
      status: row.status,
      region: row.region,
      tags,
      state
    })
  }

  function parseActorRow(row) {
    if (!isPlainObject(row)) return null
    let metadata = {}
    try {
      const parsedMetadata = JSON.parse(row.metadata_json || '{}')
      metadata = isPlainObject(parsedMetadata) ? parsedMetadata : {}
    } catch {
      metadata = {}
    }
    return mergeActorRowIntoState({
      actorId: row.actor_id,
      townId: row.town_id,
      name: row.name,
      role: row.role,
      status: row.status,
      metadata
    })
  }

  function listTowns() {
    ensureInitialized()
    const rows = runSql(`
      SELECT town_id, name, status, region, tags_json, state_json
      FROM world_towns
      ORDER BY town_id ASC;
    `, { json: true })
    return rows.map((row) => parseTownRow(row)).filter(Boolean)
  }

  function getTown(townId) {
    ensureInitialized()
    const safeTownId = asText(townId, 80)
    if (!safeTownId) return null
    const rows = runSql(`
      SELECT town_id, name, status, region, tags_json, state_json
      FROM world_towns
      WHERE town_id = ${sqlValue(safeTownId)}
      LIMIT 1;
    `, { json: true })
    if (!Array.isArray(rows) || rows.length === 0) return null
    return parseTownRow(rows[0])
  }

  function listActorsByTown(townId) {
    ensureInitialized()
    const safeTownId = asText(townId, 80)
    if (!safeTownId) return []
    const rows = runSql(`
      SELECT actor_id, town_id, name, role, status, metadata_json
      FROM world_actors
      WHERE town_id = ${sqlValue(safeTownId)}
      ORDER BY role ASC, actor_id ASC;
    `, { json: true })
    return rows.map((row) => parseActorRow(row)).filter(Boolean)
  }

  function getActor(actorId) {
    ensureInitialized()
    const safeActorId = asText(actorId, 120)
    if (!safeActorId) return null
    const rows = runSql(`
      SELECT actor_id, town_id, name, role, status, metadata_json
      FROM world_actors
      WHERE actor_id = ${sqlValue(safeActorId)}
      LIMIT 1;
    `, { json: true })
    if (!Array.isArray(rows) || rows.length === 0) return null
    return parseActorRow(rows[0])
  }

  function listOfficeholders(townId) {
    ensureInitialized()
    const safeTownId = asText(townId, 80)
    if (!safeTownId) return []
    const rows = runSql(`
      SELECT actor_id, town_id, name, role, status, metadata_json
      FROM world_actors
      WHERE town_id = ${sqlValue(safeTownId)}
        AND status = 'active'
        AND role IN (${OFFICEHOLDER_ROLES.map((role) => sqlValue(role)).join(', ')})
      ORDER BY CASE role
        WHEN 'mayor' THEN 0
        WHEN 'captain' THEN 1
        WHEN 'warden' THEN 2
        ELSE 99
      END ASC, actor_id ASC;
    `, { json: true })
    return rows.map((row) => parseActorRow(row)).filter(Boolean)
  }

  function replaceWorldSnapshot(world, options = {}) {
    ensureInitialized()
    const ts = Number.isInteger(options?.timestamp) ? options.timestamp : Math.trunc(safeNow())
    const statements = buildReplaceWorldStatements(world, { timestamp: ts })
    if (options?.includeMigrationMeta === true) {
      statements.push(`INSERT OR REPLACE INTO world_meta (
        meta_key,
        meta_value,
        updated_at
      ) VALUES (
        ${sqlValue(WORLD_STATE_MIGRATION_META_KEY)},
        ${sqlValue('complete')},
        ${sqlValue(ts)}
      );`)
    }
    runSql([
      'BEGIN IMMEDIATE;',
      ...statements,
      'COMMIT;'
    ].join('\n'))
    return loadWorldSnapshot()
  }

  function upsertTown(townRecord) {
    ensureInitialized()
    const projected = projectTownRecord(townRecord?.townId, townRecord)
    if (!projected) return null
    const world = loadWorldSnapshot() || {}
    const nextWorld = cloneValue(world)
    nextWorld.towns = isPlainObject(nextWorld.towns) ? nextWorld.towns : {}
    const existing = isPlainObject(nextWorld.towns[projected.townId]) ? nextWorld.towns[projected.townId] : {}
    nextWorld.towns[projected.townId] = {
      ...existing,
      ...projected
    }
    replaceWorldSnapshot(nextWorld)
    return getTown(projected.townId)
  }

  function upsertActor(actorRecord) {
    ensureInitialized()
    const projected = projectActorRecord(actorRecord?.actorId, actorRecord)
    if (!projected) return null
    const world = loadWorldSnapshot() || {}
    const nextWorld = cloneValue(world)
    nextWorld.actors = isPlainObject(nextWorld.actors) ? nextWorld.actors : {}
    const existing = isPlainObject(nextWorld.actors[projected.actorId]) ? nextWorld.actors[projected.actorId] : {}
    nextWorld.actors[projected.actorId] = {
      ...existing,
      ...projected
    }
    replaceWorldSnapshot(nextWorld)
    return getActor(projected.actorId)
  }

  function applyWorldPatch(patch = {}) {
    ensureInitialized()
    const world = loadWorldSnapshot() || {}
    const nextWorld = cloneValue(world)
    if (isPlainObject(patch)) {
      for (const [key, value] of Object.entries(patch)) {
        nextWorld[key] = cloneValue(value)
      }
    }
    replaceWorldSnapshot(nextWorld)
    return loadWorldSnapshot()
  }

  return {
    backendName: 'sqlite',
    dbPath: resolvedDbPath,
    sqliteCommand: resolvedSqliteCommand,
    initialize: ensureInitialized,
    hasWorldSnapshot,
    loadWorldSnapshot,
    readWorldSnapshot: loadWorldSnapshot,
    replaceWorldSnapshot,
    listTowns,
    getTown,
    listActorsByTown,
    getActor,
    listOfficeholders,
    upsertTown,
    upsertActor,
    applyWorldPatch,
    getMeta,
    setMeta,
    buildReplaceWorldStatements
  }
}

module.exports = {
  WORLD_STATE_MIGRATION_META_KEY,
  WORLD_STATE_SCHEMA_VERSION,
  createMemoryWorldStateStore,
  createSqliteWorldStateStore
}
