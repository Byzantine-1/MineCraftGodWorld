const fs = require('fs')
const path = require('path')
const { createLogger } = require('./logger')
const { AppError } = require('./errors')

/**
 * @typedef {{
 *   agents: Record<string, {
 *     short: string[],
 *     long: string[],
 *     summary: string,
 *     archive: Array<{time: number, event: string}>,
 *     recentUtterances: string[],
 *     lastProcessedTime: number
 *   }>,
 *   factions: Record<string, {
 *     long: string[],
 *     summary: string,
 *     archive: Array<{time: number, event: string}>
 *   }>,
 *   world: {
 *     warActive: boolean,
 *     rules: { allowLethalPolitics: boolean },
 *     player: { name: string, alive: boolean, legitimacy: number },
 *     factions: Record<string, { hostilityToPlayer: number, stability: number }>,
 *     archive: Array<{time: number, event: string, important?: boolean}>,
 *     processedEventIds: string[]
 *   }
 * }} MemoryState
 */

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
 * @param {number} n
 * @param {number} min
 * @param {number} max
 */
function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n))
}

/**
 * @param {Partial<MemoryState> | null | undefined} input
 * @returns {MemoryState}
 */
function freshMemoryShape(input) {
  const source = input || {}
  return {
    agents: source.agents || {},
    factions: source.factions || {},
    world: {
      warActive: !!source.world?.warActive,
      rules: {
        allowLethalPolitics: source.world?.rules?.allowLethalPolitics !== false
      },
      player: {
        name: asText(source.world?.player?.name, 'Player', 60),
        alive: source.world?.player?.alive !== false,
        legitimacy: clamp(Number(source.world?.player?.legitimacy ?? 50), 0, 100)
      },
      factions: source.world?.factions || {},
      archive: Array.isArray(source.world?.archive) ? source.world.archive : [],
      processedEventIds: Array.isArray(source.world?.processedEventIds) ? source.world.processedEventIds : []
    }
  }
}

/**
 * @param {MemoryState} memory
 * @param {string} agent
 */
function initAgent(memory, agent) {
  if (!memory.agents[agent]) {
    memory.agents[agent] = {
      short: [],
      long: [],
      summary: '',
      archive: [],
      recentUtterances: [],
      lastProcessedTime: 0
    }
    return
  }
  memory.agents[agent].recentUtterances = memory.agents[agent].recentUtterances || []
  memory.agents[agent].lastProcessedTime = memory.agents[agent].lastProcessedTime || 0
}

/**
 * @param {MemoryState} memory
 * @param {string} faction
 */
function initFaction(memory, faction) {
  if (!memory.factions[faction]) {
    memory.factions[faction] = {
      long: [],
      summary: '',
      archive: []
    }
  }
  memory.world.factions[faction] = memory.world.factions[faction] || {
    hostilityToPlayer: 10,
    stability: 70
  }
}

/**
 * @param {string[]} ids
 * @param {string} eventId
 */
function hasEvent(ids, eventId) {
  return ids.includes(eventId)
}

/**
 * @param {MemoryState} memory
 * @param {string} eventId
 */
function markEvent(memory, eventId) {
  memory.world.processedEventIds.push(eventId)
  if (memory.world.processedEventIds.length > 1000) {
    memory.world.processedEventIds = memory.world.processedEventIds.slice(-1000)
  }
}

/**
 * @param {string[]} entries
 */
function summarize(entries) {
  return `History shaped by: ${entries.slice(-10).join(' ')}`.slice(0, 500)
}

/**
 * @param {MemoryState} memory
 * @returns {MemoryState}
 */
function cloneMemory(memory) {
  if (typeof structuredClone === 'function') return structuredClone(memory)
  return JSON.parse(JSON.stringify(memory))
}

/**
 * @param {{
 *   filePath?: string,
 *   fsModule?: typeof fs,
 *   logger?: ReturnType<typeof createLogger>,
 *   now?: () => number
 * }} options
 */
function createMemoryStore(options = {}) {
  const filePath = options.filePath || path.resolve(__dirname, './memory.json')
  const fsModule = options.fsModule || fs
  const fsPromises = (fsModule.promises && typeof fsModule.promises.open === 'function')
    ? fsModule.promises
    : fs.promises
  const logger = options.logger || createLogger({ component: 'memory' })
  const now = options.now || (() => Date.now())
  // Cross-process lock file used to serialize writers touching memory.json.
  const lockPath = `${filePath}.lock`
  const maxLockRetries = 5

  /** @type {MemoryState | null} */
  let state = null
  let txQueue = Promise.resolve()

  function loadFromDisk() {
    if (!fsModule.existsSync(filePath)) {
      state = freshMemoryShape(null)
      return state
    }

    try {
      const data = JSON.parse(fsModule.readFileSync(filePath, 'utf-8'))
      state = freshMemoryShape(data)
      return state
    } catch (err) {
      logger.warn('memory_load_failed_resetting', { filePath, error: err instanceof Error ? err.message : String(err) })
      state = freshMemoryShape(null)
      return state
    }
  }

  function ensureLoaded() {
    if (state) return state
    return loadFromDisk()
  }

  /**
   * @param {number} ms
   */
  function wait(ms) {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  async function loadFromDiskUnderLock() {
    try {
      const payload = await fsPromises.readFile(filePath, 'utf-8')
      return freshMemoryShape(JSON.parse(payload))
    } catch (err) {
      if (err && typeof err === 'object' && err.code === 'ENOENT') {
        return freshMemoryShape(null)
      }
      logger.warn('memory_load_failed_resetting', { filePath, error: err instanceof Error ? err.message : String(err) })
      return freshMemoryShape(null)
    }
  }

  async function acquireLockWithRetry() {
    // Small bounded backoff avoids hot-spinning when another process holds the lock.
    for (let attempt = 0; attempt <= maxLockRetries; attempt += 1) {
      try {
        return await fsPromises.open(lockPath, 'wx')
      } catch (err) {
        const isEexist = err && typeof err === 'object' && err.code === 'EEXIST'
        if (!isEexist) {
          throw new AppError({
            code: 'MEMORY_LOCK_FAILED',
            message: 'Failed to acquire memory lock.',
            recoverable: false,
            metadata: { lockPath, error: err instanceof Error ? err.message : String(err) }
          })
        }
        if (attempt === maxLockRetries) {
          throw new AppError({
            code: 'MEMORY_LOCK_TIMEOUT',
            message: 'Timed out acquiring memory lock.',
            recoverable: false,
            metadata: { lockPath, retries: maxLockRetries }
          })
        }
        await wait(15 * (attempt + 1))
      }
    }
    throw new AppError({
      code: 'MEMORY_LOCK_TIMEOUT',
      message: 'Timed out acquiring memory lock.',
      recoverable: false,
      metadata: { lockPath, retries: maxLockRetries }
    })
  }

  async function withFileLock(fn) {
    const lockHandle = await acquireLockWithRetry()
    try {
      return await fn()
    } finally {
      try {
        await lockHandle.close()
      } finally {
        await fsPromises.unlink(lockPath).catch(() => {})
      }
    }
  }

  async function persistSnapshotAtomically(snapshot) {
    const payload = JSON.stringify(snapshot, null, 2)
    const tempPath = `${filePath}.${process.pid}.${Date.now()}.tmp`
    try {
      await fsPromises.writeFile(tempPath, payload, 'utf-8')
      // Rename on the same filesystem is atomic, so readers never observe partial JSON.
      await fsPromises.rename(tempPath, filePath)
    } catch (err) {
      await fsPromises.unlink(tempPath).catch(() => {})
      throw new AppError({
        code: 'MEMORY_WRITE_FAILED',
        message: 'Failed to persist memory state.',
        recoverable: false,
        metadata: { filePath, error: err instanceof Error ? err.message : String(err) }
      })
    }
  }

  /**
   * Serialize mutating transactions and commit only after successful persist.
   * @template T
   * @param {(memory: MemoryState) => T | Promise<T>} mutator
   * @param {{eventId?: string, persist?: boolean}} [opts]
   * @returns {Promise<{skipped: boolean, result: T | null}>}
   */
  function transact(mutator, opts = {}) {
    const run = async () => {
      const eventId = opts.eventId ? asText(opts.eventId, '', 200) : ''
      return withFileLock(async () => {
        // Always reload inside the lock so each writer mutates the latest committed snapshot.
        const current = await loadFromDiskUnderLock()
        state = current

        if (eventId && hasEvent(current.world.processedEventIds, eventId)) {
          return { skipped: true, result: null }
        }

        const working = cloneMemory(current)
        const result = await mutator(working)

        if (eventId) markEvent(working, eventId)
        if (opts.persist !== false) await persistSnapshotAtomically(working)
        state = working

        return { skipped: false, result }
      })
    }

    const chained = txQueue.then(run, run)
    txQueue = chained.then(() => undefined, () => undefined)
    return chained
  }

  /**
   * @returns {MemoryState}
   */
  function getSnapshot() {
    return cloneMemory(ensureLoaded())
  }

  /**
   * @param {{reload?: boolean}} [opts]
   * @returns {MemoryState}
   */
  function loadAllMemory(opts = {}) {
    if (opts.reload) loadFromDisk()
    else ensureLoaded()
    return getSnapshot()
  }

  /**
   * @param {string} eventId
   * @returns {boolean}
   */
  function hasProcessedEvent(eventId) {
    const id = asText(eventId, '', 200)
    if (!id) return false
    return hasEvent(ensureLoaded().world.processedEventIds, id)
  }

  /**
   * @param {string} agent
   * @param {string} entry
   * @param {boolean} [important]
   * @param {string | undefined} [eventId]
   */
  async function rememberAgent(agent, entry, important = false, eventId) {
    const safeAgent = asText(agent, '', 80)
    const safeEntry = asText(entry, '', 500)
    if (!safeAgent || !safeEntry) {
      throw new AppError({
        code: 'INVALID_MEMORY_INPUT',
        message: 'Agent memory write rejected due to invalid input.',
        recoverable: true
      })
    }

    await transact((memory) => {
      initAgent(memory, safeAgent)
      memory.agents[safeAgent].short.push(safeEntry)
      if (memory.agents[safeAgent].short.length > 20) memory.agents[safeAgent].short.shift()

      if (important) {
        memory.agents[safeAgent].long.push(safeEntry)
        if (memory.agents[safeAgent].long.length % 20 === 0) {
          memory.agents[safeAgent].summary = summarize(memory.agents[safeAgent].long)
        }
      }

      memory.agents[safeAgent].archive.push({ time: now(), event: safeEntry })
    }, { eventId: eventId ? `${eventId}:agent:${safeAgent}` : undefined })
  }

  /**
   * @param {string} faction
   * @param {string} entry
   * @param {boolean} [important]
   * @param {string | undefined} [eventId]
   */
  async function rememberFaction(faction, entry, important = false, eventId) {
    const safeFaction = asText(faction, '', 80)
    const safeEntry = asText(entry, '', 500)
    if (!safeFaction || !safeEntry) {
      throw new AppError({
        code: 'INVALID_MEMORY_INPUT',
        message: 'Faction memory write rejected due to invalid input.',
        recoverable: true
      })
    }

    await transact((memory) => {
      initFaction(memory, safeFaction)
      memory.factions[safeFaction].long.push(safeEntry)
      if (important || memory.factions[safeFaction].long.length % 20 === 0) {
        memory.factions[safeFaction].summary = summarize(memory.factions[safeFaction].long)
      }
      memory.factions[safeFaction].archive.push({ time: now(), event: safeEntry })
    }, { eventId: eventId ? `${eventId}:faction:${safeFaction}` : undefined })
  }

  /**
   * @param {string} entry
   * @param {boolean} [important]
   * @param {string | undefined} [eventId]
   */
  async function rememberWorld(entry, important = false, eventId) {
    const safeEntry = asText(entry, '', 500)
    if (!safeEntry) {
      throw new AppError({
        code: 'INVALID_MEMORY_INPUT',
        message: 'World memory write rejected due to invalid input.',
        recoverable: true
      })
    }

    await transact((memory) => {
      memory.world.archive.push({ time: now(), event: safeEntry, important: !!important })
      if (memory.world.archive.length > 500) memory.world.archive = memory.world.archive.slice(-500)
    }, { eventId: eventId ? `${eventId}:world` : undefined })
  }

  /**
   * @param {string} agent
   */
  function recallAgent(agent) {
    const safeAgent = asText(agent, '', 80)
    if (!safeAgent) return null
    return getSnapshot().agents[safeAgent] || null
  }

  /**
   * @param {string} faction
   */
  function recallFaction(faction) {
    const safeFaction = asText(faction, '', 80)
    if (!safeFaction) return null
    return getSnapshot().factions[safeFaction] || null
  }

  function recallWorld() {
    return getSnapshot().world
  }

  async function saveAllMemory() {
    await transact(() => {}, { persist: true, eventId: undefined })
  }

  return {
    loadAllMemory,
    saveAllMemory,
    getSnapshot,
    transact,
    hasProcessedEvent,
    rememberAgent,
    rememberFaction,
    rememberWorld,
    recallAgent,
    recallFaction,
    recallWorld
  }
}

module.exports = { createMemoryStore, freshMemoryShape }
