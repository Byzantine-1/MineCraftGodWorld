const crypto = require('crypto')

/**
 * Serialize async work per key to prevent interleaving state mutations.
 * @returns {(key: string, fn: () => Promise<any>) => Promise<any>}
 */
function createKeyedQueue() {
  const lanes = new Map()

  return async function runSerial(key, fn) {
    const safeKey = String(key || 'default')
    const prev = lanes.get(safeKey) || Promise.resolve()

    let release
    const current = new Promise((resolve) => { release = resolve })
    lanes.set(safeKey, current)

    await prev.catch(() => {})
    try {
      return await fn()
    } finally {
      release()
      if (lanes.get(safeKey) === current) lanes.delete(safeKey)
    }
  }
}

/**
 * Bounded async concurrency helper.
 * @param {number} limit
 * @returns {(fn: () => Promise<any>) => Promise<any>}
 */
function createSemaphore(limit) {
  const max = Number.isInteger(limit) && limit > 0 ? limit : 1
  let active = 0
  const waiters = []

  async function acquire() {
    if (active < max) {
      active += 1
      return
    }
    await new Promise(resolve => waiters.push(resolve))
    active += 1
  }

  function release() {
    active -= 1
    const next = waiters.shift()
    if (next) next()
  }

  return async function withSlot(fn) {
    await acquire()
    try {
      return await fn()
    } finally {
      release()
    }
  }
}

/**
 * Build deterministic operation id from request parts and a time bucket.
 * @param {Array<string | number | boolean | null | undefined>} parts
 * @param {{windowMs?: number, now?: () => number}} [opts]
 */
function deriveOperationId(parts, opts = {}) {
  const now = opts.now || (() => Date.now())
  const windowMs = Number(opts.windowMs || 5000)
  const bucket = Math.floor(now() / windowMs)
  const payload = JSON.stringify([bucket, ...(Array.isArray(parts) ? parts : [parts])])
  return crypto.createHash('sha256').update(payload).digest('hex').slice(0, 40)
}

/**
 * @param {string} value
 * @returns {string}
 */
function hashText(value) {
  return crypto.createHash('sha256').update(String(value || '')).digest('hex').slice(0, 16)
}

/**
 * @param {Promise<any>} promise
 * @param {number} timeoutMs
 * @param {string} label
 */
async function withTimeout(promise, timeoutMs, label = 'operation_timeout') {
  let timeout
  const timer = new Promise((_, reject) => {
    timeout = setTimeout(() => reject(new Error(label)), timeoutMs)
  })
  try {
    return await Promise.race([promise, timer])
  } finally {
    clearTimeout(timeout)
  }
}

module.exports = {
  createKeyedQueue,
  createSemaphore,
  deriveOperationId,
  hashText,
  withTimeout
}
