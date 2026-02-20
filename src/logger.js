/**
 * @typedef {Object} ErrorShape
 * @property {string} message
 * @property {string} name
 * @property {string | null} stack
 */

const LEVEL_PRIORITY = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40
}

/**
 * Convert unknown throwables into a serializable error payload.
 * @param {unknown} err
 * @returns {ErrorShape | null}
 */
function toErrorObject(err) {
  if (!err) return null
  if (err instanceof Error) {
    return {
      message: err.message,
      name: err.name,
      stack: err.stack || null
    }
  }
  return {
    message: String(err),
    name: 'Error',
    stack: null
  }
}

/**
 * @typedef {Object} LoggerOptions
 * @property {string} component
 * @property {'debug'|'info'|'warn'|'error'} [minLevel]
 * @property {{log: (line: string) => void, error: (line: string) => void}} [sink]
 * @property {Record<string, unknown>} [baseContext]
 */

/**
 * Create a structured JSON logger.
 * Invariant: logs are one-line JSON entries with stable keys for easy parsing.
 * @param {LoggerOptions} options
 */
function createLogger(options) {
  const component = options?.component || 'app'
  const minLevel = options?.minLevel || 'debug'
  const sink = options?.sink || {
    log: (line) => console.log(line),
    error: (line) => console.error(line)
  }
  const baseContext = options?.baseContext || {}

  function shouldEmit(level) {
    return LEVEL_PRIORITY[level] >= LEVEL_PRIORITY[minLevel]
  }

  function emit(level, message, context = {}) {
    if (!shouldEmit(level)) return
    const payload = {
      ts: new Date().toISOString(),
      level,
      component,
      message,
      ...baseContext,
      ...context
    }
    const line = JSON.stringify(payload)
    if (level === 'warn' || level === 'error') sink.error(line)
    else sink.log(line)
  }

  return {
    debug(message, context) {
      emit('debug', message, context)
    },
    info(message, context) {
      emit('info', message, context)
    },
    warn(message, context) {
      emit('warn', message, context)
    },
    error(message, context) {
      emit('error', message, context)
    },
    errorWithStack(message, err, context = {}) {
      emit('error', message, { ...context, error: toErrorObject(err) })
    },
    /**
     * @param {Record<string, unknown>} context
     */
    child(context) {
      return createLogger({
        component,
        minLevel,
        sink,
        baseContext: { ...baseContext, ...context }
      })
    }
  }
}

module.exports = { createLogger, toErrorObject }
