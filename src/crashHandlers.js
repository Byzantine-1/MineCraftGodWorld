const { createLogger, toErrorObject } = require('./logger')
const { isRecoverableError } = require('./errors')

const HANDLER_KEY = Symbol.for('minecraft-god-mvp.crashHandlersInstalled')

/**
 * @param {{
 *   component: string,
 *   logger?: ReturnType<typeof createLogger>,
 *   onFatal?: (reason: Error) => Promise<void> | void,
 *   exitOnFatal?: boolean,
 *   shutdownTimeoutMs?: number
 * }} options
 */
function installCrashHandlers(options) {
  if (process[HANDLER_KEY]) return
  process[HANDLER_KEY] = true

  const component = options?.component || 'app'
  const logger = options?.logger || createLogger({ component })
  const onFatal = options?.onFatal || (() => {})
  const exitOnFatal = options?.exitOnFatal !== false
  const shutdownTimeoutMs = Number(options?.shutdownTimeoutMs || 1500)
  let fatalTriggered = false

  async function handleFatal(kind, reason) {
    if (fatalTriggered) return
    fatalTriggered = true

    const err = reason instanceof Error ? reason : new Error(String(reason))
    logger.error(kind, { fatal: true, error: toErrorObject(err) })

    try {
      await Promise.race([
        Promise.resolve(onFatal(err)),
        new Promise(resolve => setTimeout(resolve, shutdownTimeoutMs))
      ])
    } catch (shutdownErr) {
      logger.error('fatal_shutdown_error', { fatal: true, error: toErrorObject(shutdownErr) })
    }

    if (exitOnFatal) process.exit(1)
  }

  process.on('uncaughtException', (err) => {
    void handleFatal('uncaught_exception', err)
  })

  process.on('unhandledRejection', (reason) => {
    if (isRecoverableError(reason)) {
      logger.warn('recoverable_unhandled_rejection', { error: toErrorObject(reason) })
      return
    }
    void handleFatal('unhandled_rejection', reason)
  })
}

module.exports = { installCrashHandlers }
