/**
 * Standardized application error for recoverability classification.
 */
class AppError extends Error {
  /**
   * @param {{code: string, message: string, recoverable?: boolean, metadata?: Record<string, unknown>}} input
   */
  constructor(input) {
    super(input.message)
    this.name = 'AppError'
    this.code = input.code
    this.recoverable = input.recoverable !== false
    this.metadata = input.metadata || {}
  }
}

/**
 * @param {unknown} err
 * @returns {boolean}
 */
function isRecoverableError(err) {
  return err instanceof AppError ? err.recoverable : false
}

module.exports = { AppError, isRecoverableError }
