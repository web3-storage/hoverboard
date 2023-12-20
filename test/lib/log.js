/**
 * @typedef {"none" | "info" | "error" | "log" | "warn" | "debug"} LogLevel
 */

/**
 * @param {unknown} input
 * @returns {LogLevel | undefined}
 */
export const createLogLevel = (input) => {
  const levels = /** @type {const} */ (['none', 'info', 'error', 'log', 'warn', 'debug'])
  // @ts-expect-error because input is string not LogLevel
  if (levels.includes(input)) {
    return /** @type {LogLevel} */ (input)
  }
}
