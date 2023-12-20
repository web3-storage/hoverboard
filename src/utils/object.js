/**
 * Object.hasOwnProperty as typescript type guard
 * @template {unknown} X
 * @template {PropertyKey} Y
 * @param {X} obj
 * @param {Y} prop
 * @returns {obj is X & Record<Y, unknown>}
 * https://fettblog.eu/typescript-hasownproperty/
 */
export function hasOwnProperty (obj, prop) {
  return Object.prototype.hasOwnProperty.call(obj, prop)
}
