/**
 * @template {Map<string, Uint8Array>} M
 * @param {M} map
 * @returns {import('./api.js').KVBucketWithRangeQueries}
 */
export function createKvBucketFromMap (map) {
  /** @type {import('./api.js').KVBucketWithRangeQueries} */
  return {
    // @ts-expect-error here for debugging
    map,
    mapValues: [...map.values()].map(v => JSON.stringify(v)),
    /**
     *
     * @param {string} key
     * @param {object} [options]
     * @param {object} [options.range]
     * @param {number} [options.range.offset]
     * @returns
     */
    async get (key, { range = {} } = {}) {
      const keyBytes = await Promise.resolve(map.get(key))
      if (!keyBytes) return new Response(null)
      const rangeOffset = 'offset' in range ? range.offset ?? 0 : 0
      const view = new DataView(keyBytes.buffer, keyBytes.byteOffset + rangeOffset, keyBytes.byteLength - rangeOffset)
      return new Response(view)
    },
    async set (key, value) {
      await Promise.resolve(map.set(key, value))
    }
  }
}
