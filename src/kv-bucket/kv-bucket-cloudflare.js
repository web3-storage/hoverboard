/**
 * @param {R2Bucket} r2
 * @returns {import('./api.js').KVBucketWithRangeQueries}
 */
export function createBucketFromR2 (r2) {
  /** @type {import('./api.js').KVBucketWithRangeQueries} */
  const bucket = {
    async get (key, options = {}) {
      const range = options?.range || undefined
      const bytes = await r2.get(key, range ? { range } : undefined).then(r => r?.arrayBuffer())
      return new Response(bytes)
    },
    async set (key, value) {
      await r2.put(key, value)
    }
  }
  return bucket
}

/**
 * @param {Pick<import('miniflare').ReplaceWorkersTypes<import('@cloudflare/workers-types/experimental').R2Bucket>, 'get'|'put'>} r2
 * @returns {import('./api.js').KVBucketWithRangeQueries}
 */
export function createBucketFromR2Miniflare (r2) {
  /** @type {import('./api.js').KVBucketWithRangeQueries} */
  const bucket = {
    async get (key, options = {}) {
      const range = options?.range || undefined
      const bytes = await r2.get(key, range ? { range } : undefined).then(r => r?.arrayBuffer())
      return new Response(bytes)
    },
    async set (key, value) {
      await r2.put(key, value)
    }
  }
  return bucket
}
