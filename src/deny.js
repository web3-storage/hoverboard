import { sha256 } from 'multiformats/hashes/sha2'
import { toString, fromString } from 'uint8arrays'

/**
 * @typedef {import('multiformats').UnknownLink} UnknownLink
 * @typedef {import('./blocks.js').Blockstore} Blockstore
 */

export class DenyingBlockStore {
  /**
   * @param {KVNamespace} denylist
   * @param {Blockstore} blockstore
   * @param {object} [options]
   * @param {number} [options.cacheTtl=3600] time in seconds that a KV result is cached in the current data-center.
   */
  constructor (denylist, blockstore, options) {
    this.denylist = denylist
    this.blockstore = blockstore
    this.cacheTtl = options?.cacheTtl ?? 3600
  }

  /**
   * @param {UnknownLink} cid
   */
  async has (cid) {
    if (await this.ok(cid)) {
      return this.blockstore.has(cid)
    }
    return false
  }

  /**
   * @param {UnknownLink} cid
   */
  async get (cid) {
    if (await this.ok(cid)) {
      return this.blockstore.get(cid)
    }
  }

  /**
   * @param {UnknownLink} cid
   */
  async ok (cid) {
    const key = await this.toDenyListAnchor(cid)
    const res = await this.denylist.get(key, { cacheTtl: this.cacheTtl })
    return res === null
  }

  /**
   * @param {UnknownLink} cid
   */
  async toDenyListAnchor (cid) {
    const hash = await sha256.encode(fromString(`${cid}/`))
    return toString(hash, 'hex')
  }
}
