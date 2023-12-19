import { CARReaderStream } from 'carstream/reader'
import * as CAR from './car.js'
import { MultihashIndexSortedReader } from 'cardex/multihash-index-sorted'
import { CID } from 'multiformats'
import * as Link from 'multiformats/link'
import * as raw from 'multiformats/codecs/raw'
import { asyncIterableReader, readBlockHead } from '@ipld/car/decoder'
import errCode from 'err-code'

/* global ReadableStream */
/* global WritableStream */
/* global TransformStream */

/**
 * @typedef {import('multiformats').UnknownLink} UnknownLink
 */

/**
 * @typedef {import('./blocks.js').Blockstore} Blockstore
 */

/**
 * a store of ipld blocks that be fetched by CID
 */
export class AbstractBlockStore {
  constructor () {
    void /** @type {Blockstore} */ (this) // eslint-disable-line no-void
  }

  /**
   * @param {UnknownLink} link
   * @returns {Promise<Uint8Array|undefined>}
   */
  async get (link) {
    throw new Error('not implemented')
  }

  /**
   * methods from import('interface-blockstore').Blockstore and e.g. its MemoryBlockstore.
   * but then I realized they are not on `Blockstore`
   */

  async delete () { throw new Error('not implemented') }
  async * deleteMany () { throw new Error('not implemented') }
  async * getAll () { throw new Error('not implemented') }
  async * getMany () { throw new Error('not implemented') }
  /** @returns {Promise<CID>} */
  async put () { throw new Error('not implemented') }
  async * putMany () { throw new Error('not implemented') }

  /**
   * @param {UnknownLink} link
   * @returns {Promise<boolean>}
   */
  async has (link) {
    throw new Error('not implemented')
  }
}

/**
 * interface of `import * as ContentClaims from "@web3-storage/content-claims"`
 */
export class AbstractClaimsClient {
  /**
   * @param {UnknownLink} link
   * @param {import('@web3-storage/content-claims/client').FetchOptions} options
   * @returns {Promise<import('@web3-storage/content-claims/client/api').Claim[]>}
   */
  async read (link, options = {}) {
    throw new Error('not implemented')
  }
}

/**
 * @implements {AbstractBlockStore}
 */
export class ContentClaimsBlockstore extends AbstractBlockStore {
  /** @type {AbstractClaimsClient['read']} */
  #read
  /** @type {URL|undefined} */
  #url
  /** @type {import('./api.js').KVBucketWithRangeQueries}  */
  #carpark

  /**
   * @param {object} options
   * @param {AbstractClaimsClient['read']} options.read
   * @param {import('./api.js').KVBucketWithRangeQueries} options.carpark - keys like `${cid}/${cid}.car` and values are car bytes
   * @param {URL} [options.url]
   */
  constructor ({ read, url, carpark }) {
    super()
    this.#read = read
    this.#url = url
    this.#carpark = carpark
    void /** @type {Blockstore} */ (this) // eslint-disable-line no-void
  }

  /**
   * @param {UnknownLink} link
   */
  async has (link) {
    const result = await claimsHas(this.#read, link, this.#url)
    return result
  }

  /**
   * @param {UnknownLink} link
   */
  async get (link) {
    const claims = await this.#read(link, { serviceURL: this.#url })
    // eslint-disable-next-line no-unreachable-loop
    for await (const block of claimsGetBlock(claims, link, this.#carpark)) {
      return block
    }
    throw errCode(new Error('not found'), 'ERR_NOT_FOUND')
  }
}

/**
 * @param {AbstractClaimsClient['read']} read
 * @param {UnknownLink} link - link to answer whether the content-claims at `url` has blocks for `link`
 * @param {URL} [serviceURL] - serviceURL to claims service to read from
 */
async function claimsHas (
  read,
  link,
  serviceURL
) {
  const claims = await read(link, { serviceURL })
  // @todo consider checking the claims themselves
  return Boolean(claims.length)
}

/**
 * @param {Map<string, Promise<IndexEntry|undefined>>} map - keys are CID strings
 * @returns {import('./api.js').IndexMap & Index}
 */
function createIndexEntryMap (map = new Map()) {
  return {
    get: async (key) => {
      return map.get(key.toString())
    },
    has: async (key) => {
      const hasKey = map.has(key.toString())
      return hasKey
    },
    set: async (key, value) => {
      map.set(key.toString(), Promise.resolve(value))
    }
  }
}

/**
 * get index for a link from a content claims client
 * @param {AsyncIterable<import('./api.js').LocationClaim>|Iterable<import('./api.js').LocationClaim>} claims
 * @param {UnknownLink} link - link to answer whether the content-claims at `url` has blocks for `link`
 * @returns {AsyncGenerator<Uint8Array>}
 */
async function * fetchBlocksForLocationClaims (claims, link) {
  /**
   * @type {ReadableStream<{
   *   claim: import('@web3-storage/content-claims/client/api').LocationClaim
   *   location: string
   *   response: Response
   *   block: import('carstream/api').Block
   * }>}
   */
  const locatedBlocks = createReadableStream(claims)
    // claim -> [claim, location]
    .pipeThrough(new TransformStream({
    /**
      * @param {import('@web3-storage/content-claims/client/api').LocationClaim} claim
      */
      async transform (claim, controller) {
        for (const location of claim.location) { controller.enqueue([claim, location]) }
      }
    }))
    // [claim, location] -> [claim, location, ok response]
    .pipeThrough(new TransformStream({
    /** @param {[claim: import('@web3-storage/content-claims/client/api').Claim, location: string]} chunk */
      async transform ([claim, location], controller) {
        const response = await fetch(location, { headers: { accept: 'application/vnd.ipld.car' } })
        if (response.ok) {
          return controller.enqueue([claim, location, response])
        }
        throw new Error(`unexpected not-ok response ${response}`)
      }
    }))
    // [claim, location, ok response] -> { claim, location, ok response, block }
    .pipeThrough(new TransformStream({
    /** @param {[claim: import('@web3-storage/content-claims/client/api').LocationClaim, location: string, response: Response]} chunk */
      async transform ([claim, location, response], controller) {
        await response.body?.pipeThrough(new CARReaderStream()).pipeTo(new WritableStream({
          write: (block) => {
          // @todo validate that block.bytes hashes to `link`
            controller.enqueue({ claim, location, response, block })
          }
        }))
      }
    }))
  for await (const locatedBlock of locatedBlocks) {
    yield locatedBlock.block.bytes
  }
}

/**
 * @param {import('./api.js').RelationClaim[]} claims
 * @param {UnknownLink} link - link to answer whether the content-claims at `url` has blocks for `link`
 * @param {import('./api.js').KVBucketWithRangeQueries} carpark - keys like `${cid}/${cid}.car` and values are car bytes
 * @param {Index & import('./api.js').IndexMap} index - map of
 * @returns {AsyncGenerator<Uint8Array>}
 */
async function * fetchBlocksForRelationClaims (claims, link, carpark, index) {
  for await (const claim of claims) {
    const blocks = [...claim.export()]

    const relationClaimPartBlocks = []

    // each part is a tuple of CAR CID (content) & CARv2 index CID (includes)
    for (const { content, includes } of claim.parts) {
      if (content.code !== CAR.code) continue
      if (!includes) continue

      if (includes.content.code !== MultihashIndexSortedReader.codec) {
        throw new Error('relationClaim.includes is link to car-multihash-index-sorted')
      }

      /** @type {{ cid: import('multiformats').UnknownLink, bytes: Uint8Array }|undefined} */
      let indexBlock = blocks.find(b => b.cid.equals(includes.content))

      // if the index is not included in the claim, it should be in CARPARK
      if (!indexBlock && includes.parts?.length) {
        const includeParts = []
        for (const part of includes.parts) {
          // part is a CARLink to a car-multihash-index-sorted
          const partCarParkPath = `${part}/${part}.car`
          const obj = await carpark.get(partCarParkPath).then(r => r.blob())
          if (!obj) continue
          const partBlocks = await CAR.decode(new Uint8Array(await obj.arrayBuffer()))
          includeParts.push(...partBlocks)
          indexBlock = indexBlock ?? partBlocks.find(b => b.cid.equals(includes.content))
          if (indexBlock) {
            // we found the indexBlock we need. No need to keep looping over parts
            break
          }
        }
      }

      // we have a block for the whole Index linked from the relationClaim
      // attempt to decode the index entries from the block and add them to the `index` map
      if (indexBlock) {
        if (indexBlock.cid.code !== MultihashIndexSortedReader.codec) { throw new Error(`indexBlock cid codec was ${indexBlock.cid.code} but expected MultihashIndexSortedReader ${MultihashIndexSortedReader.codec}`) }
        const includesContentCidCode = content.code
        const includesContentCidV1 = CID.create(1, includesContentCidCode, content.multihash)
        for await (const entry of decodeRelationIncludesIndex(includesContentCidV1, indexBlock.bytes)) {
          const entryIndexKey = Link.create(raw.code, entry.multihash)
          index.set(entryIndexKey, entry)
        }
        relationClaimPartBlocks.push(indexBlock)
      }

      // ok maybe now the index has the cid we were originally looking for?
      if (await index.has(link)) {
        /** @type {import('./api.js').KVBucketWithRangeQueries} */
        const r2Map = {
          async get (key, { range } = {}) {
            const bytes = await carpark.get(key, { range }).then(r => r.arrayBuffer())
            if (!bytes) return new Response(null)
            return new Response(bytes)
          },
          async set (key, value) {
            await carpark.set(key, value)
          }
        }
        const blockstore = new R2Blockstore(r2Map, index)
        const block = await blockstore.get(link)
        if (block) {
          yield block.bytes
        }
      }
    }
  }
}

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

/**
 * get index for a link from a content claims client
 * @param {AsyncIterable<import('./api.js').Claim>|Iterable<import('./api.js').Claim>} claims
 * @param {UnknownLink} link - link to answer whether the content-claims at `url` has blocks for `link`
 * @param {import('./api.js').KVBucketWithRangeQueries} carpark - keys like `${cid}/${cid}.car` and values are car bytes
 * @param {Index & import('./api.js').IndexMap} [index] - map of
 * @returns {AsyncGenerator<Uint8Array>}
 */
async function * claimsGetBlock (claims, link, carpark, index = createIndexEntryMap()) {
  for await (const claim of claims) {
    switch (claim.type) {
      case 'assert/location':
        yield * fetchBlocksForLocationClaims([claim], link)
        break
      case 'assert/relation':
        yield * fetchBlocksForRelationClaims([claim], link, carpark, index)
        break
      default:
        // donno about this claim type
        continue
    }
  }
}

/**
 * @template T
 * @param {Iterable<T>|AsyncIterable<T>} source
 */
function createReadableStream (source) {
  return new ReadableStream({
    async start (controller) {
      for await (const item of source) {
        controller.enqueue(item)
      }
      controller.close()
    }
  })
}

/**
 * @typedef {import('./api.js').IndexEntry} IndexEntry
 * @typedef {import('./api.js').Index} Index
 */

/**
 * Read a MultihashIndexSorted index for the passed origin CAR and return a
 * list of IndexEntry.
 * @param {import('cardex/api').CARLink} origin
 * @param {Uint8Array} bytes
 */
const decodeRelationIncludesIndex = async function * (origin, bytes) {
  const readable = new ReadableStream({
    pull (controller) {
      controller.enqueue(bytes)
      controller.close()
    }
  })
  const reader = MultihashIndexSortedReader.createReader({ reader: readable.getReader() })
  while (true) {
    const { done, value } = await reader.read()
    if (done) break
    yield (/** @type {IndexEntry} */({ origin, ...value }))
  }
}

/**
 * A blockstore that is backed by an R2 bucket which contains CARv2
 * MultihashIndexSorted indexes alongside CAR files. It can read DAGs split
 * across multiple CARs.
 */
export class R2Blockstore {
  /**
   * @param {import('./api.js').KVBucketWithRangeQueries} dataBucket
   * @param {import('./api.js').Index} index
   */
  constructor (dataBucket, index) {
    this._dataBucket = dataBucket
    this._idx = index
  }

  /** @param {UnknownLink} cid */
  async get (cid) {
    const entry = await this._idx.get(cid)
    if (!entry) return
    const carPath = `${entry.origin}/${entry.origin}.car`
    const range = { offset: entry.offset }
    const res = await this._dataBucket.get(carPath, { range })
    if (!res) return

    const reader = res.body?.getReader()
    const bytesReader = asyncIterableReader((async function * () {
      if (!reader) return
      while (true) {
        const { done, value } = await reader.read()
        if (done) return
        yield value
      }
    })())

    const blockHeader = await readBlockHead(bytesReader)
    const bytes = await bytesReader.exactly(blockHeader.blockLength)
    reader?.cancel()
    return { cid, bytes }
  }
}

/**
 * @template {Map<string, Uint8Array>} M
 * @param {M} map
 * @returns {import('../src/api.js').KVBucketWithRangeQueries}
 */
export function createKvBucketFromMap (map) {
  /** @type {import('../src/api.js').KVBucketWithRangeQueries} */
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