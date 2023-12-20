import { Map as LinkMap } from 'lnmap'
import * as Link from 'multiformats/link'
import { CARWriterStream } from 'carstream/writer'
import { Readable, Writable } from 'node:stream'
import { CARReaderStream } from 'carstream'
import * as raw from 'multiformats/codecs/raw'
import * as Block from 'multiformats/block'
import { sha256 } from 'multiformats/hashes/sha2'
import { identity } from 'multiformats/hashes/identity'
import { blake2b256 } from '@multiformats/blake2/blake2b'
import * as pb from '@ipld/dag-pb'
import * as cbor from '@ipld/dag-cbor'
import * as json from '@ipld/dag-json'
import { Assert } from '@web3-storage/content-claims/capability'
import { Signer as Ed25519Signer } from '@ucanto/principal/ed25519'
import * as http from 'node:http'
import { concat } from 'uint8arrays'
import { CarIndexer } from '@ipld/car'
import * as CAR from '../../src/car.js'
import { encodeVarint } from 'cardex/encoder'
import { MultiIndexWriter } from 'cardex/multi-index'
import { TreewalkCarSplitter } from 'carbites/treewalk' // chunk to ~10MB CARs
import { MultihashIndexSortedWriter } from 'cardex'
import { createKvBucketFromMap } from '../../src/kv-bucket/kv-bucket.js'

/* global WritableStream */
/* global ReadableStream */
/* global TransformStream */

const MULTIHASH_INDEX_SORTED_CODE = 0x0401

/**
 * @typedef {import('carstream/api').Block & { children: import('multiformats').UnknownLink[] }} RelationIndexData
 * @typedef {Map<import('multiformats').UnknownLink, import('carstream/api').Block[]>} Claims
 * @typedef {{ setClaims: (c: Claims) => void, close: () => void, port: number, signer: import('@ucanto/interface').Signer }} MockClaimsService
 * @typedef {import('@web3-storage/content-claims/server/api').ClaimStore} ClaimStore
 */

/**
 * @param {Claims} claims
 */
export const mockClaimsService = (
  claims = new LinkMap()
) => {
  /** @param {Claims} s */
  const setClaims = s => { claims = s }
  /**
   * @param {import('http').IncomingMessage} req
   * @param {import('http').OutgoingMessage} res
   */
  const listener = async (req, res) => {
    const content = Link.parse(String(req.url?.split('/')[2]))
    const blocks = [...claims.get(content) ?? []]
    await Readable.toWeb(Readable.from(blocks))
      // @ts-expect-error due to getReader.closed Promise<void> vs Promise<undefined>
      .pipeThrough(new CARWriterStream(blocks.map(b => b.cid)))
      .pipeTo(Writable.toWeb(res))
  }
  return { claims, setClaims, listener }
}

const Decoders = {
  [raw.code]: raw,
  [pb.code]: pb,
  [cbor.code]: cbor,
  [json.code]: json
}

const Hashers = {
  [identity.code]: identity,
  [sha256.code]: sha256,
  [blake2b256.code]: blake2b256
}

/**
 * @param {Claims} claims
 * @param {import('@ucanto/interface').Signer} signer
 * @param {import('multiformats').UnknownLink} dataCid
 * @param {import('cardex/api').CARLink} carCid
 * @param {ReadableStream<Uint8Array>} carStream CAR file data
 * @param {import('multiformats').Link} indexCid
 * @param {import('cardex/api').CARLink} indexCarCid
 */
export const generateClaims = async (claims = new LinkMap(), signer, dataCid, carCid, carStream, indexCid, indexCarCid) => {
  // partition claim for the data CID
  claims.set(dataCid, [
    await encodeInvocationBlock(Assert.partition.invoke({
      issuer: signer,
      audience: signer,
      with: signer.did(),
      nb: {
        content: dataCid,
        parts: [carCid]
      }
    }))
  ])

  /** @type {Map<import('multiformats').UnknownLink, RelationIndexData>} */
  const indexData = new LinkMap()

  await carStream
    .pipeThrough(new CARReaderStream())
    .pipeTo(new WritableStream({
      async write ({ cid, bytes }) {
        // @ts-expect-error cid.code may not be in Decoders
        const decoder = Decoders[cid.code]
        if (!decoder) throw Object.assign(new Error(`missing decoder: ${cid.code}`), { code: 'ERR_MISSING_DECODER' })

        // @ts-expect-error cid.multihash.code may not be in Hashers
        const hasher = Hashers[cid.multihash.code]
        if (!hasher) throw Object.assign(new Error(`missing hasher: ${cid.multihash.code}`), { code: 'ERR_MISSING_HASHER' })

        const block = await Block.decode({ bytes, codec: decoder, hasher })
        indexData.set(cid, { cid, bytes, children: [...block.links()].map(([, cid]) => cid) })
      }
    }))

  for (const [cid, { children }] of indexData) {
    const invocation = Assert.relation.invoke({
      issuer: signer,
      audience: signer,
      with: signer.did(),
      nb: {
        content: cid,
        children,
        parts: [{
          content: carCid,
          includes: {
            content: indexCid,
            parts: [indexCarCid]
          }
        }]
      }
    })

    const blocks = claims.get(cid) ?? []
    blocks.push(await encodeInvocationBlock(invocation))
    claims.set(cid, blocks)
  }

  // partition claim for the index
  claims.set(indexCid, [
    await encodeInvocationBlock(Assert.partition.invoke({
      issuer: signer,
      audience: signer,
      with: signer.did(),
      nb: {
        content: indexCid,
        parts: [indexCarCid]
      }
    }))
  ])

  return claims
}

/**
 * Encode a claim to a block.
 * @param {import('@ucanto/interface').IssuedInvocationView} invocation
 */
export async function encodeInvocationBlock (invocation) {
  const view = await invocation.buildIPLDView()
  const bytes = await view.archive()
  if (bytes.error) throw new Error('failed to archive')
  return { cid: Link.create(CAR.code, await sha256.digest(bytes.ok)), bytes: bytes.ok }
}

/**
 * @param {string} input - text input to use as the sample content that will be encoded into a car file
 * @param {import('../../src/kv-bucket/api.js').KVBucketWithRangeQueries} [carpark] - keys like `${cid}/${cid}.car` and values are car bytes
 * @param {object} options
 * @param {import('@ucanto/interface').Signer} [options.signer]
 */
export async function createSimpleContentClaimsScenario (
  input,
  carpark = createKvBucketFromMap(/** @type {Map<string,Uint8Array>} */ (new Map())),
  options = {}
) {
  const {
    signer = await Ed25519Signer.generate()
  } = options

  const inputBuffer = (new TextEncoder()).encode(input)
  const inputBlock = await Block.encode({ value: inputBuffer, codec: raw, hasher: sha256 })
  const inputCID = await Promise.resolve(inputBlock.cid)
  const createInputBlockStream = () => {
    /** @type {ReadableStream<import('carstream/api').Block>} */
    const blocks = new ReadableStream({
      start (controller) {
        /** @type {import('carstream/api').Block} */
        const block = {
          cid: inputBlock.cid,
          bytes: inputBlock.bytes
        }
        controller.enqueue(block)
        controller.close()
      }
    })
    return blocks
  }
  const createCarBlob = () => new Response(createInputBlockStream().pipeThrough(new CARWriterStream([inputBlock.cid]))).blob()

  const car = await createCarBlob()

  const carBytes = new Uint8Array(await car.arrayBuffer())
  const carDataUri = /** @type {`data:${string}`} */(`data:application/vnd.ipld.car;base64,${btoa(String.fromCharCode.apply(null, Array.from(carBytes)))}`)
  /** @type {import('cardex/api').CARLink} */
  const carLink = Link.create(CAR.code, await sha256.digest(new Uint8Array(await car.arrayBuffer())))

  // make a mock location claim
  // that cid for input can be found at a url
  const claims = await Promise.resolve(new LinkMap()).then(async claims => {
    const locationClaim = Assert.location.invoke({
      issuer: signer,
      audience: signer,
      with: signer.did(),
      nb: {
        content: inputCID,
        location: [carDataUri]
      }
    })

    claims.set(inputCID, [
      await encodeInvocationBlock(locationClaim)
    ])
    // @todo build other kinds of claims e.g. assert/relation
    // e.g. in the wild '[{"content":{"/":"bafybeidtvuezudgvdciehupi2nlduu5t2r6nkb7o3brwqhdrig6jfc2gd4"},"location":["https://dotstorage-prod-1.s3.amazonaws.com/raw/bafybeidtvuezudgvdciehupi2nlduu5t2r6nkb7o3brwqhdrig6jfc2gd4/308707845265687115/ciqcoeewnamafntujapktkuekgfroq7tgapvehgxrg5mzyn4tdbh75y.car"],"type":"assert/location"},{"parts":[{"content":{"/":"bagbaierae4ijm2ayak3hisa6vgviiumlc5b7gma7kionpcn2ztq3zggcp73q"},"includes":{"content":{"/":"bagaqqeraqqipfw4sjfwsu5s3eykpcof2omov2v6vdg6gn7765wqo4sdjr7bq"}}}],"content":{"/":"bafybeidtvuezudgvdciehupi2nlduu5t2r6nkb7o3brwqhdrig6jfc2gd4"},"children":[],"type":"assert/relation"}]'
    return claims
  })

  const sharded = await shardCar({
    car,
    carLink: Promise.resolve(carLink),
    carpark
  })

  return {
    inputCID,
    inputBlock,
    car,
    carpark,
    carLink,
    claims,
    signer,
    sharded
  }
}

/**
 * start listening to a node http request listener.
 * return a URL to the listening server, and a function that will stop listening
 * @param {object} options
 * @param {import('http').RequestListener} options.listener
 * @param {number} port
 */
export async function listen ({ listener }, port = 0) {
  const server = http.createServer(listener)
  await new Promise((resolve) => server.listen(port, () => resolve(undefined)))
  const url = getServerUrl(server)
  const stop = () => {
    server.closeAllConnections()
    return new Promise((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve(error))
    })
  }
  return { url, stop }
}

/**
 * @param {import('node:http').Server} server
 * @returns URL
 */
function getServerUrl (server) {
  const address = server.address()
  if (typeof address !== 'object') { throw new Error(`unexpected non-object address ${address}`) }
  return new URL(`http://localhost:${address?.port}`)
}

/**
 * @template T
 * @param {AsyncIterable<T>} collectable
 */
export async function collect (collectable) {
  /** @type {T[]} */
  const items = []
  for await (const item of collectable) { items.push(item) }
  return items
}

const DEFAULT_SHARD_SIZE = 1024 * 1024 * 10

/**
 * @typedef {import('multiformats/link').Link<unknown, number, 18, 1>} CarSha256Link
 * @typedef {import('multiformats/link').Link<unknown, 514, 18, 1>} ShardCarSha256Link
 * @typedef {import('multiformats/link').Link<unknown, 1025, 18, 1>} MutlihashIndexSortedSha256Link
 * @typedef {LinkMap<ShardCarSha256Link, Promise<ArrayBuffer>>} ShardLinkToShardBytesMap
 */

/**
 * Shard a CAR File and return the shards + index
 * @param {object} options
 * @param {Blob} options.car
 * @param {Promise<import('cardex/api').CARLink>} options.carLink
 * @param {number} [options.shardSize]
 * @param {ShardLinkToShardBytesMap} [options.shards]
 * @param {Map<string, Uint8Array>} [options.dudewhere]
 * @param {import('../../src/kv-bucket/api.js').KVBucketWithRangeQueries} options.carpark - keys like `${cid}/${cid}.car` and values are car bytes
 * @param {Map<string, Uint8Array>} [options.satnav]
 * @param {LinkMap<MutlihashIndexSortedSha256Link, Promise<Uint8Array>>} [options.indexes] - values are MultihashSortedIndex bytes
 * @param {LinkMap<MutlihashIndexSortedSha256Link, Promise<Uint8Array>>} [options.indexCars] - values are CARs containing MultihashSortedIndex blocks
 * @param {LinkMap<import('multiformats').UnknownLink, Promise<Uint8Array>>} [options.cars]
 * @param {Array<{ shard: import('multiformats').UnknownLink, index: import('multiformats').UnknownLink }>} [options.shardOrder]
 */
async function shardCar ({
  car,
  carLink = Promise.resolve().then(async () => Link.create(CAR.code, await sha256.digest(new Uint8Array(await car.arrayBuffer())))),
  cars = new LinkMap(),
  carpark,
  dudewhere = new Map(),
  indexes = new LinkMap(),
  indexCars = new LinkMap(),
  satnav = new Map(),
  shards = new LinkMap(),
  shardSize = DEFAULT_SHARD_SIZE,
  shardOrder = []
}) {
  // shard the car
  const splitter = await TreewalkCarSplitter.fromIterable(car.stream(), shardSize)
  for await (const shardCar of splitter.cars()) {
    const shardCarBytes = collect(shardCar).then(concat)
    const shardCarCid = Link.create(CAR.code, await sha256.digest(await shardCarBytes))

    cars.set(shardCarCid, shardCarBytes)
    shards.set(shardCarCid, shardCarBytes)

    // park the car in carpark at conventional key
    await carpark.set(`${shardCarCid}/${shardCarCid}.car`, await shardCarBytes)

    // create index for shard
    const indexer = await CarIndexer.fromBytes(await shardCarBytes)
    const index = await createIndexStream(indexer)
    const indexBytes = (new Response(index)).blob().then(async b => new Uint8Array(await b.arrayBuffer()))
    const indexLink = Link.create(MULTIHASH_INDEX_SORTED_CODE, await sha256.digest(await indexBytes))

    indexes.set(indexLink, indexBytes)

    const indexCar = await CAR.encode(indexLink, [{ cid: indexLink, bytes: await indexBytes }])
    indexCars.set(indexLink, Promise.resolve(indexCar.bytes))
    cars.set(indexLink, Promise.resolve(indexCar.bytes))

    // add shard index to satnav at conventional key
    satnav.set(`${shardCarCid}/${shardCarCid}.car.idx`, await indexBytes)

    // add shard index *car* to carpark
    await carpark.set(`${indexCar.cid}/${indexCar.cid}.car`, indexCar.bytes)

    // keep track of shard order
    shardOrder.push({
      shard: shardCarCid,
      index: indexLink
    })
  }
  for (const cid of cars) {
    dudewhere.set(`${carLink}/${cid}.car`, new Uint8Array())
  }
  const indexRollupBytes = (new Response(createIndexRollupStream({ cars, satnav }))).blob().then(async b => new Uint8Array(await b.arrayBuffer()))
  dudewhere.set(`${carLink}/.rollup.idx`, await indexRollupBytes)
  return {
    carLink,
    cars,
    carpark,
    dudewhere,
    indexes,
    satnav,
    indexRollupBytes,
    indexCars
  }
}

/**
 * @param {object} options
 * @param {LinkMap<import('multiformats').UnknownLink, Promise<Uint8Array>>} options.cars
 * @param {Map<string, Uint8Array>} [options.satnav]
 */
function createIndexRollupStream ({
  cars,
  satnav
}) {
  const carCids = [...cars.keys()]
  const rollupGenerator = async function * () {
    yield encodeVarint(MultiIndexWriter.codec)
    yield encodeVarint(carCids.length)
    for (const origin of carCids) {
      yield origin.multihash.bytes
      if (satnav && origin.code === CAR.code) {
        const carIdx = satnav.get(`${origin}/${origin}.car.idx`)
        if (!carIdx) throw new Error(`missing index: ${origin}`)
      }
      if (!cars.has(origin)) throw new Error(`missing index: ${origin}`)
      const originCar = await cars.get(origin)
      yield originCar
    }
  }
  const iterator = rollupGenerator()
  const readable = new ReadableStream({
    async pull (controller) {
      const { value, done } = await iterator.next()
      if (done) {
        controller.close()
      } else {
        controller.enqueue(value)
      }
    }
  })
  return readable
}

/**
 * @param {CarIndexer} indexer
 */
async function createIndexStream (indexer) {
  const passthru = new TransformStream()
  const indexWriter = MultihashIndexSortedWriter.createWriter({ writer: passthru.writable.getWriter() })
  for await (const { cid, offset } of indexer) {
    indexWriter.add(cid, offset)
  }
  indexWriter.close()
  return passthru.readable
}
