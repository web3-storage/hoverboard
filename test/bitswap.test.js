import { unstable_dev as testWorker } from 'wrangler'
import { multiaddr } from '@multiformats/multiaddr'
import { noise } from '@chainsafe/libp2p-noise'
import { webSockets } from '@libp2p/websockets'
import { identifyService } from 'libp2p/identify'
import { createLibp2p } from 'libp2p'
import { mplex } from '@libp2p/mplex'
import { createHelia } from 'helia'
import { unixfs } from '@helia/unixfs'
import { Builder } from './lib/builder.js'
import { createDynamo, createDynamoTable, createS3, createS3Bucket } from './lib/aws.js'
import { peerId } from './fixture/peer.js'
import test from 'ava'
import assert from 'node:assert'
import { CID } from 'multiformats'
import { collect, createSimpleContentClaimsScenario, generateClaims, listen, mockClaimsService } from './lib/content-claims-nodejs.js'
import { Signer as Ed25519Signer } from '@ucanto/principal/ed25519'
import * as Link from 'multiformats/link'
import * as CAR from '../src/car.js'
import { sha256 } from 'multiformats/hashes/sha2'
import { Miniflare, Log, LogLevel } from 'miniflare'
import { Map as LinkMap } from 'lnmap'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import { createLogLevel } from './lib/log.js'
import { createBucketFromR2Miniflare } from '../src/kv-bucket/kv-bucket-cloudflare.js'

const __dirname = fileURLToPath(new URL('.', import.meta.url))

/* global Blob */

/** @type {any[]} */
const workers = []

test.after(async _ => {
  await Promise.allSettled(workers.map(w => w.stop()))
})

/**
 * @param {Record<string, string>} env
 * @param {object} options
 * @param {"none" | "info" | "error" | "log" | "warn" | "debug"} [options.logLevel]
 */
async function createWorker (env = {}, { logLevel = createLogLevel(process?.env.WORKER_TEST_LOG_LEVEL) } = {}) {
  const w = await testWorker('src/worker.js', {
    ...(logLevel ? { logLevel } : {}),
    vars: {
      PEER_ID_JSON: JSON.stringify(peerId),
      ...env
    },
    experimental: {
      disableExperimentalWarning: true
    }
  })
  workers.push(w)
  return w
}

/**
 * @param {object} worker
 * @param {number} worker.port
 * @param {string} worker.address
 */
function getListenAddr ({ port, address }) {
  const ip = (address === 'localhost') ? '127.0.0.1' : address
  return multiaddr(`/ip4/${ip}/tcp/${port}/ws/p2p/${peerId.id}`)
}

/**
 * - create dag, pack to car, add indexes to dynamo and blocks to s3
 * - start hoverboard worker
 * - start local libp2p node, connect to hoverboard
 * - wrap libp2p in helia, fetch rood cid of dag from helia
 * - assert blocks are in helia, and data round trips
 */
test('helia bitswap', async t => {
  t.timeout(60_000)
  const { expected, libp2p, helia, heliaFs, hoverboard, root } = await createHeliaBitswapScenario()

  console.log(`Dialing ${hoverboard}`)
  const peer = await libp2p.dial(hoverboard)
  t.is(peer.remoteAddr.getPeerId()?.toString(), peerId.id)

  const decoder = new TextDecoder('utf8')
  let text = ''

  assert.ok(root)
  const rootCid = CID.create(root.version, root.code, root.multihash)

  console.log(`Fetching ${rootCid}`)
  for await (const chunk of heliaFs.cat(rootCid)) {
    text += decoder.decode(chunk, { stream: true })
  }

  text += decoder.decode()

  t.true(await helia.blockstore.has(rootCid), 'block should now be in helia blockstore')

  t.is(text, expected, 'bitswap roundtrippin')
})

test('helia bitswap + content-claims', async t => {
  const claims = new LinkMap()
  const claimsServer = await listen(mockClaimsService(claims))
  try {
    await useClaimsServer(claimsServer)
  } finally {
    await claimsServer.stop()
  }
  /**
   * @param {{ url: URL }} options
   */
  async function useClaimsServer ({ url }) {
    const { libp2p, heliaFs, hoverboard, miniflare } = await createHeliaBitswapScenarioMiniflare({ contentClaims: { url: claimsServer.url.toString() } })
    const carpark = await miniflare.getR2Bucket('CARPARK')
    const testInput = `test-${Math.random().toString().slice(2)}`
    const scenario = await createSimpleContentClaimsScenario(testInput, createBucketFromR2Miniflare(carpark))
    const firstIndex = [...scenario.sharded.indexes.entries()][0]
    const firstIndexCar = await scenario.sharded.indexCars.get(firstIndex[0])
    assert.ok(firstIndexCar)
    // writes into `claims` which is used by `claimsServer`
    await generateClaims(
      claims,
      await Ed25519Signer.generate(),
      scenario.inputCID,
      scenario.carLink,
      scenario.car.stream(),
      firstIndex[0],
      Link.create(CAR.code, await sha256.digest(firstIndexCar))
    )

    const peer = await libp2p.dial(hoverboard)
    t.is(peer.remoteAddr.getPeerId()?.toString(), peerId.id)

    // we should be able to use helia.cat(rootCid) and get blocks that come form contentA.unixfsCar
    const contentACat = await collect(heliaFs.cat(scenario.inputCID)).then(a => a[0])
    const contentACatText = await (import('uint8arrays').then(m => m.toString(contentACat)))
    t.is(contentACatText, testInput)
  }
})

/**
 * test scenario that setups up a hoverboard running in a worker,
 * a libp2p that can connect to it over websocket,
 * and some example data to test with
 * @param {object} options
 * @param {object} [options.contentClaims] - content claims service
 * @param {string} [options.contentClaims.url] - url of content claims service
 */
async function createHeliaBitswapScenario ({ contentClaims } = {}) {
  const [dynamo, s3] = await Promise.all([createDynamo(), createS3()])
  const [table, bucket] = await Promise.all([createDynamoTable(dynamo.client), createS3Bucket(s3.client)])
  const builder = new Builder(dynamo.client, table, s3.client, 'us-west-2', bucket)
  const encoder = new TextEncoder()
  const expected = 'hoverboard 🛹'
  const blob = new Blob([encoder.encode(expected)])
  const root = await builder.add(blob)

  console.warn('Creating local hoverboard')
  const worker = await createWorker({
    S3_ENDPOINT: s3.endpoint,
    DYNAMO_ENDPOINT: dynamo.endpoint,
    DYNAMO_REGION: dynamo.region,
    DYNAMO_TABLE: table,
    AWS_ACCESS_KEY_ID: s3.credentials.accessKeyId,
    AWS_SECRET_ACCESS_KEY: s3.credentials.secretAccessKey,
    PEER_ID_JSON: JSON.stringify(peerId),
    ...(contentClaims?.url ? { CONTENT_CLAIMS: contentClaims.url } : {})
  })
  const hoverboard = getListenAddr(worker)
  console.warn('Created hoverboard', hoverboard)

  console.warn('Creating local libp2p')
  const libp2p = await createLibp2p({
    connectionEncryption: [noise()],
    transports: [webSockets()],
    streamMuxers: [mplex()],
    services: {
      identify: identifyService()
    }
  })

  console.warn('Creating local helia')
  const helia = await createHelia({
    libp2p
  })
  const heliaFs = unixfs(helia)
  return {
    expected,
    helia,
    heliaFs,
    hoverboard,
    libp2p,
    root
  }
}

/**
 * test scenario that setups up a hoverboard running in a worker,
 * a libp2p that can connect to it over websocket,
 * and some example data to test with
 * @param {object} options
 * @param {object} [options.contentClaims] - content claims service
 * @param {string} [options.contentClaims.url] - url of content claims service
 */
async function createHeliaBitswapScenarioMiniflare (
  { contentClaims } = {},
  { logLevel = LogLevel.DEBUG } = {}
) {
  const [dynamo, s3] = await Promise.all([createDynamo(), createS3()])
  const [table, bucket] = await Promise.all([createDynamoTable(dynamo.client), createS3Bucket(s3.client)])
  const builder = new Builder(dynamo.client, table, s3.client, 'us-west-2', bucket)
  const encoder = new TextEncoder()
  const expected = 'hoverboard 🛹'
  const blob = new Blob([encoder.encode(expected)])
  const root = await builder.add(blob)

  const miniflareHttp = {
    host: '127.0.0.1',
    port: 63000 + Math.floor(Math.random() * 1000)
  }
  const miniflare = new Miniflare({
    ...miniflareHttp,
    scriptPath: path.join(__dirname, '../dist/worker.js'),
    compatibilityDate: '2023-10-30',
    compatibilityFlags: [
      // https://developers.cloudflare.com/workers/configuration/compatibility-dates/#nodejs-compatibility-flag
      'nodejs_compat'
    ],
    // https://github.com/cloudflare/miniflare/issues/625#issuecomment-1798422794
    modules: true,
    log: new Log(logLevel),
    r2Buckets: [
      'CARPARK'
    ],
    bindings: {
      S3_ENDPOINT: s3.endpoint,
      DYNAMO_ENDPOINT: dynamo.endpoint,
      DYNAMO_REGION: dynamo.region,
      DYNAMO_TABLE: table,
      AWS_ACCESS_KEY_ID: s3.credentials.accessKeyId,
      AWS_SECRET_ACCESS_KEY: s3.credentials.secretAccessKey,
      PEER_ID_JSON: JSON.stringify(peerId),
      ...(contentClaims?.url ? { CONTENT_CLAIMS: contentClaims.url } : {})
    }
  })

  console.warn('Creating local hoverboard')
  const hoverboard = getListenAddr({
    ...miniflareHttp,
    address: miniflareHttp.host
  })
  console.warn('Created hoverboard', hoverboard)

  console.warn('Creating local libp2p')
  const libp2p = await createLibp2p({
    connectionEncryption: [noise()],
    transports: [webSockets()],
    streamMuxers: [mplex()],
    services: {
      identify: identifyService()
    }
  })

  console.warn('Creating local helia')
  const helia = await createHelia({
    libp2p
  })
  const heliaFs = unixfs(helia)
  return {
    expected,
    helia,
    heliaFs,
    hoverboard,
    libp2p,
    root,
    miniflare
  }
}
