import { unstable_dev as testWorker } from 'wrangler'
import { multiaddr } from '@multiformats/multiaddr'
import { noise } from '@chainsafe/libp2p-noise'
import { webSockets } from '@libp2p/websockets'
import { identifyService } from 'libp2p/identify'
import { createLibp2p } from 'libp2p'
import { mplex } from '@libp2p/mplex'
import { peerId } from './fixture/peer.js'
import test from 'ava'
import { hasOwnProperty } from '../src/utils/object.js'
import { createSimpleContentClaimsScenario, listen, mockClaimsService } from './lib/content-claims-nodejs.js'
import assert from 'node:assert'
import { CARReaderStream } from 'carstream'
import { sha256 } from 'multiformats/hashes/sha2'
import * as bytes from 'multiformats/bytes'

/* global WritableStream */

/** @type {any[]} */
const workers = []

test.after(_ => {
  workers.forEach(w => w.stop())
})

/**
 * @typedef {"none" | "info" | "error" | "log" | "warn" | "debug"} LogLevel
 */

/**
 * @param {unknown} input
 * @returns {LogLevel | undefined}
 */
const createLogLevel = (input) => {
  const levels = /** @type {const} */ (['none', 'info', 'error', 'log', 'warn', 'debug'])
  // @ts-expect-error because input is string not LogLevel
  if (levels.includes(input)) {
    return /** @type {LogLevel} */ (input)
  }
}

/**
 * @param {Record<string, string>} env
 * @param {object} options
 * @param {"none" | "info" | "error" | "log" | "warn" | "debug"} [options.logLevel]
 */
async function createWorker (env = {}, { logLevel = createLogLevel(process.env.WORKER_TEST_LOG_LEVEL) } = {}) {
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
 * @param {string} worker.address
 * @param {number} worker.port
 */
function getListenAddr ({ port, address }) {
  return multiaddr(`/ip4/${address}/tcp/${port}/ws/p2p/${peerId.id}`)
}

test('get /', async t => {
  const worker = await createWorker()
  const resp = await worker.fetch()
  const text = await resp.text()
  t.regex(text, new RegExp(peerId.id))
})

test('libp2p identify', async t => {
  const worker = await createWorker({})
  const libp2p = await createLibp2p({
    connectionEncryption: [noise()],
    transports: [webSockets()],
    streamMuxers: [mplex()],
    services: {
      identify: identifyService()
    }
  })
  const peerAddr = getListenAddr(worker)
  console.warn(peerAddr)
  const peer = await libp2p.dial(peerAddr)
  t.is(peer.remoteAddr.getPeerId()?.toString(), peerId.id)
  await t.notThrowsAsync(() => libp2p.services.identify.identify(peer))
})
