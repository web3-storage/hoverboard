import { unstable_dev as testWorker } from 'wrangler'
import { multiaddr } from '@multiformats/multiaddr'
import { noise } from '@chainsafe/libp2p-noise'
import { webSockets } from '@libp2p/websockets'
import { identifyService } from 'libp2p/identify'
import { createLibp2p } from 'libp2p'
import { mplex } from '@libp2p/mplex'
import { peerId } from './fixture/peer.js'
import test from 'ava'
import { createLogLevel } from './lib/log.js'

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
async function createWorker (env = {}, { logLevel = createLogLevel(env.WORKER_TEST_LOG_LEVEL) } = {}) {
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
  const ip = (address === 'localhost') ? '127.0.0.1' : address
  return multiaddr(`/ip4/${ip}/tcp/${port}/ws/p2p/${peerId.id}`)
}

test('get /', async t => {
  const { address, port } = await createWorker()
  const url = `http://${address === 'localhost' ? '127.0.0.1' : address}:${port}`
  const resp = await fetch(url)
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
