import toMultiaddr from '@multiformats/uri-to-multiaddr'
import { createFromJSON } from '@libp2p/peer-id-factory'
import { Miniswap, BITSWAP_PROTOCOL } from 'miniswap'
import { identifyService } from 'libp2p/identify'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { mplex } from '@libp2p/mplex'
import { createLibp2p } from 'libp2p'

/** @typedef {import('./worker.js').Env} Env */

/**
 * Setup our libp2p service
 * @param {Env} env
 */
export async function getPeerId (env) {
  return createFromJSON(JSON.parse(env.PEER_ID_JSON))
}

/**
 * Setup our libp2p service
 * @param {Env} env
 * @param {import('cf-libp2p-ws-transport').WebSockets} transport
 * @param {import('@multiformats/multiaddr').Multiaddr} listenAddr
 */
export async function getLibp2p (env, transport, listenAddr) {
  const peerId = await getPeerId(env)
  const libp2p = await createLibp2p({
    peerId,
    addresses: { listen: [listenAddr.toString()] },
    transports: [() => transport],
    streamMuxers: [mplex(), yamux()],
    connectionEncryption: [noise()],
    services: {
      identify: identifyService()
    }
  })
  return libp2p
}

/**
 * @param {import('libp2p').Libp2p} libp2p
 * @param {import('./blocks.js').Blockstore} blockstore
 * @param {(err: Error) => Promise<void>} onError
 */
export function enableBitswap (libp2p, blockstore, onError = async () => {}) {
  const miniswap = new Miniswap({
    async has (cid) {
      try {
        const res = await blockstore.has(cid)
        return res
      } catch (err) {
        await onError(asError(err))
        return false
      }
    },
    async get (cid) {
      try {
        const res = await blockstore.get(cid)
        return res
      } catch (err) {
        await onError(asError(err))
      }
    }
  })
  libp2p.handle(BITSWAP_PROTOCOL, miniswap.handler)
}

/** @param {unknown} err */
function asError (err) {
  if (err instanceof Error) {
    return err
  }
  return new Error(`${err}`)
}

/**
 * Setup our libp2p websocket listener
 * @param {import('cf-libp2p-ws-transport').WebSockets} transport
 * @param {import('@multiformats/multiaddr').Multiaddr} listenAddr
 */
export function getWebSocketListener (transport, listenAddr) {
  // @ts-expect-error
  const listener = transport.listenerForMultiaddr(listenAddr)
  if (!listener) {
    throw new Error(`No listener for provided listen address ${listenAddr}`)
  }
  return listener
}

/**
 * Find our local listener multiaddr
 * @param {Request} request
 */
export function getListenAddr (request) {
  return toMultiaddr(request.url.replace('http', 'ws'))
}
