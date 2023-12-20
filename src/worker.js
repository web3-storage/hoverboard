/* eslint-env serviceworker */
import { WebSockets } from 'cf-libp2p-ws-transport'
import { enableBitswap, getLibp2p, getListenAddr, getPeerId, getWebSocketListener } from './libp2p.js'
import { getBlockstore } from './blocks.js'
import { version } from '../package.json'
import { Metrics } from './metrics.js'

/**
 * @typedef {object} Env
 * @prop {string} CONTENT_CLAIMS - URL to default content claims service
 * @prop {string} DYNAMO_TABLE - block index table name
 * @prop {string} DYNAMO_REGION - block index table region
 * @prop {string} [DYNAMO_ENDPOINT] - override the dynamo api url
 * @prop {string} [S3_ENDPOINT] - override the s3 api url
 * @prop {string} [S3_REGIONS] - override the list of s3 regions to fetch blocks from
 * @prop {R2Bucket} CARPARK - R2 binding for CAR bucket
 * @prop {KVNamespace} DENYLIST - KV binding for denylist
 * @prop {string} AWS_ACCESS_KEY_ID - secret key id
 * @prop {string} AWS_SECRET_ACCESS_KEY - secret key
 * @prop {string} PEER_ID_JSON - secret stringified json peerId spec for this node
 */

/** @type {ExportedHandler<Env>} */
export default {
  /**
   * Handle requests.
   * - libp2p websocket requests hit `/p2p/:peerid` with the upgrade header set.
   * - other requests are assumed to be http
   */
  async fetch (request, env, ctx) {
    /** @type {import('@cloudflare/workers-types').WebSocket | undefined} */
    let websocket
    try {
      const upgrade = request.headers.get('Upgrade')
      if (upgrade === 'websocket') {
        const metrics = new Metrics()
        const transport = new WebSockets()
        const bs = await getBlockstore(env, ctx, metrics)
        const listenAddr = getListenAddr(request)
        const libp2p = await getLibp2p(env, transport, listenAddr)
        libp2p.addEventListener('peer:connect', (evt) => {
          const remotePeer = evt.detail
          console.log({ msg: 'peer:connect', peer: remotePeer.toString() })
        })
        libp2p.addEventListener('peer:disconnect', (evt) => {
          const remotePeer = evt.detail
          console.log({ msg: 'peer:disconnect', peer: remotePeer.toString(), ...metrics })
        })
        const onError = async (/** @type {Error} */ err) => {
          console.error('[!] error with bitswap', err)
          websocket?.close(418, err.message)
          await libp2p.stop()
          if (!err.message.startsWith('Too many subrequests')) {
            throw err
          }
        }
        enableBitswap(libp2p, bs, onError)
        const listener = getWebSocketListener(transport, listenAddr)
        const res = await listener.handleRequest(request)
        websocket = res.websocket
        return res
      }

      // not a libp2p req. handle as http
      const { pathname } = new URL(request.url)
      if (pathname === '' || pathname === '/') {
        const res = await getHome(request, env)
        return res
      }
      return new Response('Not Found', { status: 404 })
    } catch (err) {
      if (websocket) {
        // @ts-expect-error
        websocket.close(418, err.message)
      }
      console.error('unexpected error', err)
      throw err
    }
  }
}

/**
 * handler for GET /
 * @param {Request} request
 * @param {Env} env
 */
export async function getHome (request, env) {
  const peerId = await getPeerId(env)
  const addr = getListenAddr(request).encapsulate(`/p2p/${peerId}`)
  const body = `⁂ hoverboard v${version} ${addr}\n`
  return new Response(body, {
    headers: { 'content-type': 'text/plain; charset=utf-8' }
  })
}

/* HOVERBOARD 🛹 */
