import { unstable_dev as testWorker } from 'wrangler'
import { multiaddr } from '@multiformats/multiaddr'
import { noise } from '@chainsafe/libp2p-noise'
import { webSockets } from '@libp2p/websockets'
import { identifyService } from 'libp2p/identify'
import { createLibp2p } from 'libp2p'
import { mplex } from '@libp2p/mplex'

import test from 'ava'

const PEER_ID_JSON = '{"id":"Qmcv3CsJAN8ptXR8vm5a5GRrzkHGjaEUF9cQRGzYptMwzp","privKey":"CAASpgkwggSiAgEAAoIBAQDrJS8e0E/eXiYqlE65jS/H4NNuvXwKGPTQlThKcJ1h92g/HjgbEjL52Zux82amHVfaKI9Vw+5BiC+trwpne/4mHl4d0YD6Ndeo8VjRbEnrlhBx2pWTyvof1dZeR522TGq8vc63ek9kJRY6Hdu4yBuvr7VK7XSzbLoew9Hfun+BZMN13+miQ04EHAjOfvVoDUyR78Sk3ljM8Nqaq3dBPmkuBY5ADjbkpf94N7SuopjvEKnMtryDUhA/jxJU7+6/m5IN5mag2OgZZFov5XxKwdc1KWkLH+zjo7pWyJ4+VRpVwv1vcPy/Qs1/Xf61B4lvNFOOBPE3nI+kwu1jBauPuNc7AgMBAAECggEACQUIjBsJLmWzjV5H1RA87Rmvooep4Dr+Q1BBau9HM1Ly3kxdKSb02C/DrB+eKDnnoX8PXQawQDZmY4FJNU9Eso3nXN5NsYIq7Nh61h7J9oS4pZ6DQN8JUEQokHT0OU/Cgzsq6uHumfSnXuVSBQIQAT0MmeorRal93imqKxpcsNpHFSZdi1yNAWE9yxpXlF1LAkkZDmlDqdLg2zsAJQDZf/JnBak45i/E7AnDD/rR5t+VKeWupC9QA52X7MyAp9xeJCJmRgrHs9Bj/uQF5ozQqCBaIgekbh+OCxpzTqjpVNGwWuOxIUsQpAGqCpGzGIECbw5XN/qOEsuxpt45e3mtYQKBgQDy+t8vEi3AXCfpYRuI2+GNQI/6Uch4LJ6ohP0CHNLTtKpXEbBPfBAGryHWrR3SNTxjF7myFM5YR+V5eRBslkRJt9JSN11kAe1rA98SvmlHB3dnISgLKbrmg+fXvg3rEf6n57x8+Dsh9zwxQyHjTg2MArDdicnIyvdslYazeH+zSwKBgQD3vtZ/hql37bNLSIoJl+FiSHl57KoZSA/RIE0uVkqJtgCG4f6O1AMvyCDa7vtly460RtyaKg6ADW/jepsOydiOKKm1E/5MgG7ZttbrhPDfoXeHCEPzHs0J2I9D4OXQ8uSVbIZWKEcP5702lpHX5E0iWIVaErFTZEkuSazYwYUF0QKBgGcqYWyo+Uf9WOzcUEaRpXjF+tu7sbRkrZC7tnkZJ+K/iLujRQrakCtmXKW7pzfDZrpkLnIQJ3SQQjyLTI/uVVw/ckt6Omrl7ppLcIGS2zxPTUE6cLgcpcCOLPgLN/mhEFDWMc+VzfWj9ais6kyKrXHPCq1lfYmFs/wkmKbG+OF1AoGAWh0CLHSw47yEQNfrzb1+757pAJ5C6Ns7VZfoOviMODcJTgaZ7x3S4uhqevf+XXKDP7OevjyZ9Z3tmtKX3/MvX9YOlJznHLCCoZJN7nD2pIE41tb80EvdzL0Gr2v4a95NosRMwL35yyFBw79U4TBbliiYqMCv22NY8ws9YcJSPmECgYALiFLv7Z90NOYsi0pZh3koxKtYEOEmaNIqOlBbXP2J0eo6wJKFR0nbu8t+s5poJUiU9t+e1V+Lyjy6aFg/fWvc9b9ZB9fm+MIltHVaFGsC7cMhPn3Q8demehkQvTl2XBxnhOobPdFxxJsjpDpShOgRfKBzwLEH5arLmDUrw6FNuQ==","pubKey":"CAASpgIwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDrJS8e0E/eXiYqlE65jS/H4NNuvXwKGPTQlThKcJ1h92g/HjgbEjL52Zux82amHVfaKI9Vw+5BiC+trwpne/4mHl4d0YD6Ndeo8VjRbEnrlhBx2pWTyvof1dZeR522TGq8vc63ek9kJRY6Hdu4yBuvr7VK7XSzbLoew9Hfun+BZMN13+miQ04EHAjOfvVoDUyR78Sk3ljM8Nqaq3dBPmkuBY5ADjbkpf94N7SuopjvEKnMtryDUhA/jxJU7+6/m5IN5mag2OgZZFov5XxKwdc1KWkLH+zjo7pWyJ4+VRpVwv1vcPy/Qs1/Xf61B4lvNFOOBPE3nI+kwu1jBauPuNc7AgMBAAE="}'
const PEER_ID = JSON.parse(PEER_ID_JSON).id

const workers = []

test.after(_ => {
  workers.forEach(w => w.stop())
})

/**
 * @param {Record<string, string>} env
 */
async function createWorker (env = {}) {
  const w = await testWorker('src/worker.js', {
    vars: {
      PEER_ID_JSON,
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
 * @param {string} worker.ip
 * @param {number} worker.port
 */
function getListenAddr ({ port, address }) {
  return multiaddr(`/ip4/${address}/tcp/${port}/ws/p2p/${PEER_ID}`)
}

test('get /', async t => {
  const worker = await createWorker()
  const resp = await worker.fetch()
  const text = await resp.text()
  t.regex(text, new RegExp(PEER_ID))
})

test('libp2p identify', async t => {
  const worker = await createWorker()
  const libp2p = await createLibp2p({
    connectionEncryption: [noise()],
    transports: [webSockets()],
    streamMuxers: [mplex()],
    services: {
      identify: identifyService()
    }
  })
  const peerAddr = getListenAddr(worker)
  console.log(peerAddr)
  const peer = await libp2p.dial(peerAddr)
  t.is(peer.remoteAddr.getPeerId().toString(), PEER_ID)
  await t.notThrowsAsync(() => libp2p.services.identify.identify(peer))
})
