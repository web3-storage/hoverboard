import test from 'ava'
import { createSimpleContentClaimsScenario, generateClaims, listen, mockClaimsService } from './lib/content-claims-nodejs.js'
import { ContentClaimsBlockstore } from '../src/content-claims/content-claims-blockstore.js'
import * as Claims from '@web3-storage/content-claims/client'
import { Signer as Ed25519Signer } from '@ucanto/principal/ed25519'
import * as Link from 'multiformats/link'
import * as CAR from '../src/car.js'
import { sha256 } from 'multiformats/hashes/sha2'
import assert from 'assert'

test('ContentClaimsBlockstore can get block from location claim data URL', async t => {
  const testInput = `test-${Math.random().toString().slice(2)}`
  const { claims, inputCID, carpark } = await createSimpleContentClaimsScenario(testInput)
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
    const blocks = new ContentClaimsBlockstore({ url, read: Claims.read, carpark })
    t.assert(await blocks.has(inputCID), '.has(inputCID)')
    const block = await blocks.get(inputCID)
    t.assert(block, 'got block')
    const decodedBlock = (new TextDecoder()).decode(block)
    t.is(decodedBlock, testInput)
  }
})

test('ContentClaimsBlockStore can get block from relation claim', async t => {
  const testInput = `test-${Math.random().toString().slice(2)}`
  const scenario = await createSimpleContentClaimsScenario(testInput)
  const claimsIssuer = await Ed25519Signer.generate()
  const firstIndex = [...scenario.sharded.indexes.entries()][0]
  const firstIndexCar = await scenario.sharded.indexCars.get(firstIndex[0])
  assert.ok(firstIndexCar)
  const firstIndexCarLink = Link.create(CAR.code, await sha256.digest(firstIndexCar))
  const claims = await generateClaims(
    undefined,
    claimsIssuer,
    scenario.inputCID,
    scenario.carLink,
    scenario.car.stream(),
    firstIndex[0],
    firstIndexCarLink
  )
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
    const blocks = new ContentClaimsBlockstore({
      url,
      read: Claims.read.bind(Claims),
      carpark: scenario.sharded.carpark
    })
    t.assert(await blocks.has(scenario.inputCID), '.has(inputCID)')
    const block = await blocks.get(scenario.inputCID)
    t.assert(block, 'got block')
    const decodedBlock = (new TextDecoder()).decode(block)
    t.is(decodedBlock, testInput)
  }
})
