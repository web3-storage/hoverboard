import { CompositeBlockstore, toCarKey } from '../src/blocks.js'
import test from 'ava'
import { MemoryBlockstore } from 'blockstore-core'
import * as Block from 'multiformats/block'
import * as mfjson from 'multiformats/codecs/json'
import { sha256 } from 'multiformats/hashes/sha2'

test('upgradeCarPath', t => {
  const rawCarPath = 'raw/bafybeieiltf3tnfdyvdutyolzhfahphgevnjsso26nulfqxtkptyefq3za/315318734258473269/ciqjxmllx5y73brw6mv3pkvd7sotfk2turkupkq7tsgygrdy2yxibri.car'
  const carKey = 'bagbaieratoywxp3r7wddn4zlw6vkh7e5gkvvhjcvi6vb7henqnchrvroqdcq/bagbaieratoywxp3r7wddn4zlw6vkh7e5gkvvhjcvi6vb7henqnchrvroqdcq.car'
  t.is(toCarKey(rawCarPath), carKey, 'raw carPath is converted to carKey')

  const completeCarPath = 'complete/bafybeieiltf3tnfdyvdutyolzhfahphgevnjsso26nulfqxtkptyefq3za.car'
  t.is(toCarKey(completeCarPath), undefined, 'complete carPath is not converted (yet)')

  t.is(toCarKey(carKey), carKey, 'passing a carKey is a no-op')
})

test('CompositeBlockstore can read from many blockstores', async t => {
  /** @param {number} i */
  const blockForIndex = (i) => Block.encode({
    value: i,
    codec: mfjson,
    hasher: sha256
  })
  const count = 2 + Math.floor(Math.random() * 5)
  const blockstores = await Promise.all(new Array(count).fill(undefined).map(async (e, i) => {
    const mem = new MemoryBlockstore()
    const block = await blockForIndex(i)
    await mem.put(block.cid, block.bytes)
    /** @type {import('../src/blocks.js').Blockstore} */
    const store = {
      async get (cid) {
        try {
          // @ts-expect-error cid is UnknownLink and this wants CID
          return await mem.get(cid)
        } catch (error) {
          return undefined
        }
      },
      async has (cid) {
        // @ts-expect-error cid is UnknownLink and this wants CID
        return mem.has(cid)
      }
    }
    return store
  }))
  const composed = new CompositeBlockstore(blockstores)
  // make sure we can read from each of the blockstores through the composed one
  for (let i = 0; i < count; i++) {
    const b = (await blockForIndex(i))
    const got = await composed.get(b.cid)
    t.assert(got, 'got block from compositeBlockStore')
    if (!got) {
      throw new Error('expected to get block but got falsy value')
    }
    const biDecoded = mfjson.decode(got)
    t.is(i.toString(), biDecoded.toString())
  }
})
