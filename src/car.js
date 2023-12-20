import { CarReader, CarWriter } from '@ipld/car'
import { concat } from 'uint8arrays'
import * as Link from 'multiformats/link'
import { sha256 } from 'multiformats/hashes/sha2'

/**
 * multicodec code indicating content is a CAR file
 * @see https://github.com/multiformats/multicodec/blob/master/table.csv#L140
 */
export const code = 0x0202

/**
 * @param {import('multiformats').UnknownLink} root
 * @param {Array<{ cid: import('multiformats').UnknownLink, bytes: Uint8Array }>} blocks
 * @returns {Promise<{ cid: import('cardex/api').CARLink, bytes: Uint8Array }>}
 */
export async function encode (root, blocks) {
  // @ts-expect-error
  const { writer, out } = CarWriter.create(root)
  for (const b of blocks) {
    writer.put(b)
  }
  writer.close()
  const chunks = []
  for await (const chunk of out) {
    chunks.push(chunk)
  }
  const bytes = concat(chunks)
  const digest = await sha256.digest(bytes)
  return { cid: Link.create(code, digest), bytes }
}

/**
 * @param {Uint8Array} bytes
 * @returns {Promise<Array<{ cid: import('multiformats').UnknownLink, bytes: Uint8Array }>>}
 */
export async function decode (bytes) {
  const reader = await CarReader.fromBytes(bytes)
  const blocks = []
  for await (const b of reader.blocks()) {
    blocks.push({
      cid: /** @type {import('multiformats').UnknownLink} */ (b.cid),
      bytes: b.bytes
    })
  }
  return blocks
}
