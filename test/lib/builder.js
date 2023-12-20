/* eslint-env browser */
import { CID } from 'multiformats/cid'
import { sha256 } from 'multiformats/hashes/sha2'
import { base58btc } from 'multiformats/bases/base58'
import { CarIndexer } from '@ipld/car'
import { UnixFS, ShardingStream } from '@web3-storage/upload-client'
import { PutObjectCommand } from '@aws-sdk/client-s3'
import { PutItemCommand } from '@aws-sdk/client-dynamodb'
import { marshall } from '@aws-sdk/util-dynamodb'

const carCode = 0x0202

export class Builder {
  #dynamoClient
  #dynamoTable
  #s3Client
  #s3Region
  #s3Bucket

  /**
   * @param {import('@aws-sdk/client-dynamodb').DynamoDBClient} dynamoClient
   * @param {string} dynamoTable
   * @param {import('@aws-sdk/client-s3').S3Client} s3Client
   * @param {string} s3Region
   * @param {string} s3Bucket
   */
  constructor (dynamoClient, dynamoTable, s3Client, s3Region, s3Bucket) {
    this.#dynamoClient = dynamoClient
    this.#dynamoTable = dynamoTable
    this.#s3Client = s3Client
    this.#s3Region = s3Region
    this.#s3Bucket = s3Bucket
  }

  /** @param {Uint8Array} bytes CAR file bytes */
  async #writeCar (bytes) {
    const cid = CID.createV1(carCode, await sha256.digest(bytes))
    const key = `${cid}/${cid}.car`
    const command = new PutObjectCommand({
      Bucket: this.#s3Bucket,
      Key: key,
      Body: bytes
    })
    await this.#s3Client.send(command)
    return cid
  }

  /**
   * @param {import('multiformats').Link} cid CAR CID
   * @param {Uint8Array} bytes CAR file bytes
   */
  async #writeIndex (cid, bytes) {
    const indexer = await CarIndexer.fromBytes(bytes)
    for await (const entry of indexer) {
      const command = new PutItemCommand({
        TableName: this.#dynamoTable,
        Item: marshall({
          blockmultihash: base58btc.encode(entry.cid.multihash.bytes),
          carpath: `${this.#s3Region}/${this.#s3Bucket}/${cid}/${cid}.car`,
          offset: entry.blockOffset,
          length: entry.blockLength
        })
      })
      await this.#dynamoClient.send(command)
    }
  }

  /**
   * @param {import('@web3-storage/upload-client/types').BlobLike|import('@web3-storage/upload-client/types').FileLike[]} input
   * @param {import('@web3-storage/upload-client/types').ShardingOptions} [options]
   * @returns {Promise<import('multiformats').UnknownLink | null>}
   */
  async add (input, options = {}) {
    console.log('Adding ' + (Array.isArray(input) ? `${input.length} file(s)` : '1 blob') + '...')
    const unixFsEncoder = Array.isArray(input)
      ? UnixFS.createDirectoryEncoderStream(input)
      : UnixFS.createFileEncoderStream(input)

    /** @type {import('multiformats').UnknownLink?} */
    let root = null
    await unixFsEncoder
      .pipeThrough(new TransformStream({
        transform (block, controller) {
          root = block.cid
          controller.enqueue(block)
        }
      }))
      .pipeThrough(new ShardingStream(options))
      .pipeTo(new WritableStream({
        write: async car => {
          const bytes = new Uint8Array(await car.arrayBuffer())
          const cid = await this.#writeCar(bytes)
          await this.#writeIndex(cid, bytes)
        }
      }))

    if (!root) throw new Error('no blocks generated')
    return root
  }
}
