import { Link, UnknownLink } from 'multiformats/link'
import { MultihashIndexItem } from 'cardex/multihash-index-sorted/api'
import { CARLink } from 'cardex/api'
import * as raw from 'multiformats/codecs/raw'

export interface IndexEntry extends MultihashIndexItem {
  origin: CARLink
}

export interface Index {
  get (c: UnknownLink): Promise<IndexEntry|undefined>
}

// the keys in an index map are CIDs of the hash of the index entry
export type RawIndexEntryLink = Link<unknown, typeof raw['code'], number, 1>

export type IndexMap = {
  has (key: UnknownLink): Promise<boolean>
  set (key: UnknownLink, value: IndexEntry): Promise<void>
}
