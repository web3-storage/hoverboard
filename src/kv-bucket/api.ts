export interface KVBucketWithRangeQueries {
  get (key: string, options?: { range?: { offset: number } }): Promise<Response>
  set (key: string, value: Uint8Array): Promise<void>
}
