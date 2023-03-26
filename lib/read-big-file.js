import {Buffer} from 'node:buffer'
import {createReadStream} from 'node:fs'
import {stat} from 'node:fs/promises'

const READ_FILE_CHUNK_SIZE = 64 * 1024 * 1024 // 64MB

async function readBigFile(filePath) {
  const {size: fileSize} = await stat(filePath)

  const sharedArrayBuffer = new SharedArrayBuffer(fileSize)
  const view = new Uint8Array(sharedArrayBuffer)

  const fileReadStream = createReadStream(
    filePath,
    {highWaterMark: READ_FILE_CHUNK_SIZE}
  )

  let offset = 0

  for await (const chunk of fileReadStream) {
    for (let i = 0; i < chunk.length; ++i) {
      view[offset + i] = chunk[i]
    }

    offset += chunk.length
  }

  return sharedArrayBuffer
}

export default readBigFile
