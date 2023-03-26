import {Buffer} from 'node:buffer'
import {createReadStream} from 'node:fs'

const READ_FILE_CHUNK_SIZE = 64 * 1024 * 1024 // 64MB

async function readBigFile(filePath) {
  const fileReadStream = createReadStream(
    filePath,
    {highWaterMark: READ_FILE_CHUNK_SIZE}
  )

  const chunks = []

  for await (const chunk of fileReadStream) {
    chunks.push(chunk)
  }

  return Buffer.concat(chunks)
}

export default readBigFile
