import {Transform} from 'node:stream'
import JSONStream from 'JSONStream'

export function createParser(jsonPath) {
  const parser = JSONStream.parse(jsonPath)
  const items = []
  let ended = false

  parser.on('data', item => items.push(item))
  parser.on('end', () => {
    ended = true
  })

  return new Transform({
    transform(chunk, enc, cb) {
      parser.write(chunk)

      while (items.length > 0) {
        this.push(items.shift())
      }

      cb()
    },

    flush(cb) {
      parser.end()

      while (items.length > 0) {
        this.push(items.shift())
      }

      if (items.length > 0 || !ended) {
        cb(new Error('JSON parser error'))
      }

      cb()
    },

    objectMode: true
  })
}
