import {setTimeout} from 'node:timers/promises'
import {finished} from 'node:stream/promises'
import {rm} from 'node:fs/promises'
import {createGzip, createGunzip} from 'node:zlib'
import {createReadStream, createWriteStream} from 'node:fs'
import ndjson from 'ndjson'
import geobuf from 'geobuf'
import Pbf from 'pbf'
import Flatbush from 'flatbush'
import bbox from '@turf/bbox'
import LMDB from 'lmdb'
import {pEvent} from 'p-event'

export async function createSpatialIndexBuilder(dbPath) {
  await rm(dbPath, {force: true})

  const db = LMDB.open({
    path: dbPath,
    keyEncoding: 'uint32'
  })

  let _idx = 0
  let _writing = 0
  let _written = 0
  let _startedAt = null

  async function slowDown() {
    if (_writing > 10000) {
      await setTimeout(100)
      await slowDown()
    }
  }

  const bboxesWriteStream = createWriteStream(dbPath + '-bboxes.tmp')
  const bboxesStream = ndjson.stringify()
  bboxesStream.pipe(createGzip()).pipe(bboxesWriteStream)

  return {
    async writeStream(featureStream) {
      for await (const feature of featureStream) {
        const featureBbox = bbox(feature)
        if (!bboxesStream.write(featureBbox)) {
          await pEvent(bboxesStream, 'drain')
        }

        const buffer = geobuf.encode(feature, new Pbf())
        const putPromise = db.put(_idx, LMDB.asBinary(buffer))

        if (_idx === 0) {
          _startedAt = new Date()
        }

        _writing++

        putPromise.then(() => {
          _writing--
          _written++
        })

        slowDown()
        _idx++
      }

      await db.flushed
    },

    async finish() {
      console.log(' * Finalisation de l’écriture du fichier temporaire des bboxes')

      bboxesStream.end()
      await finished(bboxesWriteStream)

      console.log(' * Construction du R-tree')

      const index = new Flatbush(_written)
      const bboxFile = createReadStream(dbPath + '-bboxes.tmp')

      const bboxStream = bboxFile
        .pipe(createGunzip())
        .pipe(ndjson.parse())

      for await (const bbox of bboxStream) {
        index.add(...bbox)
      }

      index.finish()

      await rm(dbPath + '-bboxes.tmp')

      await db.put(999_999_999, LMDB.asBinary(Buffer.from(index.data)))
      await db.close()
    },

    get written() {
      return _written
    },

    get writing() {
      return _writing
    },

    get startedAt() {
      return _startedAt
    }
  }
}
