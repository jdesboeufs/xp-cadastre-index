import {setTimeout} from 'node:timers/promises'
import {finished} from 'node:stream/promises'
import {rm, writeFile} from 'node:fs/promises'
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

  const db = LMDB.open(dbPath)
  const featuresDb = db.openDB('features')
  const relationsDb = db.openDB('rtree-relations', {keyEncoding: 'uint32', encoding: 'string'})

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

        const featureId = feature.properties.id

        const buffer = geobuf.encode(feature, new Pbf())
        const putFeaturePromise = featuresDb.put(featureId, LMDB.asBinary(buffer))
        const putRelationPromise = relationsDb.put(_idx, featureId)

        if (_idx === 0) {
          _startedAt = new Date()
        }

        _writing++

        Promise.all([putFeaturePromise, putRelationPromise]).then(() => {
          _writing--
          _written++
        })

        slowDown()
        _idx++
      }

      await Promise.all([
        featuresDb.flushed,
        relationsDb.flushed
      ])
    },

    async finish() {
      console.log(' * Fermeture de la base LMDB')

      await db.close()

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

      console.log(' * Écriture du R-tree sur le disque')

      await writeFile(dbPath + '-rtree', Buffer.from(index.data))

      console.log(' * Suppression du fichier temporaire')

      await rm(dbPath + '-bboxes.tmp')
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
