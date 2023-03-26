import Flatbush from 'flatbush'
import booleanPointInPolygon from '@turf/boolean-point-in-polygon'
import {point} from '@turf/helpers'
import LMDB from 'lmdb'
import Pbf from 'pbf'
import geobuf from 'geobuf'

import readBigFile from './read-big-file.js'

const lmdb = LMDB.open({
  path: './data/parcelles.lmdb',
  keyEncoding: 'uint32',
  readOnly: true
})
const rtreeBuffer = await readBigFile('./data/parcelles.lmdb-rtree')

const index = Flatbush.from(rtreeBuffer)

export function search([lon, lat]) {
  const indexResults = index.search(lon, lat, lon, lat)
  const candidates = indexResults.map(r => {
    const buffer = lmdb.getBinary(r)
    return geobuf.decode(new Pbf(buffer))
  })

  return candidates.filter(c => booleanPointInPolygon(point([lon, lat]), c))
}

export function closeDb() {
  lmdb.close()
}
