import Flatbush from 'flatbush'
import booleanPointInPolygon from '@turf/boolean-point-in-polygon'
import {point} from '@turf/helpers'
import LMDB from 'lmdb'
import Pbf from 'pbf'
import geobuf from 'geobuf'

const lmdb = LMDB.open({
  path: './parcelles.lmdb',
  keyEncoding: 'uint32',
  readOnly: true
})

const rtreeDataView = lmdb.getBinary(999_999_999)

const rtreeData = rtreeDataView.buffer.slice(
  rtreeDataView.byteOffset,
  rtreeDataView.byteOffset + rtreeDataView.byteLength
)

const index = Flatbush.from(rtreeData)

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
