import booleanPointInPolygon from '@turf/boolean-point-in-polygon'
import {point} from '@turf/helpers'
import LMDB from 'lmdb'
import Pbf from 'pbf'
import geobuf from 'geobuf'

const lmdb = LMDB.open('./data/parcelles.lmdb', {readOnly: true})
const featuresDb = lmdb.openDB('features')
const relationsDb = lmdb.openDB('rtree-relations', {keyEncoding: 'uint32', encoding: 'string'})

export function search([lon, lat], {rtreeIndex}) {
  const indexResults = rtreeIndex.search(lon, lat, lon, lat)
  const candidates = indexResults.map(r => {
    const featureId = relationsDb.get(r)
    const buffer = featuresDb.getBinary(featureId)
    return geobuf.decode(new Pbf(buffer))
  })

  return candidates.filter(c => booleanPointInPolygon(point([lon, lat]), c))
}

export function closeDb() {
  lmdb.close()
}
