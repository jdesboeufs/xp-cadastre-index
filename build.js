#!/usr/bin/env node
import 'dotenv/config.js'

import process from 'node:process'
import {createGunzip} from 'node:zlib'
import got from 'got'

import {createParser} from './lib/json.js'
import {createSpatialIndexBuilder} from './lib/spatial-index-builder.js'

const DEPARTEMENTS = process.env.DEPARTEMENTS.split(',')

const spatialIndexBuilder = await createSpatialIndexBuilder('./parcelles.lmdb')

const writeFeaturesLoop = setInterval(() => {
  console.log({
    writing: spatialIndexBuilder.writing,
    written: spatialIndexBuilder.written,
    writeBySec: spatialIndexBuilder.written / (new Date() - spatialIndexBuilder.startedAt) * 1000
  })
}, 2000)

for (const dep of DEPARTEMENTS) {
  console.log(dep)

  const datasetUrl = `https://cadastre.data.gouv.fr/data/etalab-cadastre/2023-01-01/geojson/departements/${dep}/cadastre-${dep}-parcelles.json.gz`

  const featureStream = got.stream(datasetUrl, {responseType: 'buffer'})
    .pipe(createGunzip())
    .pipe(createParser('features.*'))

  await spatialIndexBuilder.writeStream(featureStream)
}

clearInterval(writeFeaturesLoop)

await spatialIndexBuilder.finish()

console.log('Terminé')
