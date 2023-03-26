import {search, closeDb} from './lib/search.js'

const minLat = 42.23064
const maxLat = 50.69999

const minLon = -5.19671
const maxLon = 8.15977

function getRandomLocation() {
  const lon = minLon + Math.random() * (maxLon - minLon)
  const lat = minLat + Math.random() * (maxLat - minLat)
  return [lon, lat]
}

console.time('lookup 100k items')

for (let i = 0; i < 100_000; i++) {
  const location = getRandomLocation()
  await search(location)
}

console.timeEnd('lookup 100k items')

await closeDb()
