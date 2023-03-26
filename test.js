import {search, closeDb} from './lib/search.js'

const minLat = 48.66402805022
const maxLat = 49.302906150321

const minLon = 5.816497974086
const maxLon = 6.359570167

function getRandomLocation() {
  const lon = minLon + Math.random() * (maxLon - minLon)
  const lat = minLat + Math.random() * (maxLat - minLat)
  return [lon, lat]
}

console.time('lookup 100k items')

for (let i = 0; i < 100_000; i++) {
  const location = getRandomLocation()
  const res = search(location)
  console.log(res.length)
}

console.timeEnd('lookup 100k items')

await closeDb()
