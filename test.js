
import {Worker} from 'node:worker_threads'
import {pEvent} from 'p-event'
import PQueue from 'p-queue'
import readBigFile from './lib/read-big-file.js'

const rtreeBuffer = await readBigFile('./data/parcelles.lmdb-rtree')

const minLat = 42.23064
const maxLat = 50.69999

const minLon = -5.19671
const maxLon = 8.15977

function getRandomLocation() {
  const lon = minLon + Math.random() * (maxLon - minLon)
  const lat = minLat + Math.random() * (maxLat - minLat)
  return [lon, lat]
}



async function createWorker() {
  const worker = new Worker('./lib/worker.js')
  worker.postMessage({event: 'configure', config: {rtreeBuffer}})
  const response = await pEvent(worker, 'message')

  if (response.error) {
    throw new Error(response.error)
  }

  return worker
}

const NUM_WORKERS = 4

const _workers = await Promise.all(
  Array(NUM_WORKERS).fill(0).map(() => createWorker())
)

const _jobsQueue = []
const _idleWorkers = [..._workers]

function enqueueJob(job) {
  return new Promise((resolve, reject) => {
    _jobsQueue.push({job, resolve, reject})
    execNextJob()
  })
}

async function execNextJob() {
  if (_jobsQueue.length === 0) {
    return
  }

  if (_idleWorkers.length === 0) {
    return
  }

  const job = _jobsQueue.shift()
  const worker = _idleWorkers.shift()

  worker.postMessage({event: 'job', job: job.job})
  const result = await pEvent(worker, 'message')

  _idleWorkers.push(worker)

  if (result.error) {
    job.reject(new Error(result.error))
  } else {
    job.resolve(result.result)
  }

  execNextJob()
}

console.time('lookup 1m items')

const q = new PQueue({concurrency: NUM_WORKERS})

for (let i = 0; i < 1_000_000; i++) {
  const location = getRandomLocation()
  q.add(() => enqueueJob({location}))
  await q.onSizeLessThan(NUM_WORKERS * 4)
}

await q.onIdle()

console.timeEnd('lookup 1m items')

await Promise.all(_workers.map(async worker => {
  worker.postMessage({event: 'close'})
  await pEvent(worker, 'message')
  worker.terminate()
}))
