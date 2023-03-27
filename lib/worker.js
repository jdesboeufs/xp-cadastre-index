import {parentPort} from 'node:worker_threads'
import Flatbush from 'flatbush'
import {search, closeDb} from './search.js'

let _config = null
let _processing = false

function configureWorker(config) {
  if (_config) {
    parentPort.postMessage({error: 'Worker already configured'})
    return
  }

  _config = config
  _config.rtreeIndex = Flatbush.from(config.rtreeBuffer)
  parentPort.postMessage({success: true})
}

function processJob(job) {
  if (!_config) {
    parentPort.postMessage({error: 'Worker not configured'})
    return
  }

  if (_processing) {
    parentPort.postMessage({error: 'Worker already processing a request'})
    return
  }

  _processing = true

  try {
    const result = search(job.location, {rtreeIndex: _config.rtreeIndex})
    _processing = false
    parentPort.postMessage({result})
  } catch (error) {
    _processing = false
    parentPort.postMessage({error: error.message})
  }
}

async function closeWorker() {
  await closeDb()
  parentPort.postMessage({closed: true})
}

parentPort.on('message', message => {
  if (message.event === 'configure') {
    return configureWorker(message.config)
  }

  if (message.event === 'job') {
    return processJob(message.job)
  }

  if (message.event === 'close') {
    return closeWorker()
  }
})
