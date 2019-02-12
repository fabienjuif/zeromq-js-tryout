// FIXME: worker disconnect before sending a response (ack)
// FIXME: retry
// This is the second version, this balancer doesn't wait a worker responds from a previous request
// Instead it random on all availables workers

// a client wants something, I check my workers and ask them if they are ok
const zmq = require('zeromq')

const createQueue = () => {
  /** This map keep track of workers that listen given topic */
  const queue = new Map()

  /** This map keep track of workers topics */
  const topics = new Map()

  const add = (topic, identity) => {
    const topicStr = topic.toString()
    const identityStr = identity.toString()

    if (!queue.has(topicStr)) queue.set(topicStr, []) // TODO: should be uniq
    console.log('adding identity to queue...', topicStr, identityStr)
    queue.get(topicStr).push(identityStr)

    if (!topics.has(identity)) topics.set(identityStr, new Set())
    console.log('adding topic to identity...', topicStr, identityStr)
    topics.get(identityStr).add(topicStr)
  }

  const get = (topic) => {
    const topicStr = topic.toString()

    console.log('getting an identity for topic...', topicStr)

    if (!queue.has(topicStr)) return undefined
    if (queue.get(topicStr).length === 0) return undefined

    const availablesIdentities = queue.get(topicStr)

    // TODO: round robin
    const randIndex = Math.ceil(Math.random() * availablesIdentities.length - 1)
    console.log(`random ${randIndex + 1} / ${availablesIdentities.length}`)
    return availablesIdentities[randIndex]
  }

  const forEach = (topic, callback) => {
    const topicStr = topic.toString()

    console.log('forEach identity on topic...', topicStr)
    if (!queue.has(topicStr)) return
    if (queue.get(topicStr).size === 0) return

    queue.get(topicStr).forEach(id => callback(id))
    queue.delete(topicStr)
  }

  const remove = (identity) => {
    const identityStr = identity.toString()
    console.log(`removing identity ${identityStr} from queue...`)

    const identityTopics = topics.get(identityStr)
    if (!identityTopics || identityTopics.size === 0) return

    identityTopics.forEach((topic) => {
      // TODO: optimize next line (by changing collection type ?)
      const filteredIdentities = queue.get(topic).filter(identity => identity !== identityStr)

      if (filteredIdentities.length === 0) {
        queue.delete(topic)
        console.log(`This topic does not have identites attached: ${topic}`)
      } else {
        queue.set(topic, filteredIdentities)
      }
    })

    topics.delete(identityStr)
  }

  const send = (sock, message) => {
    let sent = false
    try {
      sock.send(message)
      sent = true
    } catch (ex) {
      if (ex.message !== 'Host unreachable') throw ex
      remove(message[0])
    }
    return sent
  }

  return {
    add,
    get,
    forEach,
    remove,
    send
  }
}

const workers = createQueue()
const clients = createQueue()

let tasks = []

const sock = zmq.socket('router')
sock.bindSync('tcp://127.0.0.1:3000')
sock.setsockopt(zmq.ZMQ_ROUTER_MANDATORY, 1)
console.log('Load balancer listening to tcp://127.0.0.1:3000')

sock.on('message', (identity, type, topic, payload) => {
  if (!type || type.length === 0) throw new Error('Message type should be set!')

  // registering a worker
  if (type.toString() === '@@REGISTER') {
    console.log('REGISTER')
    workers.add(topic, identity)

    return
  }

  // response from a worker
  if (!topic || topic.length === 0) {
    console.log('RESPOND TO CLIENT')

    tasks = tasks.filter(task => task.topic !== type.toString()) // TODO: optimize
    clients.forEach(type, id => clients.send(sock, [id, '', payload]))

    return
  }

  console.log('ASK WORKER', topic.toString())

  // client ask something to a worker
  clients.add(topic, identity) // TODO: add something so the client can not ask for response

  // TODO: move all of this (and retry part) to a function
  let worker = workers.get(type)
  const task = { topic: topic.toString(), workerTopic: type.toString(), worker, date: Date.now(), retry: 0, payload }
  tasks.push(task)
  if (worker && !workers.send(sock, [worker, '', payload])) {
    worker = workers.get(type)
    if (worker) {
      task.worker = worker
      workers.send(sock, [worker, '', payload])
    }
  }

  console.log(`There is ${tasks.length} tasks in the cluster...`)
})

// this interval is used to chase tasks that are dead so we can retry them
const RETRY_INTERVAL = 100
const RETRY_TIMEOUT = 5000
const MAX_RETRY = 3
setInterval(
  () => {
    const now = Date.now()

    let done = false
    for (let i = 0; i < tasks.length && !done; i += 1) {
      const task = tasks[i]
      if (task.date + RETRY_TIMEOUT <= now) {
        // TODO: use a centralized function to send to workers
        console.log(`RETRYING ${task.workerTopic} -> ${task.topic}`)
        const worker = workers.get(task.workerTopic)
        task.date = now
        task.retry += 1
        task.worker = worker
        if (task.retry > MAX_RETRY) {
          console.log(`Max retry reach for task ${task.workerTopic} -> ${task.topic}`)
          tasks = tasks.filter(t => t !== task) // TODO: optimize
          console.log(`There is ${tasks.length} tasks in the cluster...`)
        } else if (worker) workers.send(sock, [worker, '', task.payload])

      }
    }
  },
  RETRY_INTERVAL,
)
