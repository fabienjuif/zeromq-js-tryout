// This is the first version
// This load balancer send a request per worker and wait the worker response to set it "ready" again.

// a client wants something, I check my workers and ask them if they are ok
// TODO: track memory (clients doesn't need topics map for instance)
// TODO: track deconnexions
const zmq = require('zeromq')

const createQueue = () => {
  /** This map is to keep track of workers that are ready for a given topic */
  const queue = new Map()

  /** This map is to keep track of all topics a worker can work with */
  const topics = new Map()

  const add = (topic, identity) => {
    const topicStr = topic.toString()
    const identityStr = identity.toString()

    if (!queue.has(topicStr)) queue.set(topicStr, [])
    console.log('adding identity to queue...', topicStr, identityStr)
    queue.get(topicStr).push(identityStr)

    if (!topics.has(identityStr)) topics.set(identityStr, [])
    console.log('adding topic to identity...', topicStr, identityStr)
    topics.get(identityStr).push(topicStr)
  }

  const get = (topic) => {    // TODO: since a worker can have multiple topics, remove worker from all topics when it is get
    const topicStr = topic.toString()

    console.log('getting an identity for topic...', topicStr)

    if (!queue.has(topicStr)) throw new Error(`Topic unknown: ${topicStr}`)
    if (queue.get(topicStr).size === 0) throw new Error(`No worker ready for topic: ${topicStr}`)

    return queue.get(topicStr).pop() // TODO: should round robin
  }

  const forEach = (topic, callback) => {
    const topicStr = topic.toString()

    console.log('forEach identity on topic...', topicStr)
    if (!queue.has(topicStr)) throw new Error(`Topic unknown: ${topicStr}`)
    if (queue.get(topicStr).size === 0) throw new Error(`No worker ready for topic: ${topicStr}`)

    queue.get(topicStr).forEach(id => callback(id))
    queue.delete(topicStr)
  }

  const markReady = (identity) => {
    const identityStr = identity.toString()

    if (!topics.has(identityStr)) throw new Error(`${identityStr} has no known topics`)
    console.log(`${identityStr} is now ready [${topics.get(identityStr)}] !`)
    topics.get(identityStr).forEach(topic => {
      queue.get(topic).push(identityStr)
    })
  }

  return {
    add,
    get,
    markReady,
    forEach,
  }
}

const workers = createQueue()
const clients = createQueue()

const sock = zmq.socket('router')
sock.bindSync('tcp://127.0.0.1:3000')
console.log('Load balancer listening to tcp://127.0.0.1:3000')

sock.on('message', (identity, delimiter, type, topic, payload) => {
  if (!type || type.length === 0) throw new Error('Message type should be set!')

  // registering a worker
  if (type.toString() === '@@REGISTER') {
    console.log('REGISTER')
    workers.add(topic, identity)

    return
  }

  // response from a worker
  if (!topic || topic.length === 0) {
    console.log('RESPONSE')
    workers.markReady(identity)
    clients.forEach(type, id => sock.send([id, '', payload]))

    return
  }

  console.log('ASK WORKER', topic.toString())

  // client ask something to a worker
  // 1. registering client for the response
  // TODO: add something so the client can not ask for response
  clients.add(topic, identity)
  // 2. find and ask for a worker to process the request
  const worker = workers.get(type)
  sock.send([worker, '', payload])
})
