const zmq = require('zeromq')

interface ZMQSocket {
  send: (message: Array<string|Buffer>) => {}
}

const TASK_MAX_RETRY = (+process.env.TASK_MAX_RETRY || 3)
const TASK_TIMEOUT = +(process.env.TASK_TIMEOUT || 1000 * 60 * 1) /* 1H */

interface Client {
  name: string,
  isWorker: boolean,
  topics: Set<string>,
}

interface Topic {
  name: string,
  workers: Set<string>,
  workersIterator: IterableIterator<string>,
  clients: Set<string>,
}

interface Task {
  workerTopic: string,
  workerName?: string,
  responseTopic: string,
  retry: number,
  payload: string | Buffer,
  date: number,
}

const clients = new Map<string, Client>()
const topics = new Map<string, Topic>()
const tasks = new Set<Task>()

let closing = false

// TODO: should be accessible from a dedicated socket and only when the client ask for it
//       it will speed up the overall process since it wouldn't have to use stdout for each task
const printDebug = () => {
  const [workers, localClients] = Array.from(clients.values()).reduce(
    ([currWorkers, currClients], client) => {
      if (client.isWorker) return [currWorkers.concat(client), currClients]
      return [currWorkers, currClients.concat(client)]
    },
    [[], []],
  )

  console.log(`[${workers.length} workers; ${localClients.length} clients; ${topics.size} topics; ${tasks.size} tasks]`)
}

const addTopic = (name: string, client: string, isWorker: boolean) => {
  if (!topics.has(name)) {
    const workers = new Set()

    topics.set(
      name,
      {
        name,
        workers,
        clients: new Set(),
        workersIterator: workers.values(),
      },
    )
    }

  const topic = topics.get(name)
  if (isWorker) topic.workers.add(client)
  else topic.clients.add(client)

  return topic
}

const addClient = (name: string, topicName: string, isWorker: boolean) => {
  if (!clients.has(name)) clients.set(
    name,
    {
      name,
      topics: new Set(),
      isWorker
    },
  )

  const client = clients.get(name)
  client.topics.add(topicName)

  const topic = addTopic(topicName, name, isWorker)

  return {
    client,
    topic,
  }
}

const addTask = (workerTopic: string, responseTopic: string, payload: string | Buffer) => {
  const task = {
    payload,
    workerTopic,
    responseTopic,
    retry: -1,
    date: Date.now(),
  }
  tasks.add(task)

  return task
}

const removeTask = (task: Task) => {
  tasks.delete(task)

  printDebug()
}

/**
 * Remove a client, it can be necessary when the client is disconnected for some reason.
 * If the related topics are empty after the client removal, then these related topic are also removed.
 *
 * @param name Name of the client to remove.
 */
const removeClient = (name: string) => {
  const client = clients.get(name)
  if (!client) return

  client.topics.forEach((topicName) => {
    const topic = topics.get(topicName)
    if (!topic) return

    // removing current client from topic
    topic.workers.delete(name)
    topic.clients.delete(name)

    // if the topic is empty, we remove it
    if (topic.workers.size === 0 && topic.clients.size === 0) topics.delete(topicName)
  })

  clients.delete(name)
}

/**
 * Send a message into the socket.
 * If the message can't be sent, then the client is removed (considered disconnected).
 *
 * @param message The message to send, the first index is the client name.
 */
const send = (sock: ZMQSocket, message: [string, string, string | Buffer]) => {
  let sent = false
  try {
    sock.send(message)
    sent = true
  } catch (ex) {
    if (ex.message !== 'Host unreachable') throw ex
    removeClient(message[0])
  }
  return sent
}

/**
 * Send the response to all clients that are waiting for the given type.
 * The clients are then removed from the topic.
 * The associated tasks are removed.
 *
 * @param sock Socket to use (from ZMQ)
 * @param type the type the client listen the response from
 * @param payload the payload to send to all clients that are waiting for the topic (type)
 */
const sendResponse = (sock: ZMQSocket, type: string, payload: string | Buffer) => {
  const topic = topics.get(type)
  if (!topic) return

  topic.clients.forEach((name) => {
    send(sock, [name, '', payload])

    const client = clients.get(name)
    if (!client) return
    client.topics.delete(type)

    if (client.topics.size === 0) {
      removeClient(client.name)
    }
  })
  topic.clients.clear()

  if (topic.workers.size === 0) topics.delete(topic.name)

  const tasksToRemoved: Task[] = []
  tasks.forEach((task) => {
    if (task.responseTopic === type) tasksToRemoved.push(task)
  })
  tasksToRemoved.forEach(removeTask)
}

const getNextWorker = (topicName: string): Client => {
  const topic = topics.get(topicName)
  if (!topic || topic.workers.size === 0) return undefined

  // next iterator, loop if we are at the end
  let nextWorkerName = topic.workersIterator.next().value
  if (!nextWorkerName) {
    topic.workersIterator = topic.workers.values()
    return getNextWorker(topicName)
  }

  return clients.get(nextWorkerName)
}

const sendTask = (sock: ZMQSocket, task: Task): boolean => {
  task.date = Date.now()
  task.retry += 1
  if (task.retry >= TASK_MAX_RETRY) {
    removeTask(task)
    return false
  }

  // select a worker
  const worker = getNextWorker(task.workerTopic)
  if (!worker) return false
  task.workerName = worker.name

  // send the task to the worker
  // if it doesn't works (worker is dead for instance), then we retry
  // the recursion is done if there is no worker anymore or if the retry is to damn high
  const sent = send(sock, [worker.name, '', task.payload])
  if (!sent) return sendTask(sock, task)

  return true
}

const sock = zmq.socket('router')
sock.bindSync('tcp://127.0.0.1:3000')
sock.setsockopt(zmq.ZMQ_ROUTER_MANDATORY, 1)
console.log('Load balancer listening to tcp://127.0.0.1:3000')
console.log(`\t task timeout is ${TASK_TIMEOUT}ms`)

sock.on('message', (name: Buffer, type: Buffer, topic: Buffer, payload: Buffer) => {
  if (!type || type.length === 0) throw new Error('Message type should be set!')

  const typeStr = type.toString()
  const topicStr = topic.toString()
  const nameStr = name.toString()

  // if the server is closing, it will only send response back
  if (closing
    && (
      typeStr === '@@REGISTER'
      || topicStr
    )
  ) return

  // registering a worker
  if (typeStr === '@@REGISTER') {
    addClient(nameStr, topicStr, true)
  } else if (!topicStr) {
    // response from a worker because a worker won't register to a response
    // TODO: find an other way, because a client may want to trigger an async action without waiting for acknowledgment
    sendResponse(sock, typeStr, payload)
  } else {
    // a client ask for a worker
    // - register the client: it's waiting for a response
    addClient(nameStr, topicStr, false)
    // - create a task
    const task = addTask(typeStr, topicStr, payload)
    // - send it
    sendTask(sock, task)
  }

  printDebug()
})

// handle orphans tasks and timeout
const RETRY_TIMEOUT = 100
let retrying = false
const retryInterval = setInterval(() => {
  // We won't have a race condition
  // so we only allow one retry interval at the time
  if (retrying) return
  retrying = true

  // We ask for "now" only once
  // - it will be used to handle timeout
  // - I don't know if we have performance gain
  const now = Date.now()

  tasks.forEach((task) => {
    // task is not even started
    if (task.workerName === undefined || task.retry === -1) return

    // orphans tasks
    // - orphans tasks are tasks without a worker (worker was dead during processing)
    if (!clients.get(task.workerName)) sendTask(sock, task)

    // timeout tasks
    // - timeout tasks are tasks that are sent to a worker that is still alive
    // - but the worker didn't send the response before a TASK_TIMEOUT
    if (task.date + TASK_TIMEOUT < now) sendTask(sock, task)
  })

  retrying = false
}, RETRY_TIMEOUT)

// is it closed ?
const wait = (timeout: number) => new Promise(resolve => setTimeout(resolve, timeout))
const MAX_WAIT_RETRY = 10
let wait_retry = 0
const waitClosing = async (): Promise<boolean> => {
  wait_retry += 1
  if (wait_retry > MAX_WAIT_RETRY) return false

  if (tasks.size > 0 && clients.size > 0) {
    await wait(200)
    return waitClosing()
  }

  return true
}

const interrupt = (sigName: string) => async () => {
  console.error('caught interrupt signal', sigName)

  console.error('waiting for lasts tasks...')
  closing = true
  if (!await waitClosing()) console.error('force close...')
  if (retryInterval !== undefined) clearInterval(retryInterval)

  console.error('closing the socket')
  sock.close()
}
['SIGUSR1', 'SIGINT', 'SIGTERM', 'SIGPIPE', 'SIGHUP', 'SIGBREAK'].forEach((sigName) => {
  process.on(sigName as any, interrupt(sigName)) // TODO: type ???
})
