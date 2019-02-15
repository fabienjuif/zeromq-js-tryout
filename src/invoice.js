// you want some invoice info?
const zmq = require('zeromq')

const TIMEOUT = +(process.argv[2] || 1000)

const sock = zmq.socket('dealer')
sock.identity = `worker-invoice-${process.pid}`
sock.connect('tcp://127.0.0.1:3000')
const registerEvent = { type: '@@REGISTER', payload: 'INVOICES>GET' }
sock.send([registerEvent.type, registerEvent.payload])

const sendResponse = (returnsType, payload) => {
  setTimeout(() => {
    console.log(`[x] ${payload}`)
    const event = { type: returnsType, payload: { id: payload, prices: { total: 2828.23 } } }
    const serializedEvent = JSON.stringify(event)
    sock.send([event.type, '', serializedEvent])
  }, TIMEOUT)
}

sock.on('message', (_, message) => {
  const { type, payload, returnsType } = JSON.parse(message)

  if (type !== 'INVOICES>GET') throw new Error(`I can't handle ${type} !!`)

  console.log(`[ ] ${payload}`)

  // this is a timeout simulation
  if (payload.endsWith('-2')) return

  sendResponse(returnsType, payload)
})
