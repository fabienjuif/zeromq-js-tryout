const cluster = require('cluster');

if (cluster.isMaster) {
  const nb = +(process.argv[2] || 1)
  console.log('nb workers', nb)
  for (let i = 0; i < nb; i++) {
    cluster.fork();
  }
} else {
  const zmq = require('zeromq')

  const sock = zmq.socket('dealer')
  sock.identity = `client-${process.pid}`
  sock.connect('tcp://127.0.0.1:3000')

  let invoiceIndex = 0

  // asking for an invoice
  const getInvoice = (id) => {
    console.time(`getInvoice-${id}`)
    const event = { type: 'INVOICES>GET', payload: id, returnsType: `INVOICES>GET>${id}` }
    const serializedEvent = JSON.stringify(event)
    sock.send([event.type, event.returnsType, serializedEvent])
  }

  sock.on('message', (_, message) => {
    const { type, payload } = JSON.parse(message)

    if (type.startsWith('INVOICES>GET>')) {
      console.log('Invoice', payload.id)
      console.timeEnd(`getInvoice-${payload.id}`)
      return
    }

    throw new Error(`Receiving something I don\'t ask!!!! ${type}`)
  })

  setInterval(() => {
    getInvoice(`INV-${process.pid}-${invoiceIndex++}`)
  }, 500)
}
