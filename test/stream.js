const test = require('brittle')
const proxy = require('./helpers/proxy')
const UDX = require('../')
const { makeTwoStreams } = require('./helpers')

test('tiny echo stream', async function (t) {
  t.plan(8)

  const [a, b] = makeTwoStreams(t)

  a.on('data', function (data) {
    t.alike(data, Buffer.from('echo: hello world'), 'a received echoed data')
  })

  a.on('end', function () {
    t.pass('a ended')
  })

  a.on('finish', function () {
    t.pass('a finished')
  })

  a.on('close', function () {
    t.pass('a closed')
  })

  b.on('data', function (data) {
    t.alike(data, Buffer.from('hello world'), 'b received data')
    b.write(Buffer.concat([Buffer.from('echo: '), data]))
  })

  b.on('end', function () {
    t.pass('b ended')
    b.end()
  })

  b.on('finish', function () {
    t.pass('b finished')
  })

  b.on('close', function () {
    t.pass('b closed')
  })

  a.write(Buffer.from('hello world'))
  a.end()
})

test('end immediately', async function (t) {
  t.plan(6)

  const [a, b] = makeTwoStreams(t)

  a.on('data', function () {
    t.fail('should not send data')
  })

  a.on('end', function () {
    t.pass('a ended')
  })

  a.on('finish', function () {
    t.pass('a finished')
  })

  a.on('close', function () {
    t.pass('a closed')
  })

  b.on('data', function (data) {
    t.fail('should not send data')
  })

  b.on('end', function () {
    t.pass('b ended')
    b.end()
  })

  b.on('finish', function () {
    t.pass('b finished')
  })

  b.on('close', function () {
    t.pass('b closed')
  })

  a.end()
})

test('only one side writes', async function (t) {
  t.plan(7)

  const [a, b] = makeTwoStreams(t)

  a.on('data', function () {
    t.fail('should not send data')
  })

  a.on('end', function () {
    t.pass('a ended')
  })

  a.on('finish', function () {
    t.pass('a finished')
  })

  a.on('close', function () {
    t.pass('a closed')
  })

  b.on('data', function (data) {
    t.alike(data, Buffer.from('hello world'), 'b received data')
  })

  b.on('end', function () {
    t.pass('b ended')
    b.end()
  })

  b.on('finish', function () {
    t.pass('b finished')
  })

  b.on('close', function () {
    t.pass('b closed')
  })

  a.write(Buffer.from('hello world'))
  a.end()
})

test('unordered messages', async function (t) {
  t.plan(2)

  const [a, b] = makeTwoStreams(t)
  const expected = []

  b.on('message', function (buf) {
    b.send(Buffer.from('echo: ' + buf.toString()))
  })

  a.on('error', function () {
    t.pass('a destroyed')
  })

  a.on('message', function (buf) {
    expected.push(buf.toString())

    if (expected.length === 3) {
      t.alike(expected.sort(), [
        'echo: a',
        'echo: bc',
        'echo: d'
      ])

      // TODO: .end() here triggers a bug, investigate
      b.destroy()
    }
  })

  a.send(Buffer.from('a'))
  a.send(Buffer.from('bc'))
  a.send(Buffer.from('d'))
})

test('several streams on same socket', async function (t) {
  const u = new UDX()

  const socket = u.createSocket()
  socket.bind(0)

  for (let i = 0; i < 10; i++) {
    const stream = u.createStream(i)
    stream.connect(socket, i, socket.address().port)

    t.teardown(() => stream.destroy())
  }

  t.teardown(() => socket.close())
  t.pass('halts')
})

test('destroy unconnected stream', async function (t) {
  t.plan(1)

  const u = new UDX()

  const stream = u.createStream(1)

  stream.on('close', function () {
    t.pass('closed')
  })

  stream.destroy()
})

test('preconnect flow', async function (t) {
  t.plan(8)

  const u = new UDX()

  const socket = u.createSocket()
  socket.bind(0)

  let once = true

  const a = u.createStream(1, {
    firewall (sock, port, host) {
      t.ok(once)
      t.is(sock, socket)
      t.is(port, socket.address().port)
      t.is(host, '127.0.0.1')
      once = false

      return false
    }
  })

  a.on('data', function (data) {
    t.is(data.toString(), 'hello', 'can receive data preconnect')

    a.connect(socket, 2, socket.address().port)
    a.end()
  })

  const b = u.createStream(2)

  b.connect(socket, 1, socket.address().port)
  b.write(Buffer.from('hello'))
  b.end()

  let closed = 0

  b.resume()
  b.on('close', function () {
    t.pass('b closed')
    if (++closed === 2) socket.close()
  })

  a.on('close', function () {
    t.pass('a closed')
    if (++closed === 2) socket.close()
  })

  socket.on('close', function () {
    t.pass('socket closed')
  })
})

test('destroy streams and close socket in callback', async function (t) {
  t.plan(1)

  const u = new UDX()

  const socket = u.createSocket()
  socket.bind(0)

  const a = u.createStream(1)
  const b = u.createStream(2)

  a.connect(socket, 2, socket.address().port)
  b.connect(socket, 1, socket.address().port)

  a.on('data', async function (data) {
    a.destroy()
    b.destroy()

    await socket.close()

    t.pass('closed')
  })

  b.write(Buffer.from('hello'))
})

test('write empty buffer', async function (t) {
  t.plan(3)

  const [a, b] = makeTwoStreams(t)

  a
    .on('data', function (data) {
      t.alike(data, Buffer.alloc(0))
    })
    .on('close', function () {
      t.pass('a closed')
    })
    .end()

  b
    .on('close', function () {
      t.pass('b closed')
    })
    .end(Buffer.alloc(0))
})

test('out of order packets', async function (t) {
  t.plan(3)

  const u = new UDX()

  const a = u.createSocket()
  const b = u.createSocket()

  a.bind(0)
  b.bind(0)

  const count = 1000
  const expected = []

  const p = await proxy({ from: a, to: b }, async function (pkt) {
    // Add a random delay to every packet
    await new Promise((resolve) =>
      setTimeout(resolve, Math.random() * 1000 | 0)
    )

    return false
  })

  const aStream = u.createStream(1)
  const bStream = u.createStream(2)

  aStream.connect(a, 2, p.address().port)
  bStream.connect(b, 1, p.address().port)

  for (let i = 0; i < count; i++) {
    aStream.write(Buffer.from(i.toString()))
  }

  bStream.on('data', function (data) {
    expected.push(data.toString())

    if (expected.length === count) {
      t.alike(expected, Array(count).fill(0).map((_, i) => i.toString()), 'data in order')

      p.close()
      aStream.destroy()
      bStream.destroy()
    }
  })

  aStream.on('close', function () {
    t.pass('a stream closed')
    b.close()
  })

  bStream.on('close', function () {
    t.pass('b stream closed')
    a.close()
  })
})

test('out of order reads but can destroy (memleak test)', async function (t) {
  t.plan(3)

  const u = new UDX()

  const a = u.createSocket()
  const b = u.createSocket()

  a.bind(0)
  b.bind(0)

  let processed = 0

  const p = await proxy({ from: a, to: b }, function (pkt) {
    if (pkt.data.toString() === 'a' && processed > 0) {
      // destroy with out or order packets delivered
      t.pass('close while streams have out of order state')
      p.close()
      aStream.destroy()
      bStream.destroy()
      return true
    }

    return processed++ === 0 // drop first packet
  })

  const aStream = u.createStream(1)
  const bStream = u.createStream(2)

  aStream.connect(a, 2, p.address().port)
  bStream.connect(b, 1, p.address().port)

  aStream.write(Buffer.from('a'))
  aStream.write(Buffer.from('b'))

  aStream.on('close', function () {
    t.pass('a stream closed')
    b.close()
  })

  bStream.on('close', function () {
    t.pass('b stream closed')
    a.close()
  })
})

test('close socket on stream close', async function (t) {
  t.plan(2)

  const u = new UDX()

  const aSocket = u.createSocket()
  aSocket.bind(0)

  const bSocket = u.createSocket()
  bSocket.bind(0)

  const a = u.createStream(1)
  const b = u.createStream(2)

  a.connect(aSocket, 2, bSocket.address().port)
  b.connect(bSocket, 1, aSocket.address().port)

  a
    .on('close', async function () {
      await aSocket.close()
      t.pass('a closed')
    })
    .end()

  b
    .on('end', function () {
      b.end()
    })
    .on('close', async function () {
      await bSocket.close()
      t.pass('b closed')
    })
})

test('write string', async function (t) {
  t.plan(3)

  const [a, b] = makeTwoStreams(t)

  a
    .on('data', function (data) {
      t.alike(data, Buffer.from('hello world'))
    })
    .on('close', function () {
      t.pass('a closed')
    })
    .end()

  b
    .on('close', function () {
      t.pass('b closed')
    })
    .end('hello world')
})

test('write before connect', async function (t) {
  t.plan(1)

  const u = new UDX()

  const socket = u.createSocket()
  socket.bind(0)

  const a = u.createStream(1)
  const b = u.createStream(2)

  a.on('data', function (data) {
    t.alike(data, Buffer.from('hello world'))

    a.destroy()
    b.destroy()

    socket.close()
  })

  b.write(Buffer.from('hello world'))

  setImmediate(() => {
    a.connect(socket, 2, socket.address().port)
    b.connect(socket, 1, socket.address().port)
  })
})

test('end before connect', async function (t) {
  t.plan(1)

  const u = new UDX()

  const socket = u.createSocket()
  socket.bind(0)

  const a = u.createStream(1)
  const b = u.createStream(2)

  a.on('end', function () {
    a.destroy()
    b.destroy()

    socket.close()
  })

  b.end()

  setImmediate(() => {
    a.connect(socket, 2, socket.address().port)
    b.connect(socket, 1, socket.address().port)
  })
})
