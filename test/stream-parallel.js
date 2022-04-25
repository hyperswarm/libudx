const test = require('brittle')
const { makePairs, pipeStreamPairs } = require('./helpers')

test('16 parallel streams on 1 socket', function (t) {
  const { streams, close } = makePairs(16, 'single')
  t.timeout(1000 * 2)
  t.teardown(close)
  t.plan(1)
  const messageSize = 1024 * 64
  const limit = 1024 * 512
  pipeStreamPairs(streams, messageSize, limit)
    .then(() => t.pass('all finished'))
    .catch(t.fail)
})

test('16 parallel streams on 16 sockets', function (t) {
  const { streams, close } = makePairs(16, 'multi')
  t.timeout(1000 * 2)
  t.teardown(close)
  t.plan(1)
  const messageSize = 1024 * 64
  const limit = 1024 * 512
  pipeStreamPairs(streams, messageSize, limit)
    .then(() => t.pass('all finished'))
    .catch(t.fail)
})
