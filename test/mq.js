var test = require('tape')
var peermq = require('../')
var ram = require('random-access-memory')
var { Transform } = require('readable-stream')
var network = require('./lib/network.js')()

test('send and receive', function (t) {
  t.plan(8)
  var mqA = peermq({
    network,
    storage: function (name) { return ram() }
  })
  var mqB = peermq({
    network,
    storage: function (name) { return ram() }
  })
  mqA.listen(function (err, server) {
    t.error(err)
    t.on('end', function () {
      server.close()
    })
  })
  var results = []
  var expected = null
  mqA.createReadStream('unread', { live: true }).pipe(new Transform({
    objectMode: true,
    transform: function ({ from, seq, data }, enc, next) {
      results.push(`${from}@${seq} ${data.toString()}`)
      mqA.archive({ from, seq }, next)
      if (results.length === 3) check()
    }
  }))
  function check () {
    t.deepEqual(results, expected, 'expected results')
    t.end()
  }
  mqA.getId(function (err, idA) {
    t.error(err)
    mqB.getId(function (err, idB) {
      t.error(err)
      expected = [
        `${idB.toString('hex')}@0 one`,
        `${idB.toString('hex')}@1 two`,
        `${idB.toString('hex')}@2 three`
      ]
      mqA.addPeer(idB, function (err) {
        t.error(err)
        send(idA)
      })
    })
  })
  function send (idA) {
    mqB.send({ to: idA, message: 'one' }, function (err) {
      t.error(err)
    })
    mqB.send({ to: idA, message: 'two' }, function (err) {
      t.error(err)
    })
    mqB.send({ to: idA, message: 'three' }, function (err) {
      t.error(err)
    })
    var c = mqB.connect(idA)
    t.on('end', function () { c.close() })
  }
})
