var test = require('tape')
var peermq = require('../')
var ram = require('random-access-memory')
var { Transform } = require('readable-stream')

test('remove', function (t) {
  t.plan(9)
  var mqA = peermq({
    network: require('../network.js'),
    storage: function (name) { return ram() }
  })
  var mqB = peermq({
    network: require('../network.js'),
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
      t.fail('received a message from an unauthorized peer')
    }
  }))
  mqA.getId(function (err, idA) {
    t.error(err)
    mqB.getId(function (err, idB) {
      t.error(err)
      mqA.addPeer(idB, function (err) {
        t.error(err)
        mqA.removePeer(idB, function (err) {
          t.error(err)
          send(idA)
        })
      })
    })
  })
  function send (idA) {
    // should not receive any of these messages:
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
    c.on('close', function () {
      t.pass('connection closed')
    })
    t.on('end', function () { c.close() })
  }
})
