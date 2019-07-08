var hypercore = require('hypercore')
var peermq = require('../')
var path = require('path')

var minimist = require('minimist')
var argv = minimist(process.argv.slice(2), {
  alias: { m: 'message', d: 'datadir' }
})

var mq = peermq({
  network: require('peer-channel'),
  storage: function (name) {
    return path.join(argv.datadir, name)
  }
})
if (argv._[0] === 'listen') {
  mq.listen(argv.id)
  mq.on('message', function (msg) {
    console.log('MESSAGE', msg.toString())
  })
} else if (argv._[0] === 'send') {
  mq.send({ to: argv.to, message: argv.message }, function (err) {
    if (err) console.error(err)
    mq.close()
  })
}
