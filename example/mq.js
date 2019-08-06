var peermq = require('../')
var path = require('path')
var { Transform } = require('readable-stream')

var minimist = require('minimist')
var argv = minimist(process.argv.slice(2), {
  alias: { m: 'message', d: 'datadir' }
})
require('mkdirp').sync(argv.datadir)

var mq = peermq({
  network: require('peer-channel'),
  storage: function (name) {
    return path.join(argv.datadir, name)
  }
})
if (argv._[0] === 'listen') {
  mq.listen(function (id) {
    console.log(id.toString('hex'))
  })
  mq.createReadStream('unread', { live: true }).pipe(new Transform({
    objectMode: true,
    transform: function ({ from, seq, data }, enc, next) {
      console.log(`MESSAGE ${from}@${seq} ${data.toString()}`)
      mq.archive({ from, seq }, next)
    }
  }))
} else if (argv._[0] === 'send') {
  mq.send({ to: argv.to, message: argv.message }, function (err) {
    if (err) console.error(err)
  })
}
