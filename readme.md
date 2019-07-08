# peermq

message queue with peer networking

# example

``` js
var hypercore = require('hypercore')
var peermq = require('peermq')
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
```

send a message to a node (that hasn't been started up yet):

```
$ node mq.js send -d /tmp/xyz789 --id xyz789 --to abc123 -m hi
```

start up node abc123 and receive the message from xyz789:

```
$ node mq.js listen -d /tmp/abc123 --id abc123
MESSAGE: hi
```

