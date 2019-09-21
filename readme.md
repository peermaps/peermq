# peermq

peer to peer message queue

# example

With this program, we will set up a peer to listen and one to connect and send
messages. These roles aren't static: either peer could listen and either could
send messages.

``` js
var peermq = require('peermq')
var path = require('path')
var { Transform } = require('readable-stream')

var minimist = require('minimist')
var argv = minimist(process.argv.slice(2), {
  alias: { m: 'message', d: 'datadir' }
})
require('mkdirp').sync(argv.datadir)

var mq = peermq({
  network: require('peermq/network'),
  storage: function (name) {
    return path.join(argv.datadir, name)
  }
})

if (argv._[0] === 'id') {
  mq.getId(function (err, id) {
    if (err) console.error(err)
    else console.log(id.toString('hex'))
  })
} else if (argv._[0] === 'add-peer') {
  mq.addPeer(argv._[1], function (err) {
    if (err) console.error(err)
  })
} else if (argv._[0] === 'listen') {
  mq.listen()
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
} else if (argv._[0] === 'connect') {
  mq.connect(argv._[1])
}
```

create two new databases and print their IDs:

```
$ node mq.js -d /tmp/a id # node A
41f541aa50bbd9948e794b3a5d55b4267f731a94c1ee4053470df9f514762ac4
$ node mq.js -d /tmp/b id # node B
19eee824db6456a97f7e90a75dfde8e6ad3a4b34eee65373823a50276f33872e
```

allow incoming connections from node B on node A:

```
$ node mq.js -d /tmp/a add-peer \
  19eee824db6456a97f7e90a75dfde8e6ad3a4b34eee65373823a50276f33872e
```

create some messages on node B to send to node A:

```
$ node mq.js -d /tmp/b send \
  --to 41f541aa50bbd9948e794b3a5d55b4267f731a94c1ee4053470df9f514762ac4 \
  --message wow
$ node mq.js -d /tmp/b send \
  --to 41f541aa50bbd9948e794b3a5d55b4267f731a94c1ee4053470df9f514762ac4 \
  --message cool
$ node mq.js -d /tmp/b send \
  --to 41f541aa50bbd9948e794b3a5d55b4267f731a94c1ee4053470df9f514762ac4 \
  --message hiiiiiiii
```

connect to node A from node B:

```
$ node mq.js -d /tmp/b connect \
  41f541aa50bbd9948e794b3a5d55b4267f731a94c1ee4053470df9f514762ac4
```

and in another terminal, listen on node A. Once a connection is made, you should
see messages arrive:

```
$ node mq.js -d /tmp/a listen
MESSAGE 19eee824db6456a97f7e90a75dfde8e6ad3a4b34eee65373823a50276f33872e@0 wow
MESSAGE 19eee824db6456a97f7e90a75dfde8e6ad3a4b34eee65373823a50276f33872e@1 cool
MESSAGE 19eee824db6456a97f7e90a75dfde8e6ad3a4b34eee65373823a50276f33872e@2 hiiiiiiii
```

on node B, we can disconnect (ctrl+c), queue more messages to send, then
reconnect:

```
$ node mq.js -d /tmp/b connect \
  41f541aa50bbd9948e794b3a5d55b4267f731a94c1ee4053470df9f514762ac4
^C
$ node mq.js -d /tmp/b send \
  --to 41f541aa50bbd9948e794b3a5d55b4267f731a94c1ee4053470df9f514762ac4 \
  --message beep
$ node mq.js -d /tmp/b send \
  --to 41f541aa50bbd9948e794b3a5d55b4267f731a94c1ee4053470df9f514762ac4 \
  --message boop
$ node mq.js -d /tmp/b connect \
  41f541aa50bbd9948e794b3a5d55b4267f731a94c1ee4053470df9f514762ac4
```

back on node A, we now see the messages arrive:

```
MESSAGE 19eee824db6456a97f7e90a75dfde8e6ad3a4b34eee65373823a50276f33872e@3 beep
MESSAGE 19eee824db6456a97f7e90a75dfde8e6ad3a4b34eee65373823a50276f33872e@4 boop
```

Because the listener marks messages as "read" once it receives them (with
`mq.archive()`), we will see each message only once. This is a useful property
for building job queues or other batch processing tools.

The sender could also listen for `'ack'` events from the listener to know when
it's safe to delete messages that have been received. `'ack'` messages are sent
over an authenticated channel where the network session keys (noise) are signed
by the hypercore key.

# api

``` js
var peermq = require('peermq')
```

## var mq = peermq(opts)

Create a peermq instance `mq` from:

* `opts.network` - a network adapter (use `require('peermq/network')`)
* `opts.storage(name)` - function that returns a random-access adaptor or a
  string path for a `name` string

## mq.getId(cb)

Get the id of this node as a Buffer in `cb(err, id)`.

## mq.addPeer(publicKey, cb)

Authorize a peer to connect by its `publicKey` (Buffer or hex string).

## mq.removePeer(publicKey, cb)

Remove a peer by its `publicKey` (Buffer or hex string).

## mq.getPeers(cb)

Get the list of `peers`, an array of hex string public keys as `cb(err, peers)`.

## mq.listen(cb)

Listen for incoming connections. Get access to the network adaptor's `server`
instance (from `network.createServer()`) as `cb(err, server)`.

## var connection = mq.connect(to, cb)

Connect to a publicKey `to`. `connection` emits `'ack'` events for the
underlying replication stream.

## mq.send({ message, to }, cb)

Write a `message` addressed to a remote public key `to`. These messages will be
synchronized when a connection is available.

## var stream = mq.createReadStream(type)

Create a new objectMode `stream` to read records for a stream `type`.

This stream pulls from all peer streams in live mode. As you pull messages from
this stream, documents are downloaded from the remote database in sparse mode.

Right now `type` must be `'unread'`. Soon other categories and user-created
categories will be available.

## mq.archive({ seq, from }, cb)

Set the document in the public key `from` at the sequence number `seq` to
"read" and copy this value into the "archive" set.

## mq.clear({ seq, from }, cb)

Remove the document in the public key `from` at the sequence number `seq`.

# license

[license zero parity](https://licensezero.com/licenses/parity)
and [apache 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt)
(contributions)
