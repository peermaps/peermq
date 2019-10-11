var MultiBitfield = require('bitfield-db/multi')
var Tinybox = require('tinybox')
var hypercore = require('hypercore')
var Protocol = require('hypercore-protocol')
var hcrypto = require('hypercore-crypto')
var pump = require('pump')
var { Readable } = require('readable-stream')
var { EventEmitter } = require('events')
var { nextTick } = process
var path = require('path')
var raf = require('random-access-file')
var uniq = require('uniq')

// bitfield prefixes:
var B_READ = 'r!'
var B_ARCHIVE = 'a!'
var B_DELETED = 'd!'

// db prefixes:
var D_DKEY = 'd!'
var D_SIZE = 's!'

module.exports = MQ

function MQ (opts) {
  var self = this
  if (!(this instanceof MQ)) return new MQ(opts)
  EventEmitter.call(this)
  this._network = opts.network
  if (typeof opts.storage !== 'function' && typeof opts.storage !== 'string') {
    throw new Error('storage must be a function or a string path. received '
      + typeof opts.storage)
  }
  this._storage = opts.storage
  this._openStores = {}
  this._db = new Tinybox(this._openStore('db'))
  this._sendCores = {}
  this._sendCoreQueue = {}
  this._sendCoreLengths = {}
  this._listenCores = {}
  this._listenProtos = {}
  this._keys = null
  this._keyQueue = null
  this._id = null
  this._sendConnections = {}
  this._listening = false
  this._peers = null
  this._db.get('peers', function (err, node) {
    if (err) return self.emit('error', err)
    if (!node) {
      self._peers = []
    } else {
      try {
        self._peers = JSON.parse(node.value.toString('utf8'))
      } catch (err) {
        return self.emit('error', err)
      }
    }
    self.emit('_peers', self._peers)
  })
  this._multi = new MultiBitfield(this._openStore('bitfield'))
  this._bitfield = {
    read: {},
    archive: {},
    deleted: {}
  }
}

MQ.prototype = Object.create(EventEmitter.prototype)

MQ.prototype._openStore = function (name) {
  var self = this
  if (self._openStores[name]) return self._openStores[name]
  if (typeof self._storage === 'function') {
    var s = self._storage(name)
  } else if (typeof self._storage === 'string') {
    var s = path.join(self._storage, name)
  }
  if (typeof s === 'string') s = raf(s)
  self._openStores[name] = s
  return s
}

MQ.prototype._storeFn = function (prefix) {
  var self = this
  return function (name) {
    if (Buffer.isBuffer(prefix)) prefix = prefix.toString('hex')
    var key = path.join(prefix,name)
    if (self._openStores[key]) return self._openStores[key]
    var s = self._storage(key)
    if (typeof s === 'string') s = raf(s)
    self._openStores[key] = s
    return s
  }
}

MQ.prototype.getId = function (cb) {
  var self = this
  if (self._id) return nextTick(cb, null, self._id)
  self.getKeyPairs(function (err, keys) {
    if (err) return cb(err)
    self._id = keys.hypercore.publicKey
    cb(null, self._id)
  })
}

MQ.prototype.getKeyPairs = function (cb) {
  var self = this
  if (self._keys) return nextTick(cb, null, self._keys)
  if (self._keyQueue) {
    self._keyQueue.push(cb)
    return
  }
  self._keyQueue = []
  self._db.get('secret-key', function (err, node) {
    if (err) return finish(err)
    if (node) return finish(null, {
      hypercore: {
        secretKey: node.value.slice(0,64),
        publicKey: node.value.slice(32,64)
      },
      noise: {
        secretKey: node.value.slice(64,96),
        publicKey: node.value.slice(96,128)
      }
    })
    var hypercoreKeyPair = hcrypto.keyPair()
    var noiseKeyPair = Protocol.keyPair()
    var kpBuf = Buffer.concat([
      hypercoreKeyPair.secretKey, // 64 bytes
      noiseKeyPair.secretKey, // 32 bytes
      noiseKeyPair.publicKey // 32 bytes
    ])
    self._db.put('secret-key', kpBuf, function (err) {
      if (err) return finish(err)
      finish(null, {
        hypercore: hypercoreKeyPair,
        noise: noiseKeyPair
      })
    })
  })
  function finish (err, keys) {
    if (!err) self._keys = keys
    var q = self._keyQueue
    self._keyQueue = null
    if (err) {
      cb(err)
      for (var i = 0; i < q.length; i++) q[i](err)
    } else {
      cb(null, keys)
      for (var i = 0; i < q.length; i++) q[i](null, keys)
    }
  }
}

MQ.prototype._addPeer = function (pubKey) {
  // expects that this._peers is already loaded
  var bufPK = asBuffer(pubKey)
  var key = D_DKEY + hcrypto.discoveryKey(bufPK).toString('hex')
  this._peers.push(bufPK.toString('hex'))
  uniq(this._peers)
  this._peers.sort()
  this._db.put(D_DKEY + hcrypto.discoveryKey(bufPK).toString('hex'), bufPK)
  this._db.put('peers', Buffer.from(JSON.stringify(this._peers)))
}

MQ.prototype._removePeer = function (pubKey) {
  // expects that this._peers is already loaded
  var bufPK = asBuffer(pubKey)
  var key = D_DKEY + hcrypto.discoveryKey(bufPK).toString('hex')
  var ix = this._peers.indexOf(bufPK.toString('hex'))
  if (ix >= 0) this._peers.splice(ix,1)
  this._peers.sort()
  // del not yet implemented, overwrite with an empty buffer:
  this._db.put(D_DKEY + hcrypto.discoveryKey(bufPK).toString('hex'),
    Buffer.alloc(0))
  this._db.put('peers', Buffer.from(JSON.stringify(this._peers)))
}

MQ.prototype.getPeers = function (cb) {
  var self = this
  if (self._peers) {
    nextTick(cb, null, self._peers)
  } else {
    self.once('_peers', function () {
      cb(null, self._peers)
    })
  }
}

MQ.prototype.addPeer = function (pubKey, cb) {
  var self = this
  if (!isValidKey(pubKey)) return nextTick(cb, new Error('invalid pub key'))
  self.getPeers(function (err, peers) {
    if (err) return cb(err)
    self._addPeer(pubKey)
    self._db.flush(cb)
  })
}

MQ.prototype.removePeer = function (pubKey, cb) {
  var self = this
  if (!isValidKey(pubKey)) return nextTick(cb, new Error('invalid pub key'))
  self.getPeers(function (err, peers) {
    if (err) return cb(err)
    self._removePeer(pubKey)
    self._db.flush(cb)
  })
}

MQ.prototype.addPeers = function (pubKeys, cb) {
  var self = this
  for (var i = 0; i < pubKeys.length; i++) {
    if (!isValidKey(pubKeys[i])) {
      return nextTick(cb, new Error('invalid pub key'))
    }
  }
  self.getPeers(function (err, peers) {
    for (var i = 0; i < pubKeys.length; i++) {
      self._addPeer(pubKeys[i])
    }
    self._db.flush(cb)
  })
}

MQ.prototype.removePeers = function (pubKeys, cb) {
  var self = this
  for (var i = 0; i < pubKeys.length; i++) {
    if (!isValidKey(pubKeys[i])) {
      return nextTick(cb, new Error('invalid pub key'))
    }
  }
  self.getPeers(function (err, peers) {
    for (var i = 0; i < pubKeys.length; i++) {
      self._removePeer(pubKeys[i])
    }
    self._db.flush(cb)
  })
}

MQ.prototype._getPubKeyForDKey = function (dKey, cb) {
  if (!isValidKey(dKey)) return nextTick(cb, new Error('invalid discovery key'))
  var key = D_DKEY + asBuffer(dKey).toString('hex')
  this._db.get(key, function (err, node) {
    if (err) cb(err)
    else cb(null, node && node.value && node.value.length > 0
      ? node.value : null)
  })
}

MQ.prototype.listen = function (cb) {
  if (!cb) cb = noop
  var self = this
  if (self._listening) return nextTick(cb)
  self._listening = true
  self.getKeyPairs(function (err, keys) {
    if (err && cb) return cb(err)
    else if (err) return
    var server = self._network.createServer(function (cstream) {
      var proto = new Protocol(false, {
        live: true,
        sparse: true,
        extensions: [ 'sign-noise-key' ],
        keyPair: keys.noise
      })
      proto.once('discovery-key', function (discoveryKey) {
        onfeed(proto, keys, discoveryKey)
      })
      pump(proto, cstream, proto, function (err) {
        // ...
      })
    })
    server.listen(keys.hypercore.publicKey)
    cb(null, server)
  })
  function onfeed (proto, keys, discoveryKey) {
    self._getPubKeyForDKey(discoveryKey, function (err, pubKey) {
      if (err) return proto.destroy()
      if (!pubKey) return proto.destroy()
      var id = pubKey.toString('hex')
      self._listenProtos[id] = proto
      self._openListenCore(id, function (err, core) {
        core.on('remote-update', function () {
          if (self._sendCoreLengths[id] !== core.remoteLength) {
            self._sendCoreLengths[id] = core.remoteLength
            self.emit('_coreLength!'+id, core.remoteLength)
          }
        })
        self._sendCoreLengths[id] = core.remoteLength
        var ch = proto.open(pubKey)
        ch.extension('sign-noise-key', hcrypto.sign(
          keys.noise.publicKey, keys.hypercore.secretKey))
        var r = core.replicate(false, {
          sparse: true,
          live: true,
          keyPair: keys.noise,
          stream: proto
        })
      })
    })
  }
}

MQ.prototype.archive = function (msg, cb) {
  var readBF = this._bitfield.read[msg.from]
  var archiveBF = this._bitfield.archive[msg.from]
  readBF.add(msg.seq)
  archiveBF.add(msg.seq)
  this._multi.flush(function (err) {
    if (err) cb(err)
    else cb()
  })
}

MQ.prototype.clear = function (msg, cb) {
  var readBF = this._bitfield.read[msg.from]
    || this._multi.open(B_READ + msg.from + '!')
  var archiveBF = this._bitfield.archive[msg.from]
    || this._multi.open(B_ARCHIVE + msg.from + '!')
  var deleteBF = this._bitfield.deleted[msg.from]
    || this._multi.open(B_DELETED + msg.from + '!')
  readBF.delete(msg.seq)
  archiveBF.delete(msg.seq)
  deleteBF.add(msg.seq)
  var pending = 3, finished = false
  this._openListenCore(msg.from, function (err, core) {
    if (err) done(err)
    else core.clear(msg.seq, done)
  })
  this._multi.flush(done)
  done()
  function done (err) {
    if (finished) return
    if (err) {
      finished = true
      cb(err)
    }
    if (--pending !== 0) return
    cb()
  }
}

MQ.prototype.createReadStream = function (name, opts) {
  var self = this
  var cores = []
  var coreKeys = []
  var offsets = {}
  var queue = []
  self.on('_core', oncore)
  self.getPeers(function (err, peers) {
    peers.forEach(function (peer) {
      self._openListenCore(peer)
    })
  })
  Object.values(self._listenCores).forEach(oncore)

  var reading = false
  var stream = new Readable({
    objectMode: true,
    read: function (n) {
      if (queue.length > 0) {
        return this.push(queue.shift())
      }
      // todo: check if all open cores have closed in non-live mode
      reading = true
      for (var i = 0; i < cores.length; i++) {
        var key = coreKeys[i]
        if (self._sendCoreLengths.hasOwnProperty(key)) {
          readNext(cores[i], coreKeys[i], push)
        } else { // todo: speed this up by having a single resident listener
          self.once('_coreLength!'+key, readNext.bind(
            null, cores[i], coreKeys[i], push
          ))
        }
      }
    },
    destroy: function () {
      cleanup()
    }
  })
  if (name !== 'unread') {
    nextTick(function () {
      stream.emit('error', new Error('only "unread" is supported right now'))
    })
  }
  return stream

  function push (err, msg) {
    if (err) {
      stream.emit('error', err)
    } else if (reading) {
      reading = false
      stream.push(msg)
    } else {
      queue.push(msg)
    }
  }
  function cleanup () {
    self.removeListener('_core', oncore)
  }
  function oncore (core) {
    cores.push(core)
    var key = core.key.toString('hex')
    coreKeys.push(key)
    offsets[key] = -1
    readNext(core, key, push)
  }
  function readNext (core, key, cb) {
    var len = self._sendCoreLengths[key]
    self._bitfield.read[key].next0(offsets[key], function (err, x) {
      if (err) return cb(err)
      else if (x >= len) {
        // todo: only set up a listener the first time in non-live mode
        return self.once('_coreLength!'+key,
          readNext.bind(null, core, key, push))
      }
      core.get(x, function (err, buf) {
        if (err) return cb(err)
        offsets[key] = x
        cb(null, { seq: x, from: key, data: buf })
      })
    })
  }
}

MQ.prototype.connect = function (to, cb) {
  if (!cb) cb = noop
  var self = this
  var keys = null, core = null
  var pending = 3
  self._openSendCore(to, function (err, core_) {
    if (err) return cb(err)
    core = core_
    if (--pending === 0) ready()
  })
  self.getKeyPairs(function (err, keys_) {
    if (err) return cb(err)
    keys = keys_
    if (--pending === 0) ready()
  })
  if (--pending === 0) ready()
  var connection = new EventEmitter
  var closed = false
  connection.close = function () {
    closed = true
    connection.emit('_close')
  }
  return connection

  function ready () {
    var toBuf = asBuffer(to)
    var stream = self._network.connect(toBuf, {
      id: keys.hypercore.publicKey
    })
    var proto = new Protocol(true, {
      ack: true,
      live: true,
      sparse: true,
      keyPair: keys.noise,
      extensions: [ 'sign-noise-key' ]
    })
    connection.once('_close', function () {
      proto.end()
      stream.close()
    })
    var ch = proto.open(core.key, { onextension })
    pump(stream, proto, stream, function (err) {
      //if (err) connection.emit('error', err)
      connection.emit('close')
    })
    function onextension (i, message) {
      if (i !== 0) return // sign-noise-key
      var ok = hcrypto.verify(proto.remotePublicKey, message, toBuf)
      if (!ok) return proto.destroy()
      var r = core.replicate(true, {
        ack: true,
        live: true,
        sparse: true,
        stream: proto
      })
      connection.once('_close', function () {
        r.end()
      })
      r.on('ack', function (ack) {
        connection.emit('ack', ack)
      })
    }
  }
}

MQ.prototype.send = function (m, cb) {
  this._openSendCore(m.to, function (err, core) {
    core.append(m.message, cb)
  })
}

MQ.prototype._openSendCore = function (to, cb) {
  var self = this
  var key = to.toString('hex')
  if (!isValidKey(to)) return nextTick(cb, new Error('invalid key: ' + key))
  if (self._sendCores[key]) {
    return nextTick(cb, self._sendCores[key])
  }
  if (self._sendCoreQueue[key]) {
    self._sendCoreQueue[key].push(cb)
    return
  }
  self._sendCoreQueue[key] = []
  self.getKeyPairs(function (err, keys) {
    var q = self._sendCoreQueue[key]
    self._sendCoreQueue[key] = null
    if (err) {
      cb(err)
      for (var i = 0; i < q.length; i++) q[i](err)
      return
    }
    var core = hypercore(self._storeFn(to), keys.hypercore.publicKey, {
      secretKey: keys.hypercore.secretKey,
      extensions: [ 'sign-noise-key' ]
    })
    self._sendCores[key] = core
    cb(null, core)
    for (var i = 0; i < q.length; i++) q[i](null, core)
  })
}

MQ.prototype._openListenCore = function (id, cb) {
  var self = this
  if (!cb) cb = noop
  var core = self._listenCores[id]
  if (core) return nextTick(cb, null, core)
  var pubKey = Buffer.from(id,'hex')
  core = hypercore(self._storeFn(id), pubKey, {
    sparse: true,
    extensions: [ 'sign-noise-id' ]
  })
  self._listenCores[id] = core
  core.ready(function () {
    self._bitfield.read[id] = self._multi.open(B_READ + id + '!')
    self._bitfield.archive[id] = self._multi.open(B_ARCHIVE + id + '!')
    self._bitfield.deleted[id] = self._multi.open(B_DELETED + id + '!')
    self.emit('_core', core)
    if (core.length > 0) {
      self._sendCoreLengths[id] = core.length
      self.emit('_coreLength!'+id, core.length)
    }
    cb(null, core)
  })
}

function asBuffer (x) {
  if (typeof x === 'string') return Buffer.from(x,'hex')
  else return x
}

function isValidKey (x) {
  if (Buffer.isBuffer(x) && x.length === 32) return true
  if (typeof x === 'string' && /^[0-9A-Fa-f]{64}$/.test(x)) return true
  return false
}

function noop () {}
