var MultiBitfield = require('bitfield-db/multi')
var Tinybox = require('tinybox')
var hypercore = require('hypercore')
var protocol = require('hypercore-protocol')
var { keyPair, discoveryKey } = require('hypercore-crypto')
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
  this._storage = opts.storage
  this._openStores = {}
  this._db = new Tinybox(this._openStore('db'))
  this._sendCores = {}
  this._sendCoreQueue = {}
  this._sendCoreLengths = {}
  this._listenCores = {}
  this._listenProtos = {}
  this._secretKey = null
  this._secretKeyQueue = null
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
  var s = self._storage(name)
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

MQ.prototype.getId =
MQ.prototype.getPublicKey = function (cb) {
  this.getSecretKey(function (err, buf) {
    if (err) cb(err)
    else cb(null, buf.slice(32))
  })
}

MQ.prototype.getSecretKey = function (cb) {
  var self = this
  if (self._secretKey) return nextTick(cb, null, self._secretKey)
  if (self._secretKeyQueue) {
    self._secretKeyQueue.push(cb)
    return
  }
  self._secretKeyQueue = []
  self._db.get('secret-key', function (err, node) {
    if (err) return finish(err)
    if (node) return finish(null, node.value)
    var kp = keyPair()
    self._db.put('secret-key', kp.secretKey, function (err) {
      if (err) return finish(err)
      self._secretKey = kp.secretKey
      finish(null, kp.secretKey)
    })
  })
  function finish (err, buf) {
    var q = self._secretKeyQueue
    self._secretKeyQueue = null
    if (err) {
      cb(err)
      for (var i = 0; i < q.length; i++) q[i](err)
    } else {
      cb(null, buf)
      for (var i = 0; i < q.length; i++) q[i](null, buf)
    }
  }
}

MQ.prototype._addPeer = function (pubKey) {
  // expects that this._peers is already loaded
  var bufPK = asBuffer(pubKey)
  var key = D_DKEY + discoveryKey(bufPK).toString('hex')
  this._peers.push(bufPK.toString('hex'))
  uniq(this._peers)
  this._peers.sort()
  this._db.put(D_DKEY + discoveryKey(bufPK).toString('hex'), bufPK)
  this._db.put('peers', Buffer.from(JSON.stringify(this._peers)))
}

MQ.prototype._getPeers = function (cb) {
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
  self._getPeers(function (err, peers) {
    if (err) return cb(err)
    self._addPeer(pubKey)
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
  self._getPeers(function (err, peers) {
    for (var i = 0; i < pubKeys.length; i++) {
      self._addPeer(pubKeys[i])
    }
    self._db.flush(cb)
  })
}

MQ.prototype._getPubKeyForDKey = function (dKey, cb) {
  if (!isValidKey(dKey)) return nextTick(cb, new Error('invalid discovery key'))
  var key = D_DKEY + asBuffer(dKey).toString('hex')
  this._db.get(key, function (err, node) {
    if (err) cb(err)
    else cb(null, node ? node.value : null)
  })
}

MQ.prototype.listen = function (cb) {
  if (!cb) cb = noop
  var self = this
  if (self._listening) return nextTick(cb)
  self._listening = true
  self.getPublicKey(function (err, ownId) {
    if (err && cb) return cb(err)
    else if (err) return
    var server = self._network.createServer(function (cstream) {
      var proto = protocol({
        live: true,
        sparse: true
      })
      var core
      proto.on('feed', onfeed.bind(null, proto))
      pump(proto, cstream, proto, function (err) {
        // ...
      })
    })
    server.listen(ownId)
    cb(null, server)
  })
  function onfeed (proto, discoveryKey) {
    self._getPubKeyForDKey(discoveryKey, function (err, pubKey) {
      if (err) return proto.destroy()
      if (!pubKey) return
      var id = pubKey.toString('hex')
      self._listenProtos[id] = proto
      self._openListenCore(id, function (err, core) {
        var r = core.replicate({
          sparse: true,
          live: true,
          stream: proto
        })
        var peer = r.feeds[0].peer
        var onhave = peer.onhave
        var len = 0
        peer.onhave = function (have) {
          if (typeof onhave === 'function') onhave.call(peer, have)
          if (!have.bitfield) {
            len = Math.max(core.length, len, have.start + have.length)
            var eq = self._sendCoreLengths[id] === len
            self._sendCoreLengths[id] = len
            if (!eq) self.emit('_coreLength!'+id, len)
          }
        }
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
  self._getPeers(function (err, peers) {
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
  var pubKey = null, core = null
  var pending = 3
  self._openSendCore(to, function (err, core_) {
    if (err) return cb(err)
    core = core_
    if (--pending === 0) ready()
  })
  self.getPublicKey(function (err, pubKey_) {
    if (err) return cb(err)
    pubKey = pubKey_
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
    var stream = self._network.connect(toBuf, { id: pubKey })
    var r = core.replicate({
      ack: true,
      live: true,
      sparse: true
    })
    connection.once('_close', function () {
      r.end()
      stream.close()
    })
    r.on('ack', function (ack) {
      connection.emit('ack', ack)
    })
    pump(stream, r, stream, function (err) {
      if (err) connection.emit('error', err)
      else connection.emit('close')
    })
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
  self.getSecretKey(function (err, secretKey) {
    var q = self._sendCoreQueue[key]
    self._sendCoreQueue[key] = null
    if (err) {
      cb(err)
      for (var i = 0; i < q.length; i++) q[i](err)
      return
    }
    var pubKey = secretKey.slice(32)
    var core = hypercore(self._storeFn(to), pubKey, { secretKey })
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
  core = hypercore(self._storeFn(id), Buffer.from(id,'hex'), { sparse: true })
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
