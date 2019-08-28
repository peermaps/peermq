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
  this._db = new Tinybox(this._openStore('db'))
  this._sendCores = {}
  this._sendCoreLengths = {}
  this._listenCores = {}
  this._listenProtos = {}
  this._idStore = null
  this._secretKey = null
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
  var s = self._storage(name)
  if (typeof s === 'string') return raf(s)
  else return s
}

MQ.prototype._storeFn = function (prefix) {
  var self = this
  return function (name) {
    var s = self._storage(path.join(prefix,name))
    if (typeof s === 'string') return raf(s)
    else return s
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
  if (!self._idStore) {
    self._idStore = self._openStore('id')
  }
  self._idStore.stat(function (err, stat) {
    if (err && err.code !== 'ENOENT') return cb(err)
    if (!stat || stat.size === 0) {
      var kp = keyPair()
      self._idStore.write(0, kp.secretKey, function (err) {
        if (err) return cb(err)
        self._secretKey = kp.secretKey
        cb(null, kp.secretKey)
      })
    } else {
      self._idStore.read(0, stat.size, function (err, buf) {
        if (err) return cb(err)
        self._secretKey = buf
        cb(null, buf)
      })
    }
  })
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
        console.log('END', err)
      })
    })
    server.listen(ownId)
    cb(null)
  })
  function onfeed (proto, discoveryKey) {
    var feed = proto._remoteFeeds[0]
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
        var onhave = feed.peer.onhave
        var len = 0
        feed.peer.onhave = function (have) {
          if (typeof onhave === 'function') onhave.call(feed.peer, have)
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
  this._multi.flush(cb)
}

MQ.prototype.clear = function (msg, cb) {
  nextTick(cb)
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
    if (!self._bitfield.read[key]) {
      self._bitfield.read[key] = self._multi.open(B_READ + key + '!')
    }
    readNext(core, key, push)
  }
  function readNext (core, key, cb) {
    var len = self._sendCoreLengths[key]
    self._bitfield.read[key].next0(offsets[key], function (err, x) {
      console.log(`next ${offsets[key]} => ${x}`)
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

  function ready () {
    var toBuf = asBuffer(to)
    var stream = self._network.connect(toBuf, { id: pubKey })
    var r = core.replicate({
      ack: true,
      live: true,
      sparse: true
    })
    r.on('ack', function (ack) {
      console.log('ACK', ack)
    })
    pump(stream, r, stream, function (err) {
      if (err) console.error(err)
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
  if (!isValidKey(to)) return nextTick(cb, new Error('invalid key: ' + to))
  if (self._sendCores[to]) return nextTick(cb, self._sendCores[to])
  self.getSecretKey(function (err, secretKey) {
    if (err) return cb(err)
    var pubKey = secretKey.slice(32)
    var core = hypercore(self._storeFn(to), pubKey, { secretKey })
    self._sendCores[to] = core
    cb(null, core)
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
