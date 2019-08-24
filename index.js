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

// bitfield prefixes:
var B_READ = 'r!'
var B_ARCHIVE = 'a!'
var B_DELETED = 'd!'

// db prefixes:
var D_DKEY = 'd!'
var D_SIZE = 's!'

module.exports = MQ

function MQ (opts) {
  if (!(this instanceof MQ)) return new MQ(opts)
  EventEmitter.call(this)
  this._network = opts.network
  this._storage = opts.storage
  this._db = new Tinybox(this._openStore('db'))
  this._sendCores = {}
  this._listenCores = {}
  this._listenProtos = {}
  this._idStore = null
  this._secretKey = null
  this._sendConnections = {}
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
  var bufPK = asBuffer(pubKey)
  var key = D_DKEY + discoveryKey(bufPK).toString('hex')
  this._db.put(D_DKEY + discoveryKey(bufPK).toString('hex'), bufPK)
}

MQ.prototype.addPeer = function (pubKey, cb) {
  if (!isValidKey(pubKey)) return nextTick(cb, new Error('invalid pub key'))
  this._addPeer(pubKey)
  this._db.flush(cb)
}

MQ.prototype.addPeers = function (pubKeys, cb) {
  for (var i = 0; i < pubKeys.length; i++) {
    if (!isValidKey(pubKeys[i])) {
      return nextTick(cb, new Error('invalid pub key'))
    }
  }
  for (var i = 0; i < pubKeys.length; i++) {
    this._addPeer(pubKeys[i])
  }
  this._db.flush(cb)
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
  var self = this
  self.getPublicKey(function (err, id) {
    if (err && cb) return cb(err)
    else if (err) return
    var server = self._network.createServer(function (cstream) {
      var proto = self._listenProtos[id] = protocol({
        live: true,
        sparse: true
      })
      proto.feed(id)
      proto.on('feed', function (discoveryKey) {
        self._getPubKeyForDKey(discoveryKey, function (err, pubKey) {
          if (err) return proto.destroy()
          if (!pubKey) return
          console.log('FEED', discoveryKey, '=>', pubKey)
          var hpkey = pubKey.toString('hex')
          var core = self._listenCores[hpkey] = hypercore(
            self._storeFn(hpkey), pubKey, { sparse: true }
          )
          core.ready(function () {
            self._bitfield.read[id] = self._multi.open(B_READ + id + '!')
            self._bitfield.archive[id] = self._multi.open(B_ARCHIVE + id + '!')
            self._bitfield.deleted[id] = self._multi.open(B_DELETED + id + '!')
            self.emit('_core', core)
            core.replicate({
              sparse: true,
              live: true,
              stream: proto
            })
          })
        })
      })
      proto.on('handshake', function (h) {
        console.log('HANDSHAKE',h)
      })
      pump(proto, cstream, proto, function (err) {
        console.log('END', err)
      })
    })
    server.listen(id)
  })
  /*
  var plex = multiplex(function (stream, id) {
    if (!/^[0-9A-Fa-f]{64}$/.test(id)) return stream.destroy()
    var core = self._listenCores[id]
    if (!core) {
      core = self._listenCores[id] = hypercore(
        self._storeFn(id), id, { sparse: true }
      )
      core.ready(function () {
        self._bitfield.read[id] = self._multi.open(B_READ + id + '!')
        self._bitfield.archive[id] = self._multi.open(B_ARCHIVE + id + '!')
        self._bitfield.deleted[id] = self._multi.open(B_DELETED + id + '!')
        self.emit('_core', self._listenCores[id])
      })
    }
    var r = core.replicate({
      live: true,
      sparse: true
    })
    r.on('handshake', function (h) {
      console.log('HANDSHAKE', h)
    })
    pump(stream, r, stream, function (err) {
      // ...
    })
  })
  pump(cstream, plex, cstream, function (err) {
    // ...
  })
  */
}

MQ.prototype.createReadStream = function (name, opts) {
  var self = this
  var cores = []
  var coreKeys = []
  var offsets = {}
  var queue = []
  self.on('_core', oncore)
  Object.values(self._listenCores).forEach(oncore)

  var reading = false
  var stream = new Readable({
    objectMode: true,
    read: function (n) {
      if (queue.length > 0) {
        this.push(queue.shift())
      } else if (cores.length === 0) {
        this.push(null)
      } else {
        reading = true
        for (var i = 0; i < cores.length; i++) {
          readNext(cores[i], coreKeys[i], push)
        }
      }
    },
    destroy: function () {
      cleanup()
    }
  })
  return stream
  function push (err, msg) {
    console.log('PUSH',msg)
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
    console.log(self._listenCores)
    self.removeListener('_core', oncore)
  }
  function oncore (core) {
    cores.push(core)
    var key = core.key.toString('hex')
    coreKeys.push(key)
    offsets[key] = 0
    if (!self._bitfield.read[key]) {
      self._bitfield.read[key] = self._multi.open(B_READ + key + '!')
    }
    readNext(core, key, push)
  }
  function readNext (core, key, cb) {
    self._bitfield.read[key].next0(offsets[key], function (err, x) {
      console.log('NEXT',key,err,x, core.length)
      if (err) return cb(err)
      else if (x >= core.length) return cb(null, null)
      core.get(x, function (err, node) {
        if (err) return cb(err)
        offsets[key] = x
        cb(null, node)
      })
    })
  }
}

MQ.prototype.send = function (m, cb) {
  var self = this
  if (!isValidKey(m.to)) return nextTick(cb, new Error('invalid key: ' + m.to))
  if (!this._sendCores[m.to]) {
    this._sendCores[m.to] = hypercore(this._storeFn(m.to))
  }
  var core = this._sendCores[m.to]
  core.append(m.message, cb)

  if (!this._sendConnections[m.to]) {
    var toBuf = asBuffer(m.to)
    self.getPublicKey(function (err, id) {
      var stream = self._network.connect(toBuf, { id })
      console.log('CREATE STREAM', m.to)
      var proto = protocol({
        live: true,
        sparse: true
      })
      proto.on('feed', function (discoveryKey) {
        console.log('FEED', discoveryKey)
      })
      proto.on('handshake', function (h) {
        console.log('HANDSHAKE',h)
      })
      proto.feed(toBuf)
      pump(proto, stream, proto, function (err) {
        console.log('END', err)
      })
      proto.feed(id)
      console.log('SEND FEED', toBuf)
      console.log('SEND FEED', id)
    })
  }
  /*
  var r = core.replicate({
    live: true,
    download: false,
    ack: true
  })
  r.on('handshake', function (h) {
    console.log('HANDSHAKE',h)
  })
  r.on('ack', function (seq) {
    console.log('ACK', seq)
    core.clear(seq, seq+1, function (err) {
      // ...
    })
  })
  pump(r, stream, r, function (err) {
    console.log('END', err)
  })
  */
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
