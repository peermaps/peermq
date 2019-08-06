var MultiBitfield = require('bitfield-db/multi')
var Tinybox = require('tinybox')
var multiplex = require('multiplex')
var hypercore = require('hypercore')
var { keyPair } = require('hypercore-crypto')
var pump = require('pump')
var { Readable } = require('readable-stream')
var { EventEmitter } = require('events')
var { nextTick } = process
var path = require('path')
var raf = require('random-access-file')

var UNREAD = 'u!'
var ARCHIVE = 'a!'

module.exports = MQ

function MQ (opts) {
  if (!(this instanceof MQ)) return new MQ(opts)
  this._network = opts.network
  this._storage = opts.storage
  this._sendCores = {}
  this._idStore = null
  this._secretKey = null
  this._listenCores = {}
  this._sendConnections = {}
  var multi = new MultiBitfield({
    storage: this._openStore('bitfield')
  })
  this._bitfield = {
    unread: multi.open(UNREAD),
    archive: multi.open(ARCHIVE)
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

MQ.prototype.getPublicKey = function (cb) {
  this.getSecretKey(function (err, buf) {
    if (err) cb(err)
    else cb(null, buf.slice(32))
  })
}

MQ.prototype.getSecretKey = function (cb) {
  var self = this
  if (self._secretKey) return nextTick(cb, self._secretKey)
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

MQ.prototype.listen = function (cb) {
  var self = this
  var server = self._network.createServer(function (cstream) {
    var plex = multiplex(function (stream, id) {
      console.log('PLEX',id)
      if (!self._listenCores[id]) {
        self._listenCores[id] = hypercore(
          self._storeFn(id), id, { sparse: true }
        )
        self.emit('_core', id, self._listenCores[id])
      }
      var core = self._listenCores[id]
      var r = core.replicate({
        live: true,
        sparse: true
      })
      pump(stream, r, stream, function (err) {
        // ...
      })
    })
    pump(cstream, plex, cstream, function (err) {
      // ...
    })
  })
  self.getPublicKey(function (err, id) {
    if (err) return self.emit('error', err)
    if (typeof cb === 'function') cb(id)
    server.listen(id)
  })
}

MQ.prototype.createReadStream = function (name, opts) {
  var self = this
  self.on('_core', function (core) {
    console.log('core',core.key)
  })
  return new Readable({
    objectMode: true,
    read: function (n) {
      console.log(self._listenCores)
      this.push(null)
    }
  })
}

MQ.prototype.send = function (m, cb) {
  if (!this._sendCores[m.to]) {
    this._sendCores[m.to] = hypercore(this._storeFn(m.to))
  }
  var core = this._sendCores[m.to]
  if (!this._sendConnections[m.to]) {
    console.log('connect',m.to)
    var c = this._network.connect(m.to)
    c.on('connect', function (stream) {
      var plex = multiplex()
      var s = plex.createStream(m.to)
      var r = core.replicate({
        live: true,
        download: false,
        ack: true
      })
      r.on('ack', function (seq) {
        console.log('ACK', seq)
        core.clear(seq, seq+1, function (err) {
          // ...
        })
      })
      pump(s, r, s, function (err) {
        // ...
      })
      pump(stream, plex, stream, function (err) {
        // ...
      })
    })
  }
  core.append(m.message, cb)
}
