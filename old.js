var EventEmitter = require('events').EventEmitter
var multiplex = require('multiplex')
var pump = require('pump')
var hypercore = require('hypercore')
var onend = require('end-of-stream')
var MultiBitfield = require('bitfield-db')
var Tinybox = require('tinybox')
var { Readable } = require('readable-stream')

var SEQ = 's!'

module.exports = MQ

function MQ (opts) {
  var self = this
  if (!(this instanceof MQ)) return new MQ(opts)
  this.id = opts.id
  this._network = opts.network
  this._storage = typeof opts.storage === 'string'
    ? function (x) { return path.join(opts.storage, x) }
    : opts.storage
  this.db = new Tinybox(this._storage('db'))
  this._cores = {
    listen: null,
    send: {}
  }
  this._connections = {}
  this._coresOpen = {}
  this._streams = []
  this._multiBitfield = new MultiBitfield(this._storage('bitfield'))
  this._bitfields = {
    unread: this._multiBitfield.open('_unread!'),
    archive: this._multiBitfield.open('_archive!'),
    remote: {}
  }
}
MQ.prototype = Object.create(EventEmitter.prototype)

MQ.prototype.listen = function (ownId) {
  var self = this
  var server = this._network.createServer(function (stream) {
    var plex = multiplex(function (stream, id) {
      if (self._coresOpen.hasOwnProperty(id)) return stream.destroy()
      if (id === ownId) return stream.destroy()
      self._coresOpen[id] = true
      onend(stream, function () {
        delete self._coresOpen[id]
      })
      var feed = self._openListen(id)
      // todo: feed.download()
      // var r = feed.replicate({ sparse: true, live: true })
      var r = feed.replicate({ live: true })
      pump(r, stream, r)
    })
    pump(stream, plex, stream, function (err) {
      // ...
    })
  })
  server.listen(ownId)
}

MQ.prototype._getListenCores = function (cb) {
  var self = this
  if (self._cores.listen) return cb(null, self._cores.listen) // allowed to zalgo
  if (self._coreQueue) return self._coreQueue.push(cb)
  self._coreQueue = []
  self.db.get('cores', function (err, cores) {
    if (err && !err.notFound) return cb(err)
    ;(cores || []).forEach(function (key) {
      self._cores.listen[key] = self._openSend()
    })
    for (var i = 0; i < self._coreQueue.length; i++) {
      self._coreQueue[i](self._cores)
    }
    self._coreQueue = null
  })
}

MQ.prototype.archive = function (rows, cb) {
  var self = this
  var max = {}
  rows.forEach(function (row) {
    max[row.from] = Math.max(max[row.from] || 0, row.seq)
  })
}

MQ.prototype.clear = function (keys, cb) {
}

MQ.prototype.createReadStream = function (name, opts) {
  var self = this
  var streams = []
  var opened = {}
  var reading = false
  var registering = true
  self.db.get('cores', function (err, cores) {
    if (err && err.notFound) {}
    else if (err) {}
    else JSON.parse(cores).forEach(registerCore)
    registering = false
    if (!opts.live && streams.length === 0) {
      self.removeListener('_core', registerCore)
      return this.push(null)
    }
  })
  Object.keys(self._cores).forEach(registerCore)
  self.on('_core', registerCore)

  var j = 0
  var output = new Readable({
    objectMode: true,
    read: function (n) {
      if (streams.length === 0 && !opts.live && !registering) {
        self.removeListener('_core', registerCore)
        return this.push(null)
      }
      reading = true
      for (var i = 0; i < streams.length; i++) {
        var s = streams[(i+j)%streams.length]
        var data = s.stream.read()
        if (data !== null) {
          j = i
          reading = false
          this.push({ from: s.id, seq: s.seq++, data })
        }
      }
    }
  })
  return output

  function registerCore (id) {
    if (opened.hasOwnProperty(id)) return
    opened[id] = true
    var feed = self._openListen(id)
    self._getSeq(id, function (err, seq) {
      if (err) return
      var stream = feed.createReadStream({
        start: seq,
        live: opts.live
      })
      onend(stream, function () {
        var ix = streams.indexOf(stream)
        if (ix >= 0) streams.splice(ix,1)
      })
      var rec = { id, seq, stream }
      streams.push(rec)
      stream.on('readable', function () {
        reading = false
        output.push({ from: rec.id, seq: rec.seq++, data: stream.read() })
      })
      if (reading) {
        var data = stream.read()
        if (data !== null) {
          j = i
          reading = false
          this.push({ from: rec.id, seq: rec.seq++, data })
        }
      }
    })
  }
}

MQ.prototype._getSeq = function (id, cb) {
  this.db.get(SEQ+id, function (err, seq) {
    if (err && err.notFound) cb(null, 0)
    else if (err) cb(err)
    else cb(null, seq)
  })
}

MQ.prototype._putSeq = function (id, seq, cb) {
  this.db.put(SEQ+id, seq, cb)
}

MQ.prototype.archive = function (msgs, cb) {
  if (!Array.isArray(msgs)) msgs = [msgs]
  console.log('archive', msgs)
  cb()
}

MQ.prototype.send = function (opts, cb) {
  var core = this._openSend(opts.to)
  core.append(opts.message, cb)
}

MQ.prototype._openSend = function (to) {
  if (this._cores.hasOwnProperty(to)) return this._cores[to]
  this._cores[to] = hypercore(this._storage(to))
  this._connect(to)
  return this._cores[to]
}

MQ.prototype._openListen = function (id) {
  var self = this
  if (!self._cores[id]) {
    self._cores[id] = hypercore(self._storage(id), id, {
      sparse: true
    })
    var s = JSON.stringify(Object.keys(self._cores))
    self.db.put('cores', s, function (err) {
      // ...
    })
    self.emit('_core', id)
  }
  return self._cores[id]
}

MQ.prototype._connect = function (to) {
  var self = this
  if (self._connections.hasOwnProperty(to)) return
  var c = self._connections[to] = self._network.connect(to)
  var core = self._cores[to]
  c.on('connect', function (stream) {
    //self._streams.push(stream)
    var plex = multiplex()
    var reTime = null
    core.ready(function restream () {
      reTime = null
      var s = plex.createStream(core.key.toString('hex'))
      var r = self._cores[to].replicate({
        live: true,
        download: false,
        ack: true
      })
      r.on('ack', function (seq) {
        console.log('ACK', seq)
        //feed.clear(seq, seq+1, function (err) {
          // ...
        //})
      })
      pump(s, r, s, function (err) {
        reTime = setTimeout(restream, 1000)
      })
    })
    pump(stream, plex, stream, function (err) {
      if (reTime) clearTimeout(reTime)
      var ix = self._streams.indexOf(stream)
      if (ix >= 0) self._streams.splice(ix,1)
    })
  })
}

MQ.prototype.close = function () {
  var self = this
  Object.keys(self._connections).forEach(function (key) {
    self._connections[key].close()
  })
  self._streams.forEach(function (stream) {
    //stream.close()
  })
}
