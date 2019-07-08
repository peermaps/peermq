var EventEmitter = require('events').EventEmitter
var multiplex = require('multiplex')
var pump = require('pump')
var hypercore = require('hypercore')
var onend = require('end-of-stream')

module.exports = MQ

function MQ (opts) {
  var self = this
  if (!(this instanceof MQ)) return new MQ(opts)
  this.id = opts.id
  this._network = opts.network
  this._storage = opts.storage
  this._cores = {}
  this._connections = {}
  this._coresOpen = {}
  this._streams = []
}
MQ.prototype = Object.create(EventEmitter.prototype)

MQ.prototype.listen = function (ownId) {
  var self = this
  var server = this._network.createServer(function (stream) {
    //console.log('CONNECT')
    var plex = multiplex(function (stream, id) {
      if (self._coresOpen.hasOwnProperty(id)) return stream.destroy()
      if (id === ownId) return stream.destroy()
      self._coresOpen[id] = true
      onend(stream, function () {
        delete self._coresOpen[id]
      })
      //console.log('STREAM', id)
      if (!self._cores[id]) {
        self._cores[id] = hypercore(self._storage(id), id, {
          sparse: true
        })
        self._cores[id].createReadStream({ live: true })
          .on('data', function (row) {
            self.emit('message', row)
          })
      }
      var feed = self._cores[id]
      // todo: feed.download()
      //var r = feed.replicate({ sparse: true, live: true })
      var r = feed.replicate({ live: true })
      pump(r, stream, r)
    })
    pump(stream, plex, stream, function (err) {
      // ...
    })
  })
  server.listen(ownId)
}

MQ.prototype.send = function (opts, cb) {
  var core = this._open(opts.to)
  core.append(opts.message, cb)
}

MQ.prototype._open = function (to) {
  if (this._cores.hasOwnProperty(to)) return this._cores[to]
  this._cores[to] = hypercore(this._storage(to))
  this._connect(to)
  return this._cores[to]
}

MQ.prototype._connect = function (to) {
  var self = this
  if (self._connections.hasOwnProperty(to)) return
  var c = self._connections[to] = self._network.connect(to)
  var core = self._cores[to]
  c.on('connect', function (stream) {
    self._streams.push(stream)
    var plex = multiplex()
    var reTime = null
    core.ready(function restream () {
      reTime = null
      var s = plex.createStream(core.key.toString('hex'))
      var r = self._cores[to].replicate({ live: true, sparse: true })
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
    stream.destroy()
  })
}
