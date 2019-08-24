var hyperswarm = require('hyperswarm')
var duplexify = require('duplexify')
var EventEmitter = require('events').EventEmitter

exports.createServer = Server
exports.Server = Server

function Server (opts, fn) {
  if (!(this instanceof Server)) return new Server(opts, fn)
  if (typeof opts === 'function') {
    fn = opts
    opts = {}
  }
  if (!opts) opts = {}
  this._swarm = hyperswarm(opts)
  if (fn) this.on('connection', fn)
}
Server.prototype = Object.create(EventEmitter.prototype)

Server.prototype.listen = function (id) {
  var self = this
  self._swarm.on('connection', function (stream, info) {
    self.emit('connection', stream, info)
  })
  self._swarm.join(id, {
    lookup: false,
    announce: true
  })
}

exports.connect = function (id, opts) {
  var d = duplexify()
  d._swarm = hyperswarm(opts)
  d._swarm.join(id, {
    lookup: true,
    announce: false
  })
  d._swarm.on('connection', function (stream, info) {
    d.setReadable(stream)
    d.setWritable(stream)
    d.emit('connection', stream)
  })
  return d
}
