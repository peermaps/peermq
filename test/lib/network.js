var duplexify = require('duplexify')
var EventEmitter = require('events').EventEmitter
var onend = require('end-of-stream')
var { nextTick } = process
var through = require('through2')

module.exports = function () {
  if (/^(yes|true|1)$/i.test(process.env.PEERMQ_REAL_NETWORK)) {
    return require('../../network.js')
  }
  var swarm = {}
  var exports = {}

  exports.createServer = Server
  exports.Server = Server

  function Server (opts, fn) {
    if (!(this instanceof Server)) return new Server(opts, fn)
    if (typeof opts === 'function') {
      fn = opts
      opts = {}
    }
    if (!opts) opts = {}
    if (fn) this.on('connection', fn)
  }
  Server.prototype = Object.create(EventEmitter.prototype)

  Server.prototype.listen = function (id) {
    var self = this
    if (!swarm[id]) swarm[id] = { listen: [], connect: [] }
    var f = function (stream) {
      self.emit('connection', stream)
    }
    swarm[id].listen.push(f)
    swarm[id].connect.forEach(function (c) { f(c) })
  }

  Server.prototype.close = function () {}

  exports.connect = function (id, opts) {
    if (!swarm[id]) swarm[id] = { listen: [], connect: [] }
    var d = duplexify()
    var s = duplexify()
    var r = through()
    var w = through()
    d.setReadable(r)
    d.setWritable(w)
    s.setWritable(r)
    s.setReadable(w)
    d.close = function () {}
    s.close = function () {}
    swarm[id].connect.push(s)
    onend(d, function () {
      var ix = swarm[id].connect.indexOf(s)
      if (ix >= 0) swarm[id].connect.splice(ix,1)
    })
    swarm[id].listen.forEach(function (f) { f(s) })
    return d
  }

  return exports
}
