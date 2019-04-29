const level = require('level');
const grpc = require('grpc');
const config = require('./config.js');
const levelDBObjProto = grpc.load('levelDBObj.proto');
const db = level(config.LEVEL_DB_PATH)
const server = new grpc.Server()

const PORT = process.env.PORT || 27527;

server.bind(config.HOST_NAME + ":" + PORT)
console.log("LevelDB server running at: " + PORT)
server.start()

server.addService(levelDBObjProto.LevelDBService.service, {
  get: function(call, callback) {
    const key = call.request
    db.get(new Buffer(key), function(err, value) {
      if (err) {
        return console.error(err)
      }
      callback(null, value)
    })
  },
  put: function(call, callback) {
    const data = call.request
    db.put(new Buffer(data.key), { jwt: data.content }, function(err) {
      if (err) {
        return console.error(err)
      }
      callback(null, null)
    })
  }
})



