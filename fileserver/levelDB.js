const level = require('level');
const grpc = require('grpc');
const config = require('./config.js');
const levelDBObjProto = grpc.load(config.LEVEL_DB_OBJ_PROTO_PATH);
const db = level(config.LEVEL_DB_PATH)
const server = new grpc.Server()

const PORT = process.env.PORT || 27527;

server.bind(config.HOST_NAME + ":" + PORT, grpc.ServerCredentials.createInsecure())
console.log("LevelDB server running at: " + PORT)
server.start()

server.addService(levelDBObjProto.LevelDBService.service, {
  get: function(call, callback) {
    const key = call.request
    db.get(key, function(err, value) {
      if (err) {
        return console.error(err)
      }
      callback(null, {
        key: key,
        content: value
      })
    })
  },
  put: function(call, callback) {
    const data = call.request
    db.put(key, data.content, function(err) {
      if (err) {
        return console.error(err)
      }
      callback(null, true)
    })
  },
  del: function(call, callback) {
    const key = call.request
    db.del(key, function(err) {
      if (err) {
        return console.error(err)
      }
      callback(null, true)
    })
  }
})



