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

function get(key, callback) {
  db.get(key, function(err, value) {
    if (err) {
      return console.error(err)
    }
    callback(null, {
      key: key,
      jwt: JSON.parse(value)
    })
  })
}

function put(key, data, callback) {
  db.put(key, JSON.stringify(data), function(err) {
    if (err) {
      return console.error(err)
    }
    callback(null, true)
  })
}

server.addService(levelDBObjProto.LevelDBService.service, {
  get: function(call, callback) {
    get(call.request, callback)
  },
  put: function(call, callback) {
    put(call.request.key, call.request, callback)
  },
  del: function(call, callback) {
    const key = call.request
    db.del(key, function(err) {
      if (err) {
        return console.error(err)
      }
      callback(null, true)
    })
  },
  putChild: function(call, callback) {
    const data = call.request
    get(data.key, function(err, value) {
      if (err) {
        return console.error(err)
      }
      value = JSON.paarse(value)
      if (value.content == null) {
        value.content = []
      }
      for (var i = 0; i < data.content.length; i++) {
        value.content.push(data.content[i])
      }
      put(value.key, value, function(err, isSuccessfull) {
        if (err) {
          return console.error(err)
        }
        if (isSuccessfull) {
          callback(null, true)
        }
      })
    })
  },
  delChild: function(call, callback) {
    const data = call.request
    get(data.key, function(err, value) {
      if (err) {
        return console.error(err)
      }
      value = JSON.parse(value)
      value.content = value.content.filter(function(element) {
        return !data.content.includes(element)
      })
      put(value.key, value, function(err, isSuccessfull) {
        if (err) {
          return console.error(err)
        }
        if (isSuccessfull) {
          callback(null, true)
        }
      })
    })
  }
})