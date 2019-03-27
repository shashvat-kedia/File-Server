const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const config = require('./config.js');
const amqp = require('amqplib/callback_api');
const hash = require('object-hash');
const s3Pull = require('./s3Pull.js');
const q = require('q');
const app = express()

const S3 = new AWS.S3(config.AWS_CONFIG)

var rmq_connection = null
var con_channel = null

app.use(bodyParser.urlencoded({ extended: true }))
app.use(bodyParser.json())
app.use(cors())

const PORT = 27272 | process.env.PORT

function connectToRMQ() {
  amqp.connect(config.RMQ_URL, function(err, con) {
    if (err) {
      console.error("RMQ Error:- " + err.message)
      return setTimeout(connectToRMQ, 1000)
    }
    con.on("error", function(err) {
      if (err.message != "Connection closing") {
        console.error("RMQ Error:- " + err.message)
        throw error
      }
    })
    con.on("close", function(err) {
      console.error("RMQ Error:- " + err.message)
      console.info("Retrying...")
      return setTimeout(connectToRMQ(), 1000)
    })
    console.log("RMQ connected")
    rmq_connection = con
    startConsumer()
  })
}

function startConsumer() {
  rmq_connection.createChannel(function(err, ch) {
    if (err) {
      console.error("RMQ Error:- " + err.message)
      return
    }
    ch.on("error", function(err) {
      console.error("RMQ Error:- " + err.message)
      return
    })
    ch.on("close", function(err) {
      console.error("RMQ Error:- " + err)
      return
    })
    ch.assertQueue(config.QUEUE_NAME_S3_SERVICE, { durable: false })
    con_channel = ch
    console.log("Consumer started")
    consume()
  })
}

function consume() {
  //Enable acknolwgements here to ensure reliability and fault tolerence
  try {
    con_channel.consume(config.QUEUE_NAME_S3_SERVICE, function(message) {
      var jsonMessage = JSON.parse(message.content.toString())
      if (jsonMessage.action == config.ACTION_UPLOAD_FILE) {
        sendToS3(jsonMessage.destPath)
      }
      else {
        updateFile(jsonMessage.destPath, jsonMessage.fileId)
      }
    })
  }
  catch (exception) {
    console.error("Consumer Exception:- " + exception.message)
  }
}

function getS3Params(body, key) {
  var s3_params = config.S3_CONFIG
  s3_params["Body"] = body
  s3_params["Key"] = key
  return s3_params
}

function uploadToS3(s3_params, chunk_hash) {
  S3.upload(s3_params, function(err, data) {
    if (err) {
      console.error("S3 Upload Error:- " + err)
      throw err
    }
    if (data) {
      console.log("File chunk successfully uploaded to AWS S3:- " + chunk_hash)
    }
  })
}

function createChunksAndProcess(path, isUpload) {
  var deferred = q.defer()
  chunkPaths = [path.substring(path.lastIndexOf('.'), path.length)]
  var readStream = fs.createReadStream(path, { highWaterMark: config.READ_CHUNKSIZE })
  readStream.on('data', function(chunk) {
    var chunkHash = hash(chunk)
    var chunkPath = "/chunks/" + chunkHash + ".txt"
    chunkPaths.push(chunkPath)
    if (isUpload) {
      uploadToS3(getS3Params(chunk, chunkPath), chunkHash)
    }
  })
  readStream.on('close', function(err) {
    if (err) {
      console.error(err)
      deferred.reject(err)
    }
    deferred.resolve({
      "chunkPaths": chunkPaths
    })
  })
  return deferred.promise
}

function uploadSpecificChunks(path, chunksToUpload) {
  var countSuccess = 0
  var deferred = q.defer()
  fs.open(path, 'r', function(err, fd) {
    if (err) {
      console.error(err)
      deferred.reject(err)
    }
    for (var i = 0; i < chunksToUpload.length; i++) {
      var buffer = new Buffer(config.READ_CHUNKSIZE)
      fs.read(fd, buffer, 0, config.READ_CHUNKSIZE, chunksToUpload[i].index * config.READ_CHUNKSIZE, function(err, nread) {
        if (err) {
          console.error(err)
          deferred.reject(err)
        }
        var data
        if (nread < config.READ_CHUNKSIZE) {
          data = buffer.slice(0, nread)
        }
        else {
          data = buffer
        }
        uploadToS3(getS3Params(data, chunksToUpload[i].chunkPath), chunksToUpload[i].chunkHash)
        countSuccess += 1
      })
    }
    if (countSuccess == chunksToUpload.length) {
      deferred.resolve({
        "status": 200
      })
    }
    else {
      deferred.resolve({
        "status": 404
      })
    }
  })
}

function uploadChunkPathFile(path, chunkPaths) {
  var deferred = q.defer()
  var filePath = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.')) + ".txt"
  fs.unlinkSync(path)
  fs.writeFile(filePath, chunkPaths.toString(), function(err) {
    if (err) {
      console.error(err)
      deferred.reject(err)
    }
    else {
      uploadToS3(getS3Params(fs.createReadStream(filePath),
        "/uploads/files/" + filePath), "Chunk hash path file upload")
      fs.unlinkSync(filePath)
      deferred.resolve({
        "status": 200
      })
    }
  })
}

function sendToS3(path) {
  createChunksAndProcess(path, true).then(function(response) {
    uploadChunkPathFile(path, response.chunksPaths).then(function(response) {
      if (respone.status == 200) {
        //Message acknolegements can be sent here but have to take into account timout so that any
        //message is not processed twice
        console.log("File uploaded")
      }
    }).fail(function(err) {
      console.error(err)
    })
  }).fail(function(err) {
    console.error(err)
  })
}

function updateFile(path, fileId) {
  s3Pull.pullChunkPathFileFromS3(fileId).then(function(response) {
    if (response.status == 200) {
      createChunksAndProcess(path, false).then(function(data) {
        var chunksPaths = []
        var chunksToUpload = []
        for (var i = 0; i < data.chunkPaths.length; i++) {
          chunkPaths.push(data.chunksPaths[i])
          if (data.chunkPaths[i] != response.chunksPaths[i]) {
            chunkToUpload.push({
              index: i,
              chunksPath: data.chunkPaths[i],
              chunkHash: data.chunkPaths[i].substring(data.chunksPaths[i].lastIndexOf('/') + 1,
                data.chunksPaths[i].lastIndexOf('.'))
            })
          }
        }
        uploadSpecificChunks(path, chunksToUpload).then(function(response) {
          if (response.status = 200) {
            console.log("Chunks updated")
            uploadChunkPathFile(path, chunkPaths).then(function(response) {
              if (response.status = 200) {
                console.log("File updated")
              }
            }).fail(function(err) {
              console.error(err)
            })
          }
          else {
            //Modify to get the chunks that could not be uploaded and they retry for certain defined no. of times
            console.log("File not uploaded completely")
          }
        }).fail(function(err) {
          console.error(err)
        })
      }).fail(function(err) {
        console.error(err)
      })
    }
    else {
      //Storing them for the time being
      sendToS3(path)
    }
  }).fail(function(err) {
    console.error(err)
  })
}

app.listen(PORT, function() {
  connectToRMQ()
  console.log("Consumer service listening on :- " + PORT)
})