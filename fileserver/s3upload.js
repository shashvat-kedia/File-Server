const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const config = require('./config.js');
const amqp = require('amqplib/callback_api');
const hash = require('object-hash');
const q = require('q');
const s3Pull = require('./s3Pull.js');
const redis = require('redis');
const redisClient = redis.createClient();
const app = express()

const S3 = new AWS.S3(config.AWS_CONFIG)

var rmq_connection = null
var con_channel = null
var pub_channel = null
var offlinePubQueue = []

app.use(bodyParser.urlencoded({ extended: true }))
app.use(bodyParser.json())
app.use(cors())

const PORT = 27272 | process.env.PORT

redisClient.on('connect', function() {
  console.log("Redis client connected")
})

redisClient.on('error', function(err) {
  console.error(err)
})

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
    startPublisher()
    startConsumer()
  })
}

function startPublisher() {
  rmq_connection.createConfirmChannel(function(err, ch) {
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
    pub_channel = ch
    console.log("Publisher started")
    if (offlinePubQueue != null) {
      for (var i = 0; i < offlinePubQueue.length; i++) {
        publish(offlinePubQueue[i].queueName, offlinePubQueue[i].content)
      }
    }
    offlinePubQueue = []
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

function publish(queueName, content) {
  if (pub_channel != null) {
    try {
      pub_channel.assertQueue(queueName, { durable: false })
      pub_channel.sendToQueue(queueName, new Buffer(content))
      console.log("Message published to RMQ")
    }
    catch (exception) {
      console.error("Publisher Exception:- " + exception.message)
      offlinePubQueue.push({
        content: content,
        queueName: queueName
      })
    }
  }
}

function consume() {
  try {
    con_channel.consume(config.QUEUE_NAME_S3_SERVICE, function(message) {
      var jsonMessage = JSON.parse(message.content.toString())
      if (jsonMessage.action == config.ACTION_UPLOAD_FILE) {
        sendToS3(message, jsonMessage.destPath)
      } else if (jsonMessage.action == config.ACTION_UPDATE_FILE) {
        updateFile(message, jsonMessage.destPath, jsonMessage.fileId, jsonMessage.userId)
      } else {
        deleteFile(message, jsonMessage.fileId, jsonMessage.userId)
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
      "chunkPaths": {
        chunkPaths: chunkPaths
      }
    })
  })
  return deferred.promise
}

function search(oldChunkPaths, pathToFind) {
  for (var i = 0; i < oldChunkPaths.length; i++) {
    if (oldChunkPaths[i] == pathToFind) {
      return true;
    }
  }
  return false;
}

function compare(oldChunkPaths, newChunkPaths) {
  var chunkPaths = []
  var chunksToUpload = []
  for (var i = 0; i < newChunkPaths.length; i++) {
    chunkPaths.push(newChunkPaths[i])
    var isPresent = search(oldChunkPaths, newChunksPaths[i])
    if (!isPresent) {
      chunksToUpload.push({
        index: i,
        chunksPath: newChunkPaths[i],
        chunkHash: newChunkPaths[i].substring(newChunksPaths[i].lastIndexOf('/') + 1,
          newChunksPaths[i].lastIndexOf('.'))
      })
    }
  }
  return {
    chunkPaths: chunksPaths,
    chunksToUpload: chunksToUpload
  }
}

function uploadSpecificChunks(path, chunksToUpload) {
  var deferred = q.defer()
  fs.open(path, 'r', function(err, fd) {
    if (err) {
      console.error(err)
      deferred.reject(err)
    }
    var successCounts = 0
    for (var i = 0; i < chunksToUpload.length; i++) {
      q.fcall(function(chunkToUpload) {
        var deferred = q.defer()
        var buffer = new Buffer(config.READ_CHUNKSIZE)
        fs.read(fd, buffer, 0, config.READ_CHUNKSIZE, chunkToUpload.index * config.READ_CHUNKSIZE, function(err, nread) {
          if (err) {
            console.error(err)
            deferred.reject(err)
          }
          var data
          if (nread < config.READ_CHUNKSIZE) {
            data = buffer.slice(0, nread)
          } else {
            data = buffer
          }
          data = data.toString()
          uploadToS3(getS3Params(data, chunkToUpload.chunkPath), chunkToUpload.chunkHash)
          deferred.resolve({
            "status": 200
          })
        })
      }, chunksToUpload[i]).then(function(response) {
        if (response.status == 200) {
          successCounts += 1
        }
        if (i >= chunksToUpload.length) {
          if (successCounts == chunksToUpload.length) {
            deferred.resolve({
              "status": 200
            })
          } else {
            deferred.resolve({
              "status": 404
            })
          }
        }
      }).fail(function(err) {
        console.error(err)
      })
    }
  })
}

function uploadChunkPathFile(path, data) {
  var deferred = q.defer()
  var filePath = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.')) + ".json"
  fs.unlinkSync(path)
  fs.writeFile(filePath, JSON.stringify(data), function(err) {
    if (err) {
      console.error(err)
      deferred.reject(err)
    } else {
      uploadToS3(getS3Params(fs.createReadStream(filePath),
        "/uploads/files/" + filePath), "Chunk hash path file upload")
      fs.unlinkSync(filePath)
      deferred.resolve({
        "status": 200
      })
    }
  })
}

function deleteFile(message, fileId, userId) {
  S3.deleteObject(s3Pull.getS3ParamsForPull("/uploads/files/" + fileId + ".json"), function(err, data) {
    if (err) {
      console.error(err)
    } else {
      con_channel.ack(message)
      publish(config.QUEUE_NAME_NOTIFICATION, JSON.stringify({
        action: config.ACTION_SEND_NOTIF,
        fileId: fileId,
        userId: userId,
        message: "DELETE"
      }))
      console.log("File deleted")
    }
  })
}

function sendToS3(message, path) {
  createChunksAndProcess(path, true).then(function(response) {
    uploadChunkPathFile(path, response).then(function(response) {
      if (respone.status == 200) {
        con_channel.ack(message)
        console.log("File uploaded")
      }
    }).fail(function(err) {
      console.error(err)
    })
  }).fail(function(err) {
    console.error(err)
  })
}

function updateFile(message, path, fileId) {
  s3Pull.pullChunkPathFileFromS3(fileId).then(function(response) {
    if (response.status == 200) {
      createChunksAndProcess(path, false).then(function(data) {
        var opRes = compare(response.chunksPaths, data.chunksPaths)
        opRes.shares = response.shares
        uploadSpecificChunks(path, opRes.chunksToUpload).then(function(response) {
          if (response.status = 200) {
            console.log("Chunks updated")
            uploadChunkPathFile(path, {
              chunkPath: opRes.chunksPath,
              shares: opRes.shares
            }).then(function(response) {
              if (response.status = 200) {
                redisClient.set("/uploads/files/" + fileId + ".json", null)
                con_channel.ack(message)
                publish(config.QUEUE_NAME_NOTIFICATION, JSON.stringify({
                  action: config.ACTION_SEND_NOTIF,
                  fileId: fileId,
                  userId: userId,
                  message: "UPDATE"
                }))
                console.log("File updated")
              }
            }).fail(function(err) {
              console.error(err)
            })
          } else {
            //Modify to get the chunks that could not be uploaded and they retry for certain defined no. of times
            console.log("File not uploaded completely")
          }
        }).fail(function(err) {
          console.error(err)
        })
      }).fail(function(err) {
        console.error(err)
      })
    } else {
      //Storing them for the time being
      sendToS3(message, path)
    }
  }).fail(function(err) {
    console.error(err)
  })
}

app.listen(PORT, function() {
  connectToRMQ()
  console.log("Consumer service listening on :- " + PORT)
})
