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

function sendToS3(path) {
  var filePath = path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.')) + ".txt"
  createChunksAndProcess(path, true).then(function(response) {
    fs.unlinkSync(path)
    fs.writeFile(filePath, response.chunkPaths.toString(), function(err) {
      if (err) {
        console.error(err)
      }
      else {
        uploadToS3(getS3Params(fs.createReadStream(filePath),
          "/uploads/files/" + filePath), "Chunk hash path file upload")
        fs.unlinkSync(filePath)
      }
    })
  }).fail(function(err) {
    console.error(err)
  })
}

function updateFile(path, fileId) {
  s3Upload.pullChunkPathFileFromS3(fileId).then(function(response) {
    if (response.status == 200) {

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