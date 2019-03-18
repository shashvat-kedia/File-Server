const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const config = require('./config.js');
const amqp = require('amqplib/callback_api');
const hash = require('object-hash');
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
  try {
    con_channel.consume(config.QUEUE_NAME_S3_SERVICE, function(message) {
      var jsonMessage = JSON.parse(message.content.toString())
      if (jsonMessage.action == config.ACTION_UPLOAD_FILE) {
        sendToS3(jsonMessage.destPath)
      }
      else {
        pullFromS3(getS3ParamsForPull(jsonMessage.fileId), jsonMessage.socketId)
      }
    }, { noAck: true })
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

function getS3ParamsForPull(fileId) {
  s3_params = config.S3_CONFIG
  s3_params["Key"] = "/uploads/files/" + fileId + ".txt"
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

function fileExistsOnS3(s3_params) {
  S3.headObject(s3_params, function(err, metadata) {
    if (err && err.code == 'Not Found') {
      console.log(err)
      return false
    }
    console.log(metadata)
    return true
  })
}

function pullFromS3(s3_params, socketId) {
  if (fileExistsOnS3(s3_params)) {
    S3.getObject(s3_params, function(err, data) {
      if (err) {
        console.error(err)
      }
      console.log(data)
    })
  }
  else {
  }
}

function sendToS3(path) {
  chunk_paths = []
  var readStream = fs.createReadStream(path, { highWaterMark: config.READ_CHUNKSIZE })
  var file_path = "files/" + path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.')) + ".txt"
  readStream.on('data', function(chunk) {
    var chunk_hash = hash(chunk)
    var chunk_path = "/chunks/" + chunk_hash + ".txt"
    chunk_paths.push(chunk_path)
    uploadToS3(getS3Params(chunk, chunk_path), chunk_hash)
  })
  readStream.on('close', function() {
    fs.unlinkSync(path)
    fs.writeFile(file_path, chunk_paths.toString(), function(err) {
      if (err) {
        console.error(err)
      }
      else {
        uploadToS3(getS3Params(fs.createReadStream(file_path),
          "/uploads/files/" + file_path), "Chunk hash path file upload")
        fs.unlinkSync(file_path)
      }
    })
  })
}

app.listen(PORT, function() {
  connectToRMQ()
  console.log("Consumer service listening on :- " + PORT)
})