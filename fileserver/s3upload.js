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
const server = require('http').createServer(app);
const io = require('socket.io')(server);

AWS.config.update(config.AWS_CONFIG)

const S3 = new AWS.S3()

var rmq_connection = null
var con_channel = null
var pub_channel = null
var pubQueue = []

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
    for (var i = 0; i < offlinePubQueue.length; i++) {
      publish(offlinePubQueue[i].queueName, offlinePubQueue[i].content)
    }
    offlinePubQueue = []
  })
}

function publish(queueName, content) {
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
    ch.assertQueue(config.QUEUE_NAME_S3_UPLOAD, { durable: false })
    con_channel = ch
    console.log("Consumer started")
    consume()
  })
}

function consume() {
  try {
    con_channel.consume(config.QUEUE_NAME_S3_UPLOAD, function(message) {
      console.log(message.content.toString())
      var jsonMessage = JSON.parse(message.content.toString())
      if (jsonMessage.action == config.ACTION_FILE_UPLOAD) {
        sendToS3(jsonMessage.destPath)
      }
      else {
        pullFromS3(jsonMessage.fileId, jsonMessage.socketId)
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

function uploadToS3(s3_params) {
  S3.upload(s3_params, function(err, data) {
    if (err) {
      console.error("S3 Upload Error:- " + err)
      throw err
    }
    if (data) {
      console.log("File chunk successfully uploaded to AWS S3:- " + data.location)
    }
  })
}

function pullFromS3(fileId, socketId) {
  //Decision to be made weather to use notification service or not
}

function sendToS3(path) {
  var readStream = fs.createReadStream(path, { highWaterMark: config.READ_CHUNKSIZE })
  var file_path = "/files/" + path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.')) + ".txt"
  readStream.on('data', function(chunk) {
    var chunk_hash = hash(chunk)
    var chunk_path = "/chunks/" + chunk_hash + ".txt"
    uploadToS3(getS3Params(fs.createReadStream(chunk), chunk_path))
    fs.appendFile(file_path, chunk_path, function(err) {
      if (err) {
        console.error("File Write Error:- " + err)
        throw err
      }
    })
  })
  uploadToS3(getS3Params(fs.createReadStream(file_path),
    "/uploads/files/" + path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.')) + ".txt"))
  fs.unlinkSync(path)
  fs.unlinkSync(file_path)
  console.log("File upload procedure complete")
}

server.listen(PORT, function() {
  connectToRMQ()
  console.log("Consumer service listening on :- " + PORT)
})