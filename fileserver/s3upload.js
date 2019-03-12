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

AWS.config.update(config.AWS_CONFIG)

const S3 = new AWS.S3()
const s3_params = config.S3_CONFIG

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
    ch.assertQueue(config.RMQ_NAME, { durable: false })
    con_channel = ch
    console.log("Consumer started")
    consume()
  })
}

function consume() {
  try {
    con_channel.consume(config.RMQ_NAME, function(message) {
      console.log(message.content.toString())
      sendToS3(message.content.toString())
    }, { noAck: true })
  }
  catch (exception) {
    console.error("Consumer Exception:- " + exception.message)
  }
}

function sendToS3(path) {
  var buffer = new Buffer(config.READ_CHUNKSIZE)
  var readStream = fs.createReadStream(path, { highWaterMark: config.READ_CHUNKSIZE })
  readStream.on('data', function(chunk) {
    var chunk_hash = hash(chunk)
    s3_params["Body"] = fc.createReadStream(chunk)
    s3_params["Key"] = "/chunks/" + chunk_hash + ".txt"
    s3.upload(s3_params, function(err, data) {
      if (err) {
        console.error("S3 Upload Error:- " + err)
        throw err
      }
      if (data) {
        console.log("File chunk successfully uploaded to AWS S3:- " + data.location)
        fs.appendFile("/uploads/files/" + path.substring(path.lastIndexOf('/') + 1, path.lastIndexOf('.')),
          chunk_hash, function(err) {
            if (err) {
              console.error("File Write Error:- " + err)
              throw err
            }
          })
      }
    })
    console.log(hash(chunk))
  })
  fs.unlinkSync(path)
}

app.listen(PORT, function() {
  connectToRMQ()
  console.log("Consumer service listening on :- " + PORT)
})