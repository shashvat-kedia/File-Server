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
    })
    ch.on("close", function(err) {
      console.error("RMQ Error:- " + err)
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
  fs.open(path, 'r', function(err, fd) {
    if (err) {
      console.error("File Read Error:- " + err)
      throw err
    }
    function readNextChunk() {
      fs.read(fd, buffer, 0, config.READ_CHUNKSIZE, null, function(err, nread) {
        if (err) {
          console.error("Read Chunk Error:- " + err)
          throw err
        }
        if (nread == 0) {
          fs.close(nread, function(err) {
            console.error("Read Stream Error:- " + err)
            throw err
          })
        }
        var data
        if (nread < config.READ_CHUNKSIZE) {
          data = buffer.slice(0, nread)
        }
        else {
          data = buffer
        }
        console.log(hash(nread))
      })
    }
    readNextChunk()
  })
  fs.unlinkSync(path)
}

app.listen(PORT, function() {
  connectToRMQ()
  console.log("Consumer service listening on :- " + PORT)
})