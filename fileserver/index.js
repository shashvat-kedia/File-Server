const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const uploader = multer({ dest: '/uploads' });
const config = require('./config.js');
const amqp = require('amqplib/callback_api');
const app = express();
const server = require('http').createServer(app)
const io = require('socket.io')(server)

var rmq_connection = null
var pub_channel = null
var offlinePubQueue = []

const PORT = 8080 || process.env.PORT;

app.use(express.static(__dirname + "/uploads"))

app.use(bodyParser.urlencoded({ extended: true }))
app.use(bodyParser.json())
app.use(cors())

function getTimestampToAppend(req) {
  return "[" + Math.round((new Date()).getTime()) / 1000 + "] - " + req.body.content
}

function getDate() {
  var today = new Date()
  return today.getDay().toString().toUpperCase() + today.getMonth().toString().toUpperCase() +
    today.getFullYear().toString().toUppeCase()
}

function connectToRMQ() {
  amqp.connect(config.RMQ_URL, function(err, con) {
    if (err) {
      console.error("RMQ Error:- " + err.message)
      return setTimeout(connectToRMQ, 1000)
    }
    con.on("error", function(err) {
      if (err.message != "Connection closing") {
        console.error("RMQ Error:- " + err.message)
        throw err
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
    ch.assertQueue("jobs", { durable: false })
    pub_channel = ch
    console.log("Publisher started")
    for (var i = 0; i < offlinePubQueue.length; i++) {
      var content = offlinePubQueue[i]
      publish(content)
    }
    offlinePubQueue = []
  })
}

function publish(content) {
  try {
    pub_channel.sendToQueue(config.RMQ_NAME, new Buffer(content))
    console.log("Message published to RMQ")
  }
  catch (exception) {
    console.error("Publisher Exception:- " + exception.message)
    offlinePubQueue.push(content)
  }
}

app.use("*", function(req, res, next) {
  if (req.headers["authorization"] == config.API_KEY) {
    next()
  }
  else {
    res.status(303).json({
      "message": "Forbidden"
    })
  }
})

app.get("/", function(req, res) {
  console.log("Hit home")
  res.status(200).send({
    "message": "Hit"
  })
})

app.post("/text", function(req, res) {
  if (!fs.existsSync("/text/")) {
    fs.mkdir("./text/", function(err) {
      if (err) {
        console.log("Error while creating directory")
        console.error(err)
      }
      console.log("Directory created")
    })
  }
  fs.appendFile("./text/CP" + getDate() + "+.txt", getTimestampToAppend(req), function(err) {
    if (err) {
      console.log("Error while appending to file")
      console.error(err)
    }
    console.log("Data saved to file")
    res.status(200).send({
      "message": "Data appended to file"
    })
  })
})

app.post("/upload", uploader.single("file"), function(req, res) {
  const tempPath = req.file.path
  const destPath = path.join(__dirname + "/uploads/" + Math.round((new Date()).getTime()) / 1000 + "." + req.body.format);
  fs.rename(tempPath, destPath, function(err) {
    if (err) {
      console.error(err)
      res.status(500).send({
        "message": "Internal server error"
      })
    }
    res.statusCode = 200
    res.send({
      "message": "File upload successfull"
    })
    if (pub_channel != null) {
      publish(destPath)
    }
  })
})

app.use("*", function(req, res) {
  res.status(404).json({
    "message": "Endpoint does not exist"
  })
})

server.listen(PORT, function() {
  connectToRMQ()
  console.log("Listening to port: " + PORT)
})