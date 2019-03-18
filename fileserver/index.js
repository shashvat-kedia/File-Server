const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const uploader = multer({ dest: '/uploads' });
const config = require('./config.js');
const s3Pull = require('./s3Pull.js');
const amqp = require('amqplib/callback_api');
const hash = require('object-hash');
const randomAccessFile = require('random-access-file');
const app = express();

var rmq_connection = null
var pub_channel = null
var offlinePubQueue = []

//Decide on different READ_CHUNKSIZE for different file formats

const PORT = 8080 || process.env.PORT;

app.use(express.static(__dirname + "/uploads"))

app.use(bodyParser.urlencoded({ extended: true }))
app.use(bodyParser.json())
app.use(cors())

function getTimestamp() {
  return Math.round((new Date()).getTime()) / 1000
}

function getTimestampToAppend(req) {
  return "[" + getTimestamp() + "] - " + req.body.content
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

app.get("/:socketId", function(req, res) {
  publish(config.QUEUE_NAME_NOTIFICATION, JSON.stringify({
    channel: "message",
    message: "You have hit home",
    socketId: req.params.socketId
  }))
  console.log("Welcome message sent")
  res.status(200).json({
    "message": "Message sent"
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
      publish(config.QUEUE_NAME_S3_SERVICE, JSON.stringify({
        action: config.ACTION_UPLOAD_FILE,
        destPath: destPath
      }))
    }
  })
})

//Multithreaded download to be supported here

app.head("/pull/:fileId", function(req, res) {
  res.set({
    "Accept-Ranges": "bytes",
    "Content-Length": 
  })
})

app.get("/pull/:fileId", function(req, res) {
  s3Pull.getFileMetadata(req.params.fileId).then(function(response) {
    if (response != null) {
      s3Pull.pullChunkPathFileFromS3(req.params.fileId).then(function(response) {
        if (response.length > 0) {
          var rAF = randomAccessFile(req.params.fileId + response[0])
          s3Pull.pullChunkFromS3(response.slice(1, response.length)).then(function(response) {
            for (var j = 0; j < response.length; j++) {
              if (response[j].status != 200) {
                //Unsuccessfull
                return
              }
              else {
                if (!fs.existsSync(rAF.filename)) {
                  fs.writeFileSync(rAF.filename, "", function(err) {
                    if (err) {
                      console.error(err)
                    }
                  })
                }
                rAF.write(response[j].offset, Buffer.from(response[j].data), function(err) {
                  if (err) {
                    console.error(err)
                  }
                  if (j >= response.length - 1) {
                    fs.createReadStream(rAF.filename).pipe(res)
                    fs.unlinkSync(rAF.filename)
                  }
                })
              }
            }
          }).fail(function(err) {
            console.error(err)
          })
        }
        else {
          res.status(422).json({
            "message": "Broken file"
          })
        }
      }).fail(function(err) {
        console.error(err)
      })
    }
    else {
      res.status(404).json({
        "message": "File Not Found"
      })
    }
  }).fail(function(err) {
    console.log(err)
  })
})

app.use("*", function(req, res) {
  res.status(404).json({
    "message": "Endpoint does not exist"
  })
})

app.listen(PORT, function() {
  connectToRMQ()
  console.log("Listening to port: " + PORT)
})