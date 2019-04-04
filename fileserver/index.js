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
const mime = require('mime-types');
const jwt = require('jswonwebtoken');
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

function getFileFromRequest(req) {
  const tempPath = req.file.path
  const destPath = path.join(__dirname + "/uploads/" + Math.round((new Date()).getTime()) / 1000 + "." + req.body.format);
  fs.renameSync(tempPath, destPath)
  return destPath
}

function pullChunk(res, chunksToPull, rAF, firstByte, lastByte) {
  s3Pull.pullChunkFromS3(chunksToPull).then(function(response) {
    for (var j = 0; j < response.length; j++) {
      if (response[j].status != 200) {
        console.log("Error")
        res.status(422).json({
          "message": "File broken"
        })
        return
      }
      else {
        if (!fs.existsSync(rAF.filename)) {
          fs.writeFileSync(rAF.filename, "")
        }
        rAF.write(response[j].offset, Buffer.from(response[j].data), function(err) {
          if (err) {
            console.error(err)
          }
          if (j >= response.length - 1) {
            if (firstByte == -1) {
              res.statusCode = 200
              fs.createReadStream(rAF.filename).pipe(res)
            }
            else {
              res.statusCode = 206
              fs.createReadStream(rAF.filename, {
                start: firstByte,
                end: lastByte == -1 ? Infinity : parseInt(lastByte, 10)
              }).pipe(res)
            }
            fs.unlinkSync(rAF.filename)
          }
        })
      }
    }
  }).fail(function(err) {
    console.error(err)
  })
}

app.use("*", function(req, res, next) {
  if (req.headers["authorization"] != null) {
    var authorizationHeader = req.headers["authorization"]
    if (authorizationHeader.startsWith("Bearer:")) {
      var accessToken = authorizationHeader.substring(authorizationHeader.indexOf(':') + 1,
        authorizationHeader.length).trim()
      jwt.verify(accessToken,
        config.PRIVATE_KEY, {
          algorithms: ["RS512"],
          maxAge: config.JWT_EXP,
          clockTimestamp: new Date().getTime() / 1000
        }, function(err, payload) {
          if (err) {
            if (err.name == "TokenExpiredError") {
              res.status(400).json({
                message: "Access token expired"
              })
            } else if (err.name == "JSONWebTokenError") {
              res.status(400).json({
                message: "Malformed Access token"
              })
            }
          }
          req.accessToken = jwt.decoded(accessToken, { complete: true })
          next()
        })
    }
  }
  else {
    res.status(403).json({
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

app.get("/share/:fileId/:permission/:expTimestamp", function(req, res) {
  s3Pull.pullChunkPathFileFromS3(req.params.fileId).then(function(response) {
    if (response.status == 200) {
      var payload = {}
      if (req.params.expTimestamp != 0) {
        payload["exp"] = new Date() / 1000 + expTimestamp / 1000
      }
      if (req.params.permission = config.PERMISSION_READ) {
        payload['permissionType'] = config.PERMISION_READ
        const shareToken = jwt.sign(payload, config.PRIVATE_KEY, { algorithm: 'RS512' })
        response.fileData.shares.push(shareToken);
        res.status(200).json({
          "shareLink": "https://" + config.HOSTNAME + "/pull/" + req.params.fileId + "/" + shareToken
        })
      } else if (req.params.permission == config.PERMISSION_READ_WRITE) {
        payload['permissionType'] = config.PERMISSION_READ_WRITE
        const shareToken = jwt.sign(payload, config.PRIVATE_KEY, { algorithm: 'RS512' })
        response.fileData.shares.push(shareToken);
        res.status(200).json({
          "shareLink": "https://" + config.HOSTNAME + "/pull/" + req.params.fileId + "/" + shareToken
        })
      } else {
        res.status(422).json({
          message: "Invalid permission type"
        })
      }
    } else {
      res.status(response.status).json({
        message: response.message
      })
    }
  }).fail(function(err) {
    console.error(err)
  })
})

app.post("/upload", uploader.single("file"), function(req, res) {
  var destPath = getFileFromRequest(req)
  res.statusCode = 200
  res.send({
    "message": "File upload successfull"
  })
  publish(config.QUEUE_NAME_S3_SERVICE, JSON.stringify({
    action: config.ACTION_UPLOAD_FILE,
    destPath: destPath
  }))
})

app.use("/*/:shareToken", function(req, res) {
  var decoded = jwt.decode(req.params.sharaToken)
  jwt.verify(req.params.shareToken, config.PUBLIC_KEY, {
    algorithms: ['RS512'],
    maxAge: decoded.payload.exp,
    clockTimestamp: new Date().getTime() / 1000
  }, function(err, payload) {
    if (err) {
      if (err.name == "TokenExpiredError") {
        res.status(400).json({
          message: "Share link expired"
        })
      } else if (err.name == "JSONWebTokenError") {
        res.status(400).json({
          message: "Invalid share link"
        })
      } else {
        res.status(400).json({
          message: "Invalid share token"
        })
      }
    }
    s3Pull.pullChunkPathFileFromS3(req.params.fileId).then(function(response) {
      if (response.status == 200) {
        if (response.fileData.shares.indexOf(req.params.shareToken) > -1) {
          req.shareToken = req.params.payload
          next()
        } else {
          res.status(403).json({
            message: "Forbidden"
          })
        }
      }
    }).fail(function(err) {
      console.error(err)
    })
  })
})

app.put("/update/:fileId(|/:shareToken)", function(req, res) {
  var destPath = getFileFromRequest(req)
  res.statusCode = 200
  res.send({
    "message": "File updated"
  })
  publish(config.QUEUE_NAME_S3_SERVICE, JSON.stringify({
    action: config.ACTION_UPDATE_FILE,
    destPath: destPath,
    fileId: req.params.fileId
  }))
})

app.delete("/delete/:fileId(|/:shareToken)", function(req, res) {
  res.statusCode = 200
  res.send({
    "message": "File deleted"
  })
  publish(config.QUEUE_NAME_S3_SERVICE, JSON.stringify({
    action: config.ACTION_DELETE_FILE,
    fileId: req.params.fileId
  }))
})

app.get("/chunk/:fileId/:chunkId(|/:shareToken)", function(req, res) {
  s3Pull.pullChunkPathFileFromS3(req.params.fileId).then(function(response) {
    if (response.status == 200) {
      var chunkIndex = -1;
      for (var i = 0; i < response.fileData.chunkPaths.length; i++) {
        var chunkPath = response.fileData.chunkPaths[i]
        if (chunkPath.substring(chunkPath.lastIndexOf('/') + 1, chunkPath.indexOf('.')) == req.params.chunkId) {
          chunkIndex = i;
          break;
        }
      }
      if (chunkIndex != -1) {
        pullChunk(res, [response.fileData.chunkPaths[chunkIndex]],
          randomAccessFile(req.params.fileId + req.params.chunkId + ".txt"), -1, -1)
      }
      else {
        res.status(404).json({
          "message": "Chunk not found"
        })
      }
    }
    else {
      res.status(response.status).json({
        "message": response.message
      })
    }
  }).fail(function(err) {
    console.error(err)
  })
})

app.head("/pull/:fileId(|/:shareToken)", function(req, res) {
  s3Pull.pullChunkPathFileFromS3(req.params.fileId).then(function(response) {
    if (response.status == 200) {
      s3Pull.getFileLength(response.fileData.chunkPaths[response.fileData.chunkPaths.length - 1])
        .then(function(contentLengthLastChunk) {
          if (contentLengthLastChunk < 0) {
            res.status(422).json({
              "message": "File broken"
            })
          }
          else {
            res.set({
              "Accept-Ranges": "bytes",
              "Content-Type": mime.lookup(response.chunkPaths[0]),
              "Content-Length": config.READ_CHUNKSIZE * (response.fileData.chunkPaths.length - 2) + contentLengthLastChunk,
              "ETag": response.etag,
              "LastModified": response.lastModified,
              "Connection": "close"
            })
            res.end()
          }
        })
    }
    else {
      res.status(response.status).json({
        "message": response.message
      })
    }
  }).fail(function(err) {
    console.error(err)
  })
})

//Still have to deal with fetching partial file for the first and last chunk instead of full file from S3

app.get("/pull/:fileId(|/:shareToken)", function(req, res) {
  s3Pull.pullChunkPathFileFromS3(req.params.fileId).then(function(response) {
    if (response.status == 200) {
      var lastPos = response.fileData.chunkPaths.length - 1
      var chunksToPull = []
      var firstByte = -1
      var lastByte = -1
      if (req.headers["range"] != null) {
        var rangeHeader = req.headers["range"]
        firstByte = parseInt(rangeHeader.substring(rangeHeader.indexOf('=') + 1, rangeHeader.indexOf('-')), 10)
        var firstPos = Math.floor(firstByte / config.READ_CHUNKSIZE) + 1
        if (!(rangeHeader[rangeHeader.length - 1] == '-')) {
          lastByte = parseInt(rangeHeader.substring(rangeHeader.indexOf('-') + 1, rangeHeader.length), 10)
          lastPos = Math.ceil(lastByte / config.READ_CHUNKSIZE)
        }
        for (var i = firstPos; i <= lastPos; i++) {
          chunksToPull.push(response.fileData.chunkPaths[i])
        }
        if (lastByte != -1 && lastByte < firstByte) {
          res.status(422).json({
            "message": "Invalid range request"
          })
          return
        }
        firstByte = firstByte - (firstPos - 1) * config.READ_CHUNKSIZE
      }
      else {
        chunksToPull = response.fileData.chunkPaths.slice(1, response.length)
      }
      pullChunk(res, chunksToPull,
        randomAccessFile(req.params.fileId + response.fileData.chunkPaths[0]), firstByte, lastByte)
    }
    else {
      res.status(response.status).json({
        "message": response.message
      })
    }
  }).fail(function(err) {
    console.error(err)
  })
})

app.head("/pull/:fileId/:shareToken", function(req, res) {

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