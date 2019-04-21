const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const config = require('./config.js');
const s3Pull = require('./s3Pull.js');
const amqp = require('amqplib/callback_api');
const hash = require('object-hash');
const randomAccessFile = require('random-access-file');
const mime = require('mime-types');
const jwt = require('jsonwebtoken');
const app = express();

var rmq_connection = null
var pub_channel = null
var offlinePubQueue = []

//Decide on different READ_CHUNKSIZE for different file formats

const PORT = 8080 || process.env.PORT;
const STORAGE_TYPE_MEMORY = 1;
const STORAGE_TYPE_DISK = STORAGE_TYPE_MEMORY + 1;

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

function pullChunk(res, chunksToPull, rAF, firstByte, lastByte) {
  s3Pull.pullChunkFromS3(chunksToPull).then(function(response) {
    for (var j = 0; j < response.length; j++) {
      if (response[j].status != 200) {
        console.log("Error")
        res.status(422).json({
          "message": "File broken"
        })
        return
      } else {
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
            } else {
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

function checkConditions(conditionHeaders, etag, lastModified) {
  var passed = true
  var message = ""
  if (conditionHeaders["if-match"] != null) {
    if (etag != conditionHeaders["if-match"]) {
      message = "ETag value different as compared to value specified in header"
      passed = passed && false;
    }
  }
  if (conditionHeaders["if-none-match"] != null) {
    var etags = conditionHeaders["if-not-match"].split(',')
    if (etags.length > 0) {
      for (var i = 0; i < etags.length; i++) {
        if (etags[i] == etag) {
          passed = passed && false
          message = "ETag value found in a part of list specified by the header"
          break
        }
      }
    }
  } else if (conditionHeaders["if-modified-since"] != null) {
    if (new Date(conditionHeaders["if-modified-since"]).getTime() > lastModified) {
      passed = passed && false
      message = "Resource: " + etag + " modified before date in header"
    }
  }
  if (conditionHeaders["if-unmodified-since"] != null) {
    if (new Date(conditionHeaders["if-unmodified-since"]).getTime() < lastModified) {
      passed = passed && false
      message = "Resouce: " + etag + "modified after date in header"
    }
  }
  if (conditionHeaders["last-modified"] != null) {
    if (new Date(conditionHeaders["last-modified"]).getTime() != lastModified) {
      passed = passed && false
      message = "Resouce: " + etag + "wrong last modified date in header"
    }
  }
  return {
    isValid: passed,
    message: message
  }
}

function ifRangeConditionCheck(ifRangeHeader, etag, lastModified) {
  var parameters = ifRangeHeader.split(',')
  conditionKey = ""
  if (parameters.length == 1) {
    conditionKey = "if-match"
  } else if (parameters.length > 0) {
    conditionKey = "last-modified"
  } else {
    return false
  }
  conditionHeader[conditionKey] = parameters[0]
  return checkConditions(conditionHeader, etag, lastModified).isValid
}

function getConditionHeadersFromReq(req) {
  conditionHeaders = {}
  if (req.headers["if-range"] != null) {
    conditionHeaders["if-range"] = req.headers["if-range"]
    return condHeaders
  }
  if (req.headers["if-match"] != null) {
    conditionHeaders["if-match"] = req.headers["if-match"]
  }
  if (req.headers["if-none-match"] != null) {
    conditionHeaders["if-none-match"] = req.headers["if-none-match"]
  }
  if (req.headers["if-modified-since"] != null) {
    conditionHeaders["if-modified-since"] = req.headers["if-modified-since"]
  }
  if (req.headers["if-unmodified-since"] != null) {
    conditionHeaders["if-unmodified-since"] = req.headers["if-unmodified-since"]
  }
  return conditionHeaders
}

function getUploaderFromReq(req) {
  var storageType = STORAGE_TYPE_DISK
  var storage = null
  if (req.body.fileSize != null && req.body.fileSize > config.FILE_SIZE_LARGE) {
    storageType = STORAGE_TYPE_MEMORY
    storage = new multer.memoryStorage()
  } else {
    storage = new multer.diskStorage({
      destination: function(req, file, callbackl) {
        callback(null, "/uploads")
      },
      filename: function(req, file, callback) {
        callback(null, Math.round((new Date()).getTime()) / 1000 + file.originalName.substring(file.originalName.lastIndexOf('.')))
      }
    })
  }
  return {
    uploader: multer({
      storage: storage,
      fileFilter: uploadFileFilter
    }).single("file"),
    storageType: storageType
  }
}

function handleUploadedFile(req, res, actionType) {
  var deferred = q.defer()
  var uploaderOb = getUploaderFromReq(req)
  uploaderOb.uploader(req, res, function(err) {
    if (err == multer.MulterError) {
      console.log("Multer error: ")
      deferred.reject(err)
    } else if (err) {
      deferred.reject(err)
    } else {
      var message = {
        action: actionType
      }
      if (uploaderOb.storageType == STORAGE_TYPE_DISK) {
        message.destPath = req.file.path
      } else {
        message.dataBuffer = req.file.buffer
        message.fileId = req.file.originalName.substring(req.file.originalName.lastIndexOf('.') + 1)
      }
      if (actionType == config.ACTION_UPDATE_FILE) {
        message.fileId = req.params.fileId
        message.userId = req.accessToken.payload.userId
      }
      deferred.resolve(message)
    }
  })
  return deferred.promise
}

function uploadFileFilter(req, file, callback) {
  if (config.FILE_FORMAT_BLACKLIST.indexOf(file.originalName.substring(file.originalName.lastIndexOf('.') + 1)) > -1) {
    callback(null, false)
  } else {
    callback(null, true)
  }
}

app.use("*", function(req, res, next) {
  if (req.headers["authorization"] != null) {
    var authorizationHeader = req.headers["authorization"]
    if (authorizationHeader.startsWith("Bearer:")) {
      var accessToken = authorizationHeader.substring(authorizationHeader.indexOf(':') + 1,
        authorizationHeader.length).trim()
      jwt.verify(accessToken,
        config.PRIVATE_KEY, {
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
            } else {
              res.status(400).json({
                message: "Invalid Access token"
              })
            }
          }
          req.accessToken = jwt.decode(accessToken, { complete: true })
          next()
        })
    } else {
      res.status(400).json({
        message: "Invalid token"
      })
    }
  } else {
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
    userId: req.accessToken.payload.userId,
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
      var payload = {
        fileId: req.params.fileId
      }
      if (req.params.expTimestamp != 0) {
        payload.exp = Math.floor(new Date().getTime() / 1000) + expTimestamp / 1000
      }
      if (req.params.permission = config.PERMISSION_READ || req.params.permission == config.PERMISSION_READ_WRITE) {
        payload['permissionType'] = req.params.permission == config.PERMISION_READ ?
          config.PERMISION_READ : config.PERMISSION_READ_WRITE
        const shareToken = jwt.sign(payload, config.PRIVATE_KEY)
        response.fileData.shares.push(shareToken);
        res.status(200).json({
          shareLink: "https://" + config.HOSTNAME + "/pull/" + req.params.fileId + "/" + shareToken,
          shareToken: shareToken
        })
        publish(config.QUEUE_NAME_S3_SERVICE, JSON.stringify({
          action: config.ACTION_CHUNK_PATH_FILE_UPDATE,
          data: response.fileData
        }))
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

app.get("/shares/:fileId", function(req, res) {
  s3Pull.pullChunkPathFileFromS3(req.params.fileId).then(function(response) {
    if (response.status == 200) {
      res.status(200).json({
        shares: response.fileData.shares
      })
    } else {
      res.status(response.status).json({
        message: response.message
      })
    }
  }).fail(function(err) {
    console.error(err)
  })
})

app.get("/chunks/:fileId", function(req, res) {
  s3Pull.pullChunkPathFileFromS3(req.params.fileId).then(function(response) {
    if (response.status == 200) {
      res.status(200).json({
        chunks: response.fileData.chunkPaths.map(function(item) {
          return item.substring(item.lastIndexOf('/' + 1, item.lastIndexOf('.')))
        })
      })
    } else {
      res.status(response.status).json({
        message: response.message
      })
    }
  }).fail(function(err) {
    console.error(err)
  })
})

app.post("/upload", function(req, res) {
  handleUploadedFile(req, res, config.ACTION_UPLOAD_FILE).then(function(message) {
    res.statusCode = 200
    res.send({
      "message": "File upload successfull"
    })
    publish(config.QUEUE_NAME_S3_SERVICE, JSON.stringify(message))
  }).fail(function(err) {
    console.error(err)
  })
})

app.use("/pull/:fileId/:shareToken", function(req, res) {
  var decoded = jwt.decode(req.params.shareToken)
  jwt.verify(req.params.shareToken, config.PRIVATE_KEY, {
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
        if (response.fileData.shares.indexOf(req.params.shareToken) > -1 && response.fileId == req.params.fileId) {
          if (req.method != "HEAD" && payload.permissionType == config.PERMISION_READ && req.path != "/pull") {
            res.status(403).json({
              message: "Forbidden"
            })
          } else {
            req.shareToken = req.params.payload
            next()
          }
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

app.put("/update/token/:shareToken/:permissionType", function(req, res) {
  s3Pull.pullChunkPathFileFromS3(req.shareToken.payload.fileId).then(function(response) {
    if (response.status == 200) {
      var newShareToken = req.shareToken
      var newPayload = req.shareToken.payload
      if (req.params.permissionType != req.shareToken.payload.permissionType) {
        newPayload.permissionType = req.params.permissionType
        response.fileData.shares = response.fileData.shares.filter(function(item) {
          return item != req.shareToken
        })
        newShareToken = jwt.sign(newPayload, config.PRIVATE_KEY)
        response.fileData.shares.push(newShareToken)
        publish(config.QUEUE_NAME_S3_SERVICE, JSON.stringify({
          action: config.ACTION_CHUNK_PATH_FILE_UPDATE,
          data: response.fileData
        }))
      }
      res.status(200).json({
        shareToken: newShareToken
      })
    } else {
      res.status(response.status).json({
        message: response.message
      })
    }
  }).fail(function(err) {
    console.error(err)
  })
})

app.delete("/delete/token/:shareToken", function(req, res) {
  s3Pull.pullChunkPathFileFromS3(req.shareToken.payload.fileId).then(function(response) {
    if (response.status == 200) {
      response.fileData.shares = response.fileData.shares.filter(function(item) {
        return item != req.shareToken
      })
      res.status(200).json({
        message: "Share token deleted"
      })
      publish(config.QUEUE_NAME_S3_SERVICE, JSON.stringify({
        action: config.ACTION_CHUNK_PATH_FILE_UPDATE,
        data: response.fileData
      }))
    } else {
      res.status(response.status).json({
        message: response.message
      })
    }
  }).fail(function(err) {
    console.error(err)
  })
})

app.put("/update/:fileId(|/:shareToken)", function(req, res) {
  handleUploadedFile(req, res, config.ACTION_UPDATE_FILE).then(function(message) {
    s3Pull.pullChunkPathFileFromS3(req.params.fileId).then(function(response) {
      if (response.status == 200) {
        message.fileData = response.fileData
        res.status(200).json({
          message: "File updated"
        })
        publish(config.QUEUE_NAME_S3_SERVICE, JSON.stringify(message))
      } else {
        res.status(response.status).json({
          message: response.message
        })
      }
    }).fail(function(err) {
      console.error(err)
    })
  }).fail(function(err) {
    console.error(err)
  })
})

app.delete("/delete/:fileId(|/:shareToken)", function(req, res) {
  res.statusCode = 200
  res.send({
    "message": "File deleted"
  })
  publish(config.QUEUE_NAME_S3_SERVICE, JSON.stringify({
    action: config.ACTION_DELETE_FILE,
    fileId: req.params.fileId,
    userId: req.accessToken.payload.userId,
    s3Params: s3Pull.getS3ParamsForPull("/uploads/files/" + req.params.fileId + ".json")
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
      } else {
        res.status(404).json({
          "message": "Chunk not found"
        })
      }
    } else {
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
          } else {
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
    } else {
      res.status(response.status).json({
        "message": response.message
      })
    }
  }).fail(function(err) {
    console.error(err)
  })
})

app.get("/pull/:fileId(|/:shareToken)", function(req, res) {
  s3Pull.pullChunkPathFileFromS3(req.params.fileId).then(function(response) {
    if (response.status == 200) {
      conditionHeaders = getConditionHeadersFromReq(req)
      var validation = { isValid: true }
      if (Object.keys(conditionHeaders).length > 0 && conditionHeaders["if-range"] == null) {
        validation = checkConditions(conditionHeaders, response.etag, new Date(response.lastModified).getTime())
      }
      if (validation.isValid) {
        var lastPos = response.fileData.chunkPaths.length - 1
        var chunksToPull = []
        var firstByte = -1
        var lastByte = -1
        if ((req.headers["range"] != null && conditionHeaders["if-range"] == null) || (req.headers["range"] != null &&
          ifRangeConditionCheck(conditionHeaders["if-range"], response.etag, new Date(response.lastModified).getTime()))) {
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
        } else {
          chunksToPull = response.fileData.chunkPaths.slice(1, response.length)
        }
        pullChunk(res, chunksToPull,
          randomAccessFile(req.params.fileId + response.fileData.chunkPaths[0]), firstByte, lastByte)
      } else {
        res.status(412).json({
          message: validation.message
        })
      }
    } else {
      res.status(response.status).json({
        "message": response.message
      })
    }
  }).fail(function(err) {
    console.error(err)
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
