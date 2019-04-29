const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcrypt');
const speakeasy = require('speakeasy');
const mongo = require('./mongo.js');
const config = require('./config.js');
const q = require('q');
const amqp = require('amqplib/callback_api');
const grpc = require('grpc');
const LevelDBService = grpc.load(config.LEVEL_DB_OBJ_PROTO_PATH).LevelDBService;
const grpcClient = new LevelDBService(config.HOST_NAME + ":" + config.LEVEL_DB_GRPC_PORT, grpc.credentials.createInsecure());
const app = express()

//Handle logout (For this support to blacklist JWT has to be added which is not so easy to perform in a distributer enviornment 
//thus to verify weather each JWT is valid or not DB will have to be accessed thus increasing the overall time to process a request
//this sort of goes againt the reason why we use JWT that is stateless authentication) 

var rmq_connection = null
var pub_channel = null
var offlinePubQueue = []

app.use(bodyParser.urlencoded({ extended: true }))
app.use(bodyParser.json())
app.use(cors())

const PORT = 27327 || process.env.PORT

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

function generatePasswordHash(password, salt) {
  var passSalt = salt
  if (passSalt == null) {
    salt = bcrypt.genSalt(config.SALT_ROUNDS).then(function(response) {
      passSalt = response
    }).fail(function(err) {
      console.error(err)
    })
  }
  return bcrypt.hash(password, passSalt)
}

function generateJWT(payload) {
  return jwt.sign(payload, config.PRIVATE_KEY)
}

function generateRefreshToken(id) {
  var deferred = q.defer()
  var refreshToken = generateJWT({
    userId: id,
    exp: config.REFRESH_JWT_EXP
  })
  mongo.saveRefreshToken(refreshToken).then(function(tokenSaved) {
    if (tokenSaved) {
      deferred.resolve({
        status: 200,
        refreshToken: refreshToken
      })
    } else {
      deferred.resolve({
        status: 400,
        message: "Unable to generate Refresh Token retry."
      })
    }
  }).fail(function(err) {
    deferred.reject(err)
  })
  return deferred.promise
}

function isValid(username, password) {
  return true
}

function getLevelDBObject(key) {
  const deferred = q.defer()
  grpcClient.get(key, function(err, object) {
    if (err) {
      deferred.reject(err)
    }
    deferred.resolve(object.content.jwt)
  })
  return deferred.promise
}

function putLevelDbObject(key, val) {
  const deferred = q.defer()
  grpcClient.put({
    key: key,
    content: { jwt: val }
  }, function(err, val) {
    if (err) {
      deferred.reject(err)
    }
    deferred.resolve(true)
  })
  return deferred.promise
}

app.post("/signup", function(req, res) {
  if (isValid(req.body.username, req.body.password)) {
    generatePasswordHash(req.body.password, null).then(function(passwordHash) {
      mongo.insertAuthCredentials({
        username: req.body.username,
        salt: salt,
        passwordHash: passwordHash
      }).then(function(response) {
        if (response.status == 200) {
          generateRefreshToken(response.id).then(function(result) {
            if (result.status == 200) {
              res.status(200).json({
                accessToken: generateJWT({
                  userId: response.id,
                  exp: config.JWT_EXP
                }),
                refreshToken: result.refreshToken,
                tokenType: "JWT"
              })
            } else {
              res.status(result.status).json({
                message: result.message
              })
            }
          }).fail(function(err) {
            console.error(err)
          })
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
  }
})

app.post("/login", function(req, res) {
  if (isValid(req.body.username, req.body.password)) {
    mongo.getAuthCredentials(req.body.username).then(function(response) {
      if (response.status == 200) {
        generatePassswordHash(req.body.password, response.credentials.salt).then(function(passwordHash) {
          if (response.credentials.passwordHash == passwordHash) {
            console.log(response.credentials.id)
            generateRefreshToken(response.credentials.id).then(function(result) {
              if (result.status == 200) {
                res.status(200).json({
                  accessToken: generateJWT({
                    userId: response.credentials.id,
                    exp: config.JWT_EXP
                  }),
                  refreshToken: result.refreshToken,
                  tokenType: "JWT"
                })
              } else {
                res.status(result.status).json({
                  message: result.message
                })
              }
            }).fail(function(err) {
              console.error(err)
            })
          } else {
            res.status(403).json({
              message: "Invalid credentials"
            })
          }
        }).fail(function(err) {
          console.error(err)
        })
      } else {
        res.json(response.status).json({
          message: response.message
        })
      }
    }).fail(function(err) {
      console.error(err)
    })
  }
})

app.get("/accesstoken/:token", function(req, res) {
  mongo.fetchRefreshToken(req.params.token).then(function(response) {
    if (response.status == 200) {
      jwt.verify(response.refreshToken,
        config.PRIVATE_KEY, {
          maxAge: config.REFRESH_JWT_EXP,
          clockTimestamp: new Date().getTime() / 1000
        }, function(err, payload) {
          mongo.deleteRefreshToken(response.refreshToken).then(function(isDeleted) {
            if (isDeleted) {
              if (err) {
                if (err.name == "TokenExpiredError") {
                  res.status(400).json({
                    message: "Refresh token expired"
                  })
                } else if (err.name == "JSONWebTokenError") {
                  res.status(400).json({
                    message: "Malformed Refresh token"
                  })
                } else {
                  res.status(400).json({
                    message: "Invalid Refresh token"
                  })
                }
              } else {
                res.status(200).json({
                  accessToken: generateJWT({
                    userId: payload.userId,
                    exp: config.JWT_EXP
                  }),
                  tokenType: "JWT"
                })
              }
            } else {
              res.status(400).json({
                message: "Invalid Refresh token"
              })
            }
          }).fail(function(err) {
            console.error(err)
          })
        })
    } else {
      res.status(response.status).json({
        message: response.message
      })
    }
  }).fail(function(err) {
    console.log(err)
  })
})

function clearAuthTokens(userId) {

}

app.post("/password/change", function(req, res) {
  mongo.getAuthCredentials(req.body.userId).then(function(response) {
    if (response.status == 200) {
      generatePasswordHash(req.body.password, response.credentials.salt).then(function(passwordHash) {
        if (response.credentials.passwordHash == passwordHash) {
          generatePasswordHash(req.body.newPassword, null).then(function(newPasswordHash) {
            mongo.updateAuthCredentials(req.body.userId, newPasswordHash, newSalt).then(function(response) {
              if (response.status == 200) {
                clearAuthTokens(req.body.userId)
                res.status(200).json({
                  message: "Password updated"
                })
                //Invalidate all JWT for this account
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
        } else {
          res.status(401).json({
            message: "Incorrect password"
          })
        }
      }).fail(function(err) {
        console.error(err)
      })
    }
  }).fail(function(err) {
    console.error(err)
  })
})

app.post("/logout/:token", function(req, res) {

})

/*app.use("*", function(req, res, next) {
  if (req.headers["authorization"] != null) {
    var authorizationHeader = req.headers["authorization"]
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
          }
        }
        req.accessToken = jwt.decode(accessToken, { complete: true })
        next()
      })
  } else {
    res.json(401).json({
      "message": "Unauthorized"
    })
  }
})*/

//Add support for 2FA using TOTP here

app.listen(PORT, function() {
  console.log("Listening to port:- " + PORT);
})