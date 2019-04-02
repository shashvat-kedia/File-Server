const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const speakeasy = require('speakeasy');
const mongo = require('./mongo.js');
const app = express()

app.use(bodyParser.urlencoded({ extended: true }))
app.use(bodyParser.json())
app.use(cors())

const PORT = 27327 || process.env.PORT

function getSalt(length) {
  return crypto.randomBytes(Math.ceil(length / 2)).toString('hex').slice(0, length)
}

function sha512(password, salt) {
  var hash = crypto.createHmac('sha512', salt)
  hash.update(password)
  var value = hash.digest('hex');
  return {
    salt: salt,
    passwordHash: value
  }
}

function generateJWT(payload) {
  return jwt.sign(payload, config.PRIVATE_KEY, { algorithm: 'RS512' })
}

function generateRefreshToken(id) {
  var deferred = q.defer()
  var refreshToken = generateJWT({
    userId: id,
    exp: config.REFRESH_JWT_EXP
  })
  mongo.saveRefreshToken(response.id, refreshToken).then(function(tokenSaved) {
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
}

function isValid(username, password) {

}

app.post("/signup", function(req, res) {
  if (isValid(req.body.username, req.body.password)) {
    var salt = getSalt(config.SALT_LENGTH)
    var credentials = sha512(res.body.password, salt)
    credentials["username"] = req.body.username
    mongo.insertAuthCredentials(credentials).then(function(response) {
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
        res.json(response.status).json({
          message: response.message
        })
      }
    }).fail(function(err) {
      console.error(err)
    })
  }
})

app.post("/login", function(req, res) {
  if (isValid(req.body.username, req.body, password)) {
    mongo.getAuthCredentials(req.body.username).then(function(response) {
      if (response.status == 200) {
        if (response.credentials.password == sha512(req.body.username, response.credentials.salt)) {
          generateRefreshToken(response.id).then(function(result) {
            if (result.status == 200) {
              res.status(200).json({
                accessToken: generateJWT({
                  userId: response.credentials._id,
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

app.use("*", function(req, res, next) {
  if (req.headers["authorization"] != null) {
    var authorizationHeader = req.headers["authorization"]
    var accessToken = authorizationHeader.substring(authorizationHeader.indexOf(':') + 1,
      authorizationHeader.length).trim()
    jwt.verify(accessToken,
      config.PUBLIC_KEY, {
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
  } else {
    res.json(401).json({
      "message": "Unauthorized"
    })
  }
})

app.get("/accesstoken", function(req, res) {
  mongo.fetchRefreshToken(req.accessToken.payload.userId,
    req.accessToken.payload.refreshToken).then(function(response) {
      if (response.status == 200) {
        if (response.refreshToken == req.accessToken.payload.refreshToken) {
          jwt.verify(request.refreshToken,
            config.PUBLIC_KEY, {
              algorithms: ["RS512"],
              maxAge: config.REFRESH_JWT_EXP,
              clockTimestamp: new Date().getTime() / 1000
            }, function(err, payload) {
              if (err) {
                if (err.name == "TokenExpiredError") {
                  res.status(400).json({
                    message: "Refresh token expired"
                  })
                } else if (err.name == "JSONWebTokenError") {
                  res.status(400).json({
                    message: "Malformed Refresh token"
                  })
                }
              }
              mongo.deleteRefreshToken(req.accessToken.payload.userId).then(function(isDeleted) {
                if (isDeleted) {
                  res.status(200).json({
                    accessToken: generateJWT({
                      userId: req.accessToken.payload.userId,
                      exp: config.JWT_EXP
                    }),
                    tokenType: "JWT"
                  })
                }

              }).fail(function(err) {
                console.error(err)
              })
            })
        } else {
          res.status(400).json({
            message: "Incorrect refresh token"
          })
        }
      } else {
        res.status(response.status).json({
          message: response.message
        })
      }
    }).fail(function(err) {
      console.log(err)
    })
})

//Add support for 2FA using TOTP here

app.listen(PORT, function() {
  console.log("Listening to port:- " + PORT);
})