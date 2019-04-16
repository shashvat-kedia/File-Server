const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const speakeasy = require('speakeasy');
const mongo = require('./mongo.js');
const config = require('./config.js');
const q = require('q');
const ldap = require('ldapjs');
const app = express()

app.use(bodyParser.urlencoded({ extended: true }))
app.use(bodyParser.json())
app.use(cors())

const PORT = 27327 || process.env.PORT

function getLDAPConnection() {
  return ldap.createClient(config.LDAP_CONFIG)
}

function getSalt(length) {
  return crypto.randomBytes(Math.ceil(length / 2)).toString('hex').slice(0, length)
}

function sha512(password, salt) {
  var hash = crypto.createHmac('sha512', salt)
  hash.update(password)
  var value = hash.digest('hex');
  return value
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

app.post("/signup", function(req, res) {
  if (isValid(req.body.username, req.body.password)) {
    var salt = getSalt(config.SALT_LENGTH)
    var passwordHash = sha512(req.body.password)
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
  }
})

app.post("/login", function(req, res) {
  if (isValid(req.body.username, req.body.password)) {
    mongo.getAuthCredentials(req.body.username).then(function(response) {
      if (response.status == 200) {
        if (response.credentials.passwordHash == sha512(req.body.password, response.credentials.salt)) {
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