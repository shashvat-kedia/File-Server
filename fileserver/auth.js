const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
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
  return res.json(payload, config.PRIVATE_KEY, { algorithm: 'RS512' })
}

function isValid(username, password) {

}

app.post("/signup", function(req, res) {
  if (isValid(req.body.username, req.body.password)) {
    var salt = getSalt(config.SALT_LENGTH)
    var credentials = sha512(res.body.password, salt)
    credentials["username"] = req.body.username
    mongo.insertOne(credentials).then(function(response) {
      if (response.status == 200) {
        res.status(200).json({
          accessToken: generateJWT({
            userId: response.id,
            exp: config.JWT_EXP
          }),
          tokenType: "JWT"
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
          res.status(200).json({
            accessToken: generateJWT({
              userId: response.credentials._id,
              exp: config.JWT_EXP
            }),
            tokenType: "JWT"
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

app.listen(PORT, function() {
  console.log("Listening to port:- " + PORT);
})