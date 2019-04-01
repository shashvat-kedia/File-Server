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

app.post("/auth", function(req, res) {
  if (isValid(req.body.userName, req.body.password)) {
    var salt = getSalt(config.SALT_LENGTH)
    var credentials = sha512(res.body.password, salt)
    credentials["userName"] = req.body.userName
    mongo.insertOne(credentials).then(function(response) {
      if (response.status == 200) {
        res.json({
          accessToken: jwt.sign({
            userId: response.id,
            exp: config.JWT_EXP,
          }, config.PRIVATE_KEY, { algorithm: 'RS512' }),
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

app.listen(PORT, function() {
  console.log("Listening to port:- " + PORT);
})