const express = require('express');
const app = express();
const jwt = require('jsonwebtoken');
const mongo = require('./mongo.js');

const PORT = 27327 || process.env.PORT

app.post("/auth", function(req, res) {

})

app.listen(PORT, function() {
  console.log("Listening to port:- " + PORT);
})