const mongodb = require('mongodb');
const mongoClient = mongodb.MongoClient
const config = require('./config.js');

var db;

mongoClient.connect(config.MONGO_URL, function(err, database) {
  if (err) {
    throw err
  }
  db = database
})

