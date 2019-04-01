const mongodb = require('mongodb');
const mongoClient = mongodb.MongoClient
const q = require('q');
const config = require('./config.js');

var db0;

mongoClient.connect(config.MONGO_URL, function(err, database) {
  if (err) {
    throw err
  }
  db0 = database.db(config.DB_NAME);
})

function insertAuthCredentials(credentials) {
  var deferred = q.defer()
  if (dbo.collection(config.AUTH_COLLECTION_NAME).findOne({ userName: credentials.userName }).count() == 0) {
    dbo.collection(config.AUTH_COLLECTION_NAME).insertOne(credentials, function(err, result) {
      if (err) {
        deferred.reject(err)
      }
      deferred.resolve({
        status: 200,
        id: result.insertedIds[0]
      });
    })
  } else {
    deferred.resolve({
      status: 409,
      message: "Account with username " + credentials.userName + " already exists"
    })
  }
  return deferred.promise()
}

