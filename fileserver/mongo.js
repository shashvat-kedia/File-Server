const mongodb = require('mongodb');
const mongoClient = mongodb.MongoClient
const q = require('q');
const config = require('./config.js');

var dbo;

mongoClient.connect(config.MONGO_URL, function(err, database) {
  if (err) {
    throw err
  }
  dbo = database.db(config.DB_NAME);
})

module.exports = {
  insertAuthCredentials: function insertAuthCredentials(credentials) {
    var deferred = q.defer()
    dbo.collection(config.AUTH_COLLECTION_NAME).find({ username: credentials.username }).count(function(err, count) {
      if (err) {
        deferred.reject(err)
      } else if (count == 0) {
        dbo.collection(config.AUTH_COLLECTION_NAME).insertOne(credentials, function(err, result) {
          if (err) {
            deferred.reject(err)
          }
          deferred.resolve({
            status: 200,
            id: result.insertedId
          });
        })
      } else {
        deferred.resolve({
          status: 409,
          message: "Account with username " + credentials.username + " already exists"
        })
      }
    })
    return deferred.promise
  },
  getAuthCredentials: function getAuthCredentials(username) {
    var deferred = q.defer()
    dbo.collection(config.AUTH_COLLECTION_NAME).findOne({ username: username }).toArray(function(err, result) {
      if (err) {
        console.error(err)
        deferred.reject(err)
      }
      if (result == null) {
        deferred.resolve({
          status: 404,
          message: "Account not found"
        })
      } else {
        deferred.resolve({
          status: 200,
          credentials: result
        })
      }
    })
    return deferred.promise
  },
  fetchRefreshToken: function fetchRefreshToken(id, refreshToken) {
    var deferred = q.defer()
    dbo.collection(config.REFRESH_TOKEN_COLLECTION_NAME).findOne({ userId: id }).toArray(function(err, result) {
      if (err) {
        console.error(err)
        deferred.reject(err)
      }
      if (result == null) {
        deferred.resolve({
          status: 404,
          message: "Refresh token not found try re-authenticating"
        })
      } else {
        deferred.resolve({
          status: 200,
          refreshToken: result.refreshToken
        })
      }
    })
    return deferred.promise
  },
  saveRefreshToken: function saveRefreshToken(id, refreshToken) {
    var deferred = q.defer()
    dbo.collection(config.REFRESH_TOKEN_COLLECTION_NAME).insertOne({
      userId: id,
      refreshToken: refreshToken
    }, function(err, result) {
      if (err) {
        console.error(err)
        deferred.reject(err)
      }
      deferred.resolve(true)
    })
    return deferred.promise
  },
  deleteRefreshToken: function deleteRefreshToken(id) {
    var deferred = q.defer()
    dbo.collection(config.REFRESH_TOKEN_COLLECTION_NAME).deleteOne({ userId: id }, function(err, result) {
      if (err) {
        console.error(err)
        deferred.reject(err)
      }
      if (result.ok == 1) {
        deferred.resolve(true)
      } else {
        deferre.resolve(false)
      }
    })
    return deferred.promise
  }
}