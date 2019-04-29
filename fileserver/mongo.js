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
  updateAuthCredentials: function updateAuthCredentials(userId, newPasswordHash, newSalt) {
    var deferred = q.defer()
    dbo.collection(config.AUTH_COLLECTION_NAME).updateOne({ _id: userId }, {
      $set: {
        passwordHash: newPasswordHash,
        salt: newSalt
      }
    }, function(err, result) {
      if (err) {
        deferred.reject(err)
      }
      if (result.result.n == 1) {
        deferred.resolve({
          status: 200
        })
      } else {
        deferred.resolve({
          status: 404,
          message: "No account found"
        })
      }
    })
    return deferred.promise
  },
  getAuthCredentials: function getAuthCredentials(username) {
    var deferred = q.defer()
    dbo.collection(config.AUTH_COLLECTION_NAME).findOne({ username: username }, function(err, result) {
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
        result.id = result._id
        deferred.resolve({
          status: 200,
          credentials: result
        })
      }
    })
    return deferred.promise
  },
  fetchRefreshToken: function fetchRefreshToken(refreshToken) {
    var deferred = q.defer()
    dbo.collection(config.REFRESH_TOKEN_COLLECTION_NAME).findOne({ refreshToken: refreshToken }, function(err, result) {
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
  saveRefreshToken: function saveRefreshToken(userId, refreshToken) {
    var deferred = q.defer()
    dbo.collection(config.REFRESH_TOKEN_COLLECTION_NAME).insertOne({
      userId: userId,
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
  deleteOneRefreshToken: function deleteOneRefreshToken(refreshToken) {
    var deferred = q.defer()
    dbo.collection(config.REFRESH_TOKEN_COLLECTION_NAME).deleteOne({ refreshToken: refreshToken }, function(err, result) {
      if (err) {
        console.error(err)
        deferred.reject(err)
      }
      if (result.result.ok == 1) {
        deferred.resolve(true)
      } else {
        deferred.resolve(false)
      }
    })
    return deferred.promise
  },
  deleteManyRefreshToken: function deleteManyRefreshToken(userId) {
    var deferred = q.defer()
    dbo.collection(config.REFRESH_TOKEN_COLLECTION_NAME).deleteMany({ userId: userId }, function(err, result) {
      if (err) {
        console.error(err)
        deferred.reject(err)
      }
      if (result.result.ok == 1) {
        deferred.resolve(true)
      } else {
        deferred.resolve(false)
      }
    })
  }
}