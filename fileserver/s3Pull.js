const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const config = require('./config.js');
const hash = require('object-hash');
const q = require('q');
const redis = require('redis');
const redisClient = redis.createClient();

const S3 = new AWS.S3(config.AWS_CONFIG)

redisClient.on('connect', function() {
  console.log("Redis client connected")
})

redisClient.on('error', function(err) {
  console.error(err)
})

function getS3ParamsForPull(path) {
  s3_params = config.S3_CONFIG
  s3_params["Key"] = path
  return s3_params
}

function getChunk(chunkPathWithOffset) {
  var deferred = q.defer()
  redisClient.get(chunkPathWithOffset.path, function(err, result) {
    if (err) {
      console.error(err)
      deferred.reject(response.error)
    }
    else if (result == null) {
      var data = ""
      S3.getObject(getS3ParamsForPull(chunkPathWithOffset.path)).on('httpData', function(chunk) {
        data += chunk.toString()
      }).on('httpDone', function(response) {
        if (response.error) {
          deferred.reject(response.error)
        }
        else {
          redisClient.set(chunkPathWithOffset.path, data)
          deferred.resolve({
            'data': data,
            'offset': chunkPathWithOffset.offset,
            'status': 200
          })
        }
      }).send()
    }
    else {
      console.log("Redis cache hit")
      deferred.resolve({
        'data': result,
        'offset': chunkPathWithOffset.offset,
        'status': 200
      })
    }
  })
  return deferred.promise
}

module.exports = {
  getFileMetadata: function getFileMetadata(fileId) {
    var deferred = q.defer()
    S3.headObject(getS3ParamsForPull("/uploads/files/" + fileId + ".txt"), function(err, metadata) {
      if (err && err.code == 'Not Found') {
        console.error("File not found on S3")
        deferred.resolve(false)
      }
      else if (err) {
        console.error(err)
        deferred.reject(err)
      }
      else {
        deferred.resolve(true)
      }
    })
    return deferred.promise
  },
  pullChunkPathFileFromS3: function pullChunkFilePathFromS3(fileId) {
    var deferred = q.defer()
    S3.getObject(getS3ParamsForPull("/uploads/files/" + fileId + ".txt"), function(err, data) {
      if (err) {
        deferred.reject(err)
      }
      chunkPaths = data.Body.toString().split(',')
      deferred.resolve(chunkPaths)
    })
    return deferred.promise
  },
  pullChunkFromS3: function pullChunkFromS3(chunkPaths) {
    chunkPathsWithOffset = []
    for (var i = 0; i < chunkPaths.length; i++) {
      chunkPathsWithOffset.push({
        path: chunkPaths[i],
        offset: i * config.READ_CHUNKSIZE
      })
    }
    if (chunkPaths.length > 0) {
      return q.all(chunkPathsWithOffset.map(getChunk))
    }
    else {
      return null
    }
  }
}