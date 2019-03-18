const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const config = require('./config.js');
const hash = require('object-hash');
const q = require('q');
const redis = require('redis');
const redisClient = redis.createClient();

//Redis to be added here

const S3 = new AWS.S3(config.AWS_CONFIG)

redisClient.on('connect', function() {
  console.log("Redis client connected")
})

function getS3ParamsForPull(path) {
  s3_params = config.S3_CONFIG
  s3_params["Key"] = path
  return s3_params
}

function getChunk(chunkPathWithOffset) {
  var deferred = q.defer()
  var data = ""
  S3.getObject(getS3ParamsForPull(chunkPathWithOffset.path)).on('httpData', function(chunk) {
    data += chunk.toString()
  }).on('httpDone', function(response) {
    if (response.error) {
      deferred.reject(response.error)
    }
    else {
      deferred.resolve({
        'data': data,
        'offset': chunkPathWithOffset.offset,
        'status': 200
      })
    }
  }).send()
  return deferred.promise
}

module.exports = {
  fileExists: function fileExists(fileId) {
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
  pullFromS3: function pullFromS3(fileId, chunkPaths) {
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