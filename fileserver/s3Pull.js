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
      deferred.reject(err)
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
      console.log("Redis Chunk cache hit " + chunkPathWithOffset.path)
      deferred.resolve({
        'data': result,
        'offset': chunkPathWithOffset.offset,
        'status': 200
      })
    }
  })
  return deferred.promise
}

function getFileMetadata(key) {
  var deferred = q.defer()
  redisClient.get(key + "-metadata", function(err, result) {
    if (err) {
      console.error(err)
      deferred.reject(err)
    }
    else if (result == null) {
      S3.headObject(getS3ParamsForPull(key), function(err, metadata) {
        if (err && err.code == 'Not Found') {
          console.error("File not found on S3")
          deferred.resolve(null)
        }
        else if (err) {
          console.error(err)
          deferred.reject(err)
        }
        else {
          redisClient.set(key + "-metadata", JSON.stringify(metadata))
          deferred.resolve(metadata)
        }
      })
    }
    else {
      console.log(1)
      console.log("Redis cache hit")
      deferred.resolve(JSON.parse(result))
    }
  })
  return deferred.promise
}

module.exports = {
  getS3ParamsForPull: getS3ParamsForPull,
  getFileMetadata: getFileMetadata,
  getFileLength: function getFileLength(key) {
    var deferred = q.defer()
    getFileMetadata(key).then(function(response) {
      if (response != null) {
        deferred.resolve(response.ContentLength)
      }
      else {
        deferred.resolve(-1)
      }
    })
    return deferred.promise
  },
  pullChunkPathFileFromS3: function pullChunkPathFileFromS3(fileId) {
    var deferred = q.defer()
    getFileMetadata("/uploads/files/" + fileId + ".json").then(function(response) {
      if (response != null) {
        redisClient.get("/uploads/files/" + fileId + ".json", function(err, result) {
          if (err) {
            console.error(err)
            deferred.reject(err)
          }
          else if (result == null) {
            S3.getObject(getS3ParamsForPull("/uploads/files/" + fileId + ".json"), function(err, data) {
              if (err) {
                console.error(err)
                deferred.reject(err)
              }
              chunkPaths = JSON.parse(data.Body).chunkPaths
              if (chunkPaths.length > 1) {
                redisClient.set("/uploads/files/" + fileId + ".json", JSON.parse(data.Body))
                deferred.resolve({
                  "status": 200,
                  "etag": data.ETag,
                  "lastModified": data.LastModified,
                  "fileData": JSON.parse(data.Body)
                })
              }
              else {
                deferred.resolve({
                  "status": 422,
                  "message": "File broken"
                })
              }
            })
          }
          else {
            console.log("Redis cache hit")
            deferred.resolve({
              "status": 200,
              "fileData": result
            })
          }
        })
      }
      else {
        deferred.resolve({
          "status": 404,
          "message": "File not found"
        })
      }
    }).fail(function(err) {
      console.error(err)
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