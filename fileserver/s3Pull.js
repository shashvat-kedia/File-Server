const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const config = require('./config.js');
const hash = require('object-hash');
const q = require('q');
const randomAccessFile = require('random-access-file');

const S3 = new AWS.S3(config.AWS_CONFIG)

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
      chunkPathWithOffset.file.write(chunkPathWithOffset.offset, Buffer.from(data))
      deferred.resolve("Done")
    }
  }).send()
  return deferred.promise
}

module.exports = {
  pullFromS3: function pullFromS3(fileId) {
    S3.headObject(getS3ParamsForPull("/uploads/files/" + fileId + ".txt"), function(err, metadata) {
      if (err && err.code == 'Not Found') {
        console.error(err)
        //Send Message to Notification service for file not found
      }
      else {
        console.log(metadata)
        console.log(2)
        S3.getObject(s3_params, function(err, data) {
          if (err) {
            console.error(err)
          }
          chunkPaths = data.Body.toString().split(',')
          chunkPathsWithOffset = []
          var rAF = randomAccessFile("nesfile.txt")
          for (var i = 0; i < chunkPaths.length; i++) {
            chunkPathsWithOffset.push({
              path: chunkPaths[i],
              offset: i * config.READ_CHUNKSIZE,
              file: rAF
            })
          }
          if (chunkPaths.length > 0) {
            console.log(chunkPaths)
            q.all(chunkPathsWithOffset.map(getChunk)).then(function(response) {
              console.log(response)
            })
          }
          else {
            //Send Message to Notification service for empty file
          }
        })
      }
    })
  }
}