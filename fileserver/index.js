const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const uploader = multer({dest: '/uploads'});
const AWS = require('aws-sdk');
const config = require('./config.js');
const app = express();

AWS.config.update(config.AWS_CONFIG)

const S3 = new AWS.S3()
const s3_params = config.S3_CONFIG

const PORT = 8080 || process.env.PORT;

function getDataToAppend(req){
  return "[" + Math.round((new Date()).getTime())/1000 + "] - " + req.body.content 
}

function getDate(){
  var today = new Date()
  return today.getDay().toString().toUpperCase() + today.getMonth().toString().toUpperCase() + 
          today.getFullYear().toString().toUppeCase()
}

app.use(express.static(__dirname + "/uploads"))

app.use(bodyParser.urlencoded({extended: true}))
app.use(bodyParser.json())
app.use(cors())

app.use("*",function(req,res,next){
  if(req.headers["authorization"] == config.API_KEY){
    next()
  }
  else{
    res.status(303).json({
      "message": "Forbidden"
    })
  }
})

app.get("/",function(req,res){
  console.log("Hit home")
  res.status(200).send({
    "message": "Hit"
  })
})

app.post("/text",function(req,res){
  if(!fs.existsSync("/text/")){
    fs.mkdir("./text/",function(err){
      if(err){
        console.log("Error while creating directory")
        console.error(err)
      }
      console.log("Directory created")
    })
  }
  fs.appendFile("./text/CP" + getDate() + "+.txt",getDataToAppend(req),function(err){
    if(err){
      console.log("Error while appending to file")
      console.error(err)
    }
    console.log("Data saved to file")
    res.status(200).send({
      "message": "Data appended to file"
    })
  }) 
})

app.post("/upload",uploader.single("file"),function(req,res){
  console.log(req)
  const tempPath = req.file.path
  console.log(tempPath)
  const destPath = path.join(__dirname + "/uploads/" + Math.round((new Date()).getTime())/1000 + ".png");
  fs.rename(tempPath,destPath,function(err){
    if(err){
      console.error(err)
      res.status(500).send({
        "message": "Internal server error"
      })
    }
    res.status(200).send({
      "message": "File upload successfull"
    })
  })
})

app.use("*",function(req,res){
  res.status(404).json({
    "message": "Endpoint does not exists"
  })
})

app.listen(PORT,function(){
  console.log("Listening to port: " + PORT)
})