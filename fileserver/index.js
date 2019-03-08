const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const multer = require('multer');
const uploader = multer({dest: '/uploads'});
const AWS = require('aws-sdk');
const config = require('./config.js');
const amqp = require('amqplib/callback_api');
const app = express();

AWS.config.update(config.AWS_CONFIG)

const S3 = new AWS.S3()
const s3_params = config.S3_CONFIG

var rmq_connection = null
var pub_channel = null
var offlinePubQueue = []

const PORT = 8080 || process.env.PORT;

function getTimestampToAppendcd(req){
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

function connectToRMQ(){
  amqp.connect(config.RMQ_URL,function(err,con){
    if(err){
      console.error("RMQ Error:- " + err.message)
      return setTimeout(connectToRMQ,1000)
    }
    con.on("error",function(err){
      if(err.message != "Connection closing"){
        console.error("RMQ Error:- " + err.message)
      }
    })
    con.on("close",function(err){
      console.error("RMQ Error:- " + err.message)
      console.info("Retrying...")
      setTimeout(connectToRMQ,1000)
    })
    console.log("RMQ connected")
    rmq_connection = con
    startPublisher()
    //startWorker()
  })
}

function startPublisher(){
  rmq_connection.createConfirmChannel(function(err,ch){
    if(err){
      console.error("RMQ Error:- " + err.message)
      return
    }
    ch.on("error",function(err){
      console.error("RMQ Error:- " + err.message)
    })
    ch.on("close",function(err){
      console.error("RMQ Error:- " + err.message)
    })
    pub_channel = ch
    while(true){
      var [exchange, routingKey, content] = offlinePubQueue.shift()
      publish(exchange, routingKey, content)
    }
  })
}

function startWorker() {
  rmq_connection.createChannel(function(err,ch){
    if(err){
      console.log("RMQ Error:- " + err.message)
      return;
    }
    ch.on("error",function(err){
      console.error("RMQ Error:- " + err.message)
    })
    ch.on("close",function(err){
      console.error("RMQ Error:- " + err.message)
    })
    ch.prefetch(10)
    ch.assertQueue("jobs",{durable: true},function(err,_ok){
      if(err){
        console.error("RMQ Error:- " + err.message)
      }
      ch.consume("jobs",processMessage,{noAck: false})
      console.log("Worker is started")
    })
  })
}

function processMessage(message){
  work(message,function(ok){
    try{
      if(ok){
        ch.ack(message)
      }
      else{
        ch.reject(message,true)
      }
    }
    catch(err){
      console.error("Message processing Error:- " + err.message)
    }
  })
}

function work(message,cb){
  console.log("Message to be processed here")
  cb(true)
}

function publish(exchange, routingKey, content){
  try{
    pub_channel.publish(exchange,routingKey,content,{persistent: true},function(err,ok){
      if(err){
        console.error("RMQ Error:- " + err.message)
        offlinePubQueue.push([exchange,routingKey,content])
        pub_channel.connection.close()
      }
    })
  }
  catch(exception){
    console.error("Exception:- " + exception.message)
    offlinePubQueue.push([exchange,routingKey,content])
  }
}

app.use("*",function(req,res,next){
  if(req.headers["authorization"] == config.API_KEY){
    connectToRMQ()
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
  fs.appendFile("./text/CP" + getDate() + "+.txt",getTimestampToAppend(req),function(err){
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
  const tempPath = req.file.path
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
    publish("","jobs",new Buffer(destPath))
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