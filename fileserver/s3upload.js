const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const config = requrie('./config.js');
const amqp = require('amqplib/callback_api');
const app = express()

AWS.config.update(config.AWS_CONFIG)

const S3 = new AWS.S3()
const s3_params = config.S3_CONFIG

var rmq_connection = null
var con_channel = null

app.use(bodyParser.urlextended({encoded: true}))
app.use(bodyParser.json())
app.use(cors())

const PORT = 5000 | process.env.PORT

function connectToRMQ(){
  amqp.connect(config.RMQ_URL,function(err,con){
    if(err){
      console.error("RMQ Error:- " + err.message)
      return setTimeout(connectToRMQ, 1000)
    }
    con.on("error",function(err){
      if(err.message != "Connection closing"){
        console.error("RMQ Error:- " + err.message)
      }
    })
    con.on("close",function(err){
      console.error("RMQ Error:- " + err.message)
      console.info("Retrying...")
      return setTimeout(connectToRMQ(), 1000)
    })
    console.log("RMQ connected")
    rmq_connection = con
    startConsumer()
  })
}

function startConsumer(){
  rmq_connection.createChannel(function(err,ch){
    if(err){
      console.error("RMQ Error:- " + err.message)
      return
    }
    ch.on("error",function(err){
      console.error("RMQ Error:- " + err.message)
    })
    ch.on("close",function(err){
      console.error("RMQ Error:- " + err)
    })
    ch.assertQueue(config.RMQ_NAME, {durable: false})
    con_channel = ch
    console.log("Consumer started")
    consume()
  })
}

function consume(){
  try{
    con_channel.consume(config.RMQ_NAME,function(message){
      console.log(message.content.toString())
    },{noAck: true})
    setTimeout(consume,5000)
  }
  catch(exception){
    console.error("Consumer Exception:- " + exception.message)
  }
}

app.listen(PORT,function(){
  connectToRMQ()
  console.log("Consumer service listening on :- " + PORT)
})