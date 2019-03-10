const amqp = require('amqp/callback_api');
const config = require('./config.js');

function connectToRMQ(){
  amqp.connect(config.RMQ_URL,function(err,con){
    if(err){
      console.error("RMQ Error:- " + err.message)
      return null
    }
    con.on("error",function(err){
      if(err.message != "Connection closing"){
        console.error("RMQ Error:- " + err.message)
      }
    })
    con.on("close",function(err){
      console.error("RMQ Error:- " + err.message)
      console.info("Retrying...")
      return null
    })
    console.log("RMQ connected")
    return con
  })
}

module.exports.connectToRMQ = connectToRMQ