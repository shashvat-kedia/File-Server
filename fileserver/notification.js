const server = require('http').createServer();
const io = require('socket.io')(server);
const hash = require('object-hash');
const amqp = require('amqplib/callback_api');
const config = require('./config.js');

var PORT = 27127 | process.env.PORT

var rmq_connection = null
var con_channel = null

connectedDevices = {}

function connectToRMQ() {
  amqp.connect(config.RMQ_URL, function(err, con) {
    if (err) {
      console.error("RMQ Error:- " + err.message)
      return setTimeout(connectToRMQ, 1000)
    }
    con.on("error", function(err) {
      if (err.message != "Connection closing") {
        console.error("RMQ Error:- " + err.message)
        throw error
      }
    })
    con.on("close", function(err) {
      console.error("RMQ Error:- " + err.message)
      console.info("Retrying...")
      return setTimeout(connectToRMQ(), 1000)
    })
    console.log("RMQ connected")
    rmq_connection = con
    startConsumer()
  })
}

function startConsumer() {
  rmq_connection.createChannel(function(err, ch) {
    if (err) {
      console.error("RMQ Error:- " + err.message)
      return
    }
    ch.on("error", function(err) {
      console.error("RMQ Error:- " + err.message)
      return
    })
    ch.on("close", function(err) {
      console.error("RMQ Error:- " + err)
      return
    })
    ch.assertQueue(config.QUEUE_NAME_NOTIFICATION, { durable: false })
    con_channel = ch
    console.log("Notification consumer started")
    consume()
  })
}

function consume() {
  try {
    con_channel.consume(config.QUEUE_NAME_NOTIFICATION, function(message) {
      sendNotification(JSON.parse(message.content.toString()))
    }, { noAck: true })
  }
  catch (exception) {
    console.error("Consumer Exception:- " + exception.message)
  }
}

function sendNotification(messageConfig) {
  var socket = connectedDevices[messageConfig.socketId]
  if (socket != null) {
    socket.emit(messageConfig.channel, messageConfig.message)
    console.log("Message emitted")
  } else {
    console.error("Socker returned null")
  }
}

io.on('connection', function(client) {
  console.log("New connection added")
  connectedDevices[client['id']] = client
  client.on('closing', function(data) {
    connectedDevices[client['id']] = null
  })
  client.emit('message', 'Connected to Notification Service')
  connectedDevices[client['id']] = client
})

server.listen(PORT, function() {
  connectToRMQ()
  console.log("Listening to PORT " + PORT)
})