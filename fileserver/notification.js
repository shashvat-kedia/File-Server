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
      var message = JSON.parse(message.content.toString())
      if (message.action == config.ACTION_SEND_NOTIF) {
        message.channel = config.NOTIF_CHANNEL
        sendNotificationClientDevices(message)
      } else {
        sendNotificationDevice(message)
      }
    }, { noAck: true })
  }
  catch (exception) {
    console.error("Consumer Exception:- " + exception.message)
  }
}

function sendNotificationClientDevices(messageConfig) {
  Object.keys(connectedDevices[messageConfig.userId]).forEach(function(key) {
    messageConfig.socketId = key
    sendNotificationDevice(messageConfig)
  })
}

function sendNotificationDevice(messageConfig) {
  var socket = connectedDevices[messageConfig.userId][messageConfig.socketId]
  if (socket != null) {
    socket.emit(messageConfig.channel, messageConfig.message)
    console.log("Message emitted")
  } else {
    console.error("Socker returned null")
  }
}

io.on('connection', function(client) {
  //Can be connected to using io(<NOTIFICATION_SERVICE_URL>,{query: "usedId:" + <USED_ID>})
  console.log("New connection added")
  if (connectedDevices[client._query['userId']] == null) {
    connectedDevices[client._query['userId']] = {}
  }
  connectedDevices[client._query['userId']][client['id']] = client
  client.on('closing', function(data) {
    connectedDevices[client._query['userId']][client['id']] = null
  })
  client.emit('message', 'Connected to Notification Service')
})

server.listen(PORT, function() {
  connectToRMQ()
  console.log("Listening to PORT " + PORT)
})