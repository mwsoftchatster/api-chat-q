/*
  Copyright (C) 2017 - 2020 MWSOFT

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/* jshint esnext: true */
require('events').EventEmitter.prototype._maxListeners = 0;
var config = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-chat-q/config/config.js');
var email = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-chat-q/lib/email_lib.js');
var functions = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-chat-q/lib/func_lib.js');
var config = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-chat-q/config/config.js');
var fs = require("fs");
var express = require("express");
var https = require('https');
var options = {
    key: fs.readFileSync(config.security.key),
    cert: fs.readFileSync(config.security.cert)
};
var app = express();
var bodyParser = require("body-parser");
var cors = require("cors");
var amqp = require('amqplib/callback_api');
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(express.static("./public"));
app.use(cors());

app.use(function(req, res, next) {
    next();
});

var server = https.createServer(options, app).listen(config.port.chat_q_port, function() {
    email.sendNewApiChatQIsUpEmail();
});


/**
 *  GET all 1-to-1 chat offline messages for the user with id
 *
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
app.get("/chatOfflineMessages", function(req, res) {
    console.log("/chatOfflineMessages has been called");
    functions.chatOfflineMessages(req, res);
});


/**
 *   RabbitMQ connection object
 */
var amqpConn = null;


/**
 *  Subscribe api-chat-q to topic to receive messages
 */
function subscribeToChatQ(topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiChatQ.*';
            var toipcName = `apiChatQ.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.assertQueue(toipcName, { exclusive: false, auto_delete: true }, function(err, q) {
                ch.bindQueue(q.queue, exchange, toipcName);
                ch.consume(q.queue, function(msg) {
                    // check if status ok or error
                    var message = JSON.parse(msg.content.toString());
                    if (toipcName === `apiChatQ.${config.rabbitmq.topics.offlineMessage}`){
                        functions.insertNewOfflineMessage(message, amqpConn, config.rabbitmq.topics.offlineMessageC);
                    } else if (toipcName === `apiChatQ.${config.rabbitmq.topics.receivedOnlineMessage}`){
                        functions.insertNewReceivedOnlineMessage(message, amqpConn, config.rabbitmq.topics.receivedOnlineMessageC);
                    } else if (toipcName === `apiChatQ.${config.rabbitmq.topics.deleteReceivedOnlineMessage}`){
                        functions.deleteReceivedOnlineMessage(message, amqpConn, config.rabbitmq.topics.deleteReceivedOnlineMessageC);
                    } 
                }, { noAck: true });
            });
        });
    }
}


/**
 *  Connect to RabbitMQ
 */
function connectToRabbitMQ() {
    amqp.connect(config.rabbitmq.url, function(err, conn) {
        if (err) {
            email.sendApiChatQErrorEmail(err);
            console.error("[AMQP]", err.message);
            return setTimeout(connectToRabbitMQ, 1000);
        }
        conn.on("error", function(err) {
            email.sendApiChatQErrorEmail(err);
            if (err.message !== "Connection closing") {
                console.error("[AMQP] conn error", err.message);
            }
        });
        conn.on("close", function() {
            console.error("[AMQP] reconnecting");
            return setTimeout(connectToRabbitMQ, 1000);
        });
        console.log("[AMQP] connected");
        amqpConn = conn;

        subscribeToChatQ(config.rabbitmq.topics.offlineMessage);
        subscribeToChatQ(config.rabbitmq.topics.receivedOnlineMessage);
        subscribeToChatQ(config.rabbitmq.topics.deleteReceivedOnlineMessage);
    });
}

connectToRabbitMQ();


