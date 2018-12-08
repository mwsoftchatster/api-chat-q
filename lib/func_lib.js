/* jshint esnext: true */
var email = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-chat-q/lib/email_lib.js');
var config = require('/Users/nikolajuskarpovas/Desktop/AWS/chatster_microservices/api-chat-q/config/config.js');


/**
 *  Setup the pool of connections to the db so that every connection can be reused upon it's release
 *
 */
var mysql = require('mysql');
var Sequelize = require('sequelize');
const sequelize = new Sequelize(config.db.name, config.db.user_name, config.db.password, {
    host: config.db.host,
    dialect: config.db.dialect,
    port: config.db.port,
    operatorsAliases: config.db.operatorsAliases,
    pool: {
      max: config.db.pool.max,
      min: config.db.pool.min,
      acquire: config.db.pool.acquire,
      idle: config.db.pool.idle
    }
});

/**
 *  Publishes offline message received response to api-chat-c
 */
function publishOfflineMessageOnChatC(message, amqpConn, topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiChatC.*';
            var key = `apiChatC.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.publish(exchange, key, new Buffer(message));
        });
    }
}

/**
 *  Publishes received online message received response to api-chat-c
 */
function publishReceivedOnlineMessageOnChatC(message, amqpConn, topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiChatC.*';
            var key = `apiChatC.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.publish(exchange, key, new Buffer(message));
        });
    }
}

/**
 *  Publishes delete received online message received response to api-chat-c
 */
function publishDeleteReceivedOnlineMessageOnChatC(message, amqpConn, topic) {
    if (amqpConn !== null) {
        amqpConn.createChannel(function(err, ch) {
            var exchange = 'apiChatC.*';
            var key = `apiChatC.${topic}`;
            ch.assertExchange(exchange, 'topic', { durable: true });
            ch.publish(exchange, key, new Buffer(message));
        });
    }
}

/**
 *  Retrieves offline chat messages
 *
 * (req Object): object that holds all the request information
 * (res Object): object that is used to send user response
 */
module.exports.chatOfflineMessages = function (req, res){
    var offlineMsgs = [];
    sequelize.query('CALL GetOfflineMessages(?)',
    { replacements: [ req.query.dstId ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            for (var i = 0; i < result.length; i++) {
                var offlineMessage = {
                    msgType: result[i].msg_type,
                    contentType: result[i].content_type,
                    senderId: result[i].sender_id,
                    senderName: result[i].sender_name,
                    receiverId: result[i].receiver_id,
                    chatName: result[i].chat_name,
                    messageData: result[i].message,
                    uuid: result[i].message_uuid,
                    contactPublicKeyUUID: result[i].contact_public_key_uuid,
                    messageCreated: result[i].item_created
                };
                offlineMsgs.push(offlineMessage);
            }
            res.json(offlineMsgs);
    }).error(function(err){
        email.sendApiChatQErrorEmail(err);
        res.json(offlineMsgs);
    });
};


module.exports.insertNewOfflineMessage = function (message, amqpConn, topic){
    console.log("insertNewOfflineMessage has been called");
    sequelize.query('CALL InsertNewOfflineMessage(?,?,?,?,?,?,?,?,?,?)',
    { replacements: [ message.msgType, message.contentType, message.senderId, message.senderName, message.receiverId, message.chatname, message.messageText, message.uuid, message.contactPublicKeyUUID, message.messageCreated ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            var response = {
                status: config.rabbitmq.statuses.ok
            };
            publishOfflineMessageOnChatC(JSON.stringify(response), amqpConn, topic);
    }).error(function(err){
        email.sendApiChatQErrorEmail(err);
        var response = {
            status: config.rabbitmq.statuses.error
        };
        publishOfflineMessageOnChatC(JSON.stringify(response), amqpConn, topic);
    });
};


module.exports.insertNewReceivedOnlineMessage = function (message, amqpConn, topic) {
    console.log("insertNewReceivedOnlineMessage has been called");
    console.log("message.message => " + message.messageText);
    sequelize.query('CALL InsertNewReceivedOnlineMessage(?,?,?,?,?,?,?,?,?,?)',
    { replacements: [ message.msgType, message.contentType, message.senderId, message.receiverId, message.chatname, message.messageText, message.uuid, message.contactPublicKeyUUID, "message", message.messageCreated ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            var response = {
                status: config.rabbitmq.statuses.ok
            };
            publishReceivedOnlineMessageOnChatC(JSON.stringify(response), amqpConn, topic);
    }).error(function(err){
        email.sendApiChatQErrorEmail(err);
        var response = {
            status: config.rabbitmq.statuses.error
        };
        publishReceivedOnlineMessageOnChatC(JSON.stringify(response), amqpConn, topic);
    });
};

module.exports.deleteReceivedOnlineMessage = function (message, amqpConn, topic) {
    console.log("deleteReceivedOnlineMessage has been called");
    sequelize.query('CALL DeleteReceivedOnlineMessage(?)',
    { replacements: [ message.uuid ],
        type: sequelize.QueryTypes.RAW }).then(result => {
            var response = {
                status: config.rabbitmq.statuses.ok
            };
            publishDeleteReceivedOnlineMessageOnChatC(JSON.stringify(response), amqpConn, topic);
    }).error(function(err){
        email.sendApiChatQErrorEmail(err);
        var response = {
            status: config.rabbitmq.statuses.error
        };
        publishDeleteReceivedOnlineMessageOnChatC(JSON.stringify(response), amqpConn, topic);
    });
};