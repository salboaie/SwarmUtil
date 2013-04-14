var redis = require("redis");

exports.createRedisPubSubClient = function (redisPort, redisHost){
    return new RedisPubSub(redisPort,redisHost);
}

function RedisPubSub(redisPort, redisHost){
  var client =  redis.createClient(redisPort, redisHost);
    client.retry_delay  = 2000;
    client.max_attempts = 20;

    this.on = function (eventName,callBack){
        if(eventName == "message"){
            client.on(eventName, function (channel, message){
                try{
                    var obj = JSON.parse(message);
                    callBack(channel, obj);
                } catch(err){
                    logErr("Failed to parse JSON message", err);
                }
            });
        } else {
            client.on(eventName, callBack);
        }
    }

    this.subscribe = function(channel){
        client.subscribe(channel);
    }

    this.publish = function(channel, obj , callBack){
        try{
            var message = JSON.stringify(obj);
            client.publish(channel,message, callBack);
        } catch(err){
            logErr("Failed to make JSON from a swarm object", err);
        }
    }
}
