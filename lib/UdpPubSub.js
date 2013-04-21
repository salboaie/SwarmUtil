var dgram = require("dgram");
var events = require('events');
var sys = require('sys');

var abstractPubSUB = require('./abstractPubSub.js').getPubSub();

exports.createUdpPubSubNode = function (cores){
    var pssock = new UdpPubSubSock();

    //topology.addNodeInGroup("Core","Core");
    topology.addNodeInfo("Core", "udpSockAddress", cores );

    startSwarm("alive.js", "initUdp", pssock.getAddr(), function(err, ret){
        if(err){
            logErr("Could not load topology information. Make sure that the 'Core' adapter is started");
        }
        topology.update(ret);
        // we received topology information, send all the pending swarms
        for(i = 0; i<= pendingList; i++){
            var p = pendingList[i];
            pssock.publish(p.channel, p.obj, p.callBack);
        }
        pendingList = null;
    });

    return pssock;
}

exports.createUdpCoreNode = function (current, cores){
    try{
        topology.addNodeInfo("Core", "udpSockAddress", cores);
        var addr = current.split(":");
        var host = addr[0];
        var port = parseInt(addr[1]);
        var pssock = new UdpPubSubSock(port, host);
        return pssock;
    } catch(err){
      cprint("Failed to create udp socket " + err);
    }
}

function UdpPubSubSock(port, adress){
    var self = this;
    var emitter = new events.EventEmitter();
    var sock = dgram.createSocket('udp4');
    var pendingList = [];

    this.getAddr = function(){
        var address = server.address();
        return address.address + ":" + address.port;
    }
    //socket.address().address and socket.address().port
    if(port != undefined){
        sock.bind(port, adress, function(event) {
            emitter.emit("ready", event);
        });
    } else {
        emitter.emit("ready", {});
    }

    sock.on("message", function (msg, rinfo) {
        console.log("server got: " + msg + " from " +
            rinfo.address + ":" + rinfo.port);
        try {
            var obj = JSON.parse(msg);
            emitter.emit("message", msg.channel, msg.value);
        } catch(err){
            errLog("Failed parsing message", err);
        }
    });

    sock.on("listening",function (event){
        emitter.emit("ready", event);
    });

    this.on = function (eventName,callBack){
     if(eventName == "message"){
         emitter.on("message", callBack);
     } else {
             if(eventName == "listening"){
                 emitter.on("ready", callBack);
             }
             else if(eventName == "close" || eventName == "error"){
                 sock.on(eventName,callBack);
             } else {
                 emitter.on(eventName, callBack);
             }
        }
    }

     this.subscribe = function(channel){
         var udpSockAddress = topology.getNodeInfo(channel,"udpSockAddress");
         if(udpSockAddress == undefined){
             startSwarm("udpSubscribe.js","subscribe", channel, thisAdapter.nodeName);
         }
     }

     function errorHandler(err, bytes){
        if(err || bytes <=0){
            logErr("Error sending UDP message");
        }
     }

      this.publish = function(channel,obj, callBack){
          if(pendingList != null && channel != "Core"){
              pendingList.push({"channel":channel,"obj":obj, "callBack":callBack});
          } else {
              var udpSockAddress = topology.getNodeInfo();
              var message = JSON.stringify(obj);
              if(message.length > 10000){
                  logErr("UDP Message is long, likely to be dropped or fragmented. Switch to Redis's Pub/Sub or upgrade");
              }
              topology.mapAddr(channel, function(sockAddress){
                  sock.send(message, 0, message.length, sockAddress.port, sockAddress.host, errorHandler );
              });
          }
      }
}

/*
 .on("error",onRedisError(event));
 .on("reconnecting",onRedisReconnecting(event));
 .on("subscribe",onPubSubRedisReady(event));
 .on("message", function (channel, message) {
 */