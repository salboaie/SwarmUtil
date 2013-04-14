var dgram = require("dgram");
var events = require('events');
var sys = require('sys');

var topology = require('topology').getTopology();

exports.createUdpPubSubNode = function (port, adress){
    if(port == undefined){
        port = 0;
    }
    return new UdpPubSubNode(port, adress);
}

function UdpPubSubSock(port, adress){
    var self = this;
    var emitter = new events.EventEmitter();
    var sock = dgram.createSocket('udp4');
    var pendingList = [];

    //socket.address().address and socket.address().port

    topology.addNodeInGroup("Core","Core");
    topology.addNodeInfo("Core", "udpSockAddress", {"port":port, "address":address} );
    startSwarm("alive.js", "initUdp", thisAdaper, sock, function(err, ret){
        if(err){
            logErr("Could not load topology information. Make sure that the 'Core' adapter is started");
        }
        topology.update(ret);
        // we received topology information, send all te pending swarms
        for(i =0; i<= pendingList; i++){
            var p = pendingList[i];
            this.publish(p.channel, p.obj, p.callBack);
        }
        pendingList = null;
    });
    topology.addNodeInfo("Core", "udpSockAddress", {"port":port, "address":address});

    sock.bind(port, adress, function(event) {
        emitter.emit("ready", event);
       // s.addMembership('224.0.0.114');
    });

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
             startSwarm("udpSubscribe.js", channel, thisAdapter.nodeName);
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
              topology.map(channel,"udpSockAddress", function(sockAddress){
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