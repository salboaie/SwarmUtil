/**
 * Created with JetBrains WebStorm.
 * User: sinica
 * Date: 7/9/12
 * Time: 5:25 PM
 * To change this template use File | Settings | File Templates.
 */

var redis = require("redis");
var util = require('swarmutil');
var adaptor = util.adaptor;

exports.newOutlet = function(socketParam, thisAdaptor,onLoginCallback){
    var outlet={
        thisAdaptor: thisAdaptor,
        onLoginCallback:onLoginCallback,
        redisClient:null,
        socket:socketParam,
        sessionId:null,
        loginSwarmingVariables:null,
        waitingMsg:null,
        isClosed:false,
        userId:null,
        onChannelNewMessage:function (channel, message) {
            var json = JSON.parse(message);
            json.channel = channel;
            message = JSON.stringify(json);
            util.writeSizedString(this.socket,message);
        },
        successfulLogin:function (swarmingVariables) {
            this.loginSwarmingVariables = swarmingVariables;
            this.userId = swarmingVariables.userId;
            this.currentExecute = this.executeSafe;
            this.onLoginCallback(this);
        },
        close:function () {
            if(!this.isClosed){
                console.log("Closing outlet " + this.sessionId)
                if(this.redisClient != null){
                    this.redisClient.quit();
                }
                delete thisAdaptor.connectedOutlets[this.sessionId];
                this.socket.destroy();
                this.isClosed = true;
            }
        },
        currentExecute:null,
        execute : function(messageObj){
            this.currentExecute(messageObj);
        },
        executeButNotIdentified : function (messageObj){
            if(messageObj.sessionId == null){
                console.log("Wrong begin message" + JSON.stringify(messageObj));
                this.close();
                return;
            }
            var existingOultet = thisAdaptor.connectedOutlets[messageObj.sessionId];
            if( existingOultet != null){
                console.log("Disconnecting already connected session " + JSON.stringify(messageObj));
                existingOultet.close(); //disconnect the other client,may be is hanging..
            }

            thisAdaptor.connectedOutlets[messageObj.sessionId] = this;

            this.sessionId = messageObj.sessionId;
            this.currentExecute = this.executeButNotAuthenticated;
            this.redisClient = redis.createClient(thisAdaptor.redisPort,thisAdaptor.redisHost);
            this.redisClient.subscribe(this.sessionId);
            this.redisClient.on("message",this.onChannelNewMessage.bind(this));

            outlet.waitingMsg = messageObj;
            this.redisClient.on("connect",function(){
                this.executeButNotAuthenticated(this.waitingMsg);
            }.bind(this));
        },
        executeButNotAuthenticated : function (messageObj){
            if(messageObj.swarmingName != thisAdaptor.loginSwarmingName ){
                console.log("Could not execute [" +messageObj.swarmingName +"] swarming without being logged in");
                this.close();
            }
            else{
                this.executeSafe(messageObj);
            }
        },
        executeSafe : function (messageObj){
            if(messageObj.command == "start"){

                var ctorName = "start";
                if(messageObj.ctor != undefined){
                    ctorName = messageObj.ctor;
                }

                var swarming = new adaptor.SwarmingPhase(messageObj.swarmingName,ctorName);
                var initVars = thisAdaptor.compiledSwarmingDescriptions[messageObj.swarmingName].vars;
                for (var i in initVars){
                    swarming[i] = initVars[i];
                }
                for (var i in messageObj){
                    swarming[i] = messageObj[i];
                }
                swarming.command = "phase";
                var start = thisAdaptor.compiledSwarmingDescriptions[messageObj.swarmingName][ctorName];
                var args = messageObj.commandArguments;
                delete swarming.commandArguments;
                try{
                    start.apply(swarming,args);
                }
                catch (err){
                    printPhaseError(err,messageObj.swarmingName,ctorName,"none");
                }
            }
            else
            if(messageObj.command == "phase"){
                var swarming = new adaptor.SwarmingPhase(messageObj.swarmingName,messageObj);
                swarming.swarm(swarming.currentPhase);
            }
            else{
                console.log("["+thisAdaptor.nodeName +"] I don't know what to execute "+ JSON.stringify(messageObj));
            }
        }
    };
    outlet.currentExecute = outlet.executeButNotIdentified;
    var parser = util.createFastParser(outlet.execute.bind(outlet));

    socketParam.on('data', function (data){
        console.log(data.toString('utf8'));
        if(data.toString('utf8').indexOf("<policy-file-request/>") != -1){
            exports.writePolicy(socketParam);
            //socketParam.end();
            //outlet.close();
            return;
        }
        parser.parseNewData(data.toString('utf8'));
        //parser.parseNewData(data.toString('utf8');
    });

    socketParam.on('error',outlet.close.bind(outlet));
    socketParam.on('close',outlet.close.bind(outlet));

    return outlet;
}


exports.writePolicy = function(socket){
    var domains = ["*:3000-3001"];
    socket.write("<?xml version=\"1.0\"?>\n");
    socket.write("<cross-domain-policy>\n");
    domains.forEach(
        function(domain)
        {
            var parts = domain.split(':');
            socket.write("<allow-access-from domain=\""+parts[0]+"\" to-ports=\""+parts[1]+"\"/>\n");
        }
    );
    socket.write("</cross-domain-policy>\n");
    socket.end();

}