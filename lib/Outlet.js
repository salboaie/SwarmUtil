/**
 * Created with JetBrains WebStorm.
 * User: sinica
 * Date: 7/9/12
 * Time: 5:25 PM
 * To change this template use File | Settings | File Templates.
 */

var redis = require("redis");
var util = require('swarmutil');
var uuid = require('node-uuid');



exports.newOutlet = function(socketParam, onLoginCallback){
    var outlet={
        onLoginCallback:onLoginCallback,
        pendingCmds: new Array(),
        redisClient:null,
        socket:socketParam,
        sessionId:null,
        loginSwarmingVariables:null,
        isClosed:false,
        userId:null,
        parser:null,
        onChannelNewMessage:function (channel, message) {
            //var json = JSON.parse(message);
            //message = JSON.stringify(json);
            dprint("Writing to client socket: " + message);
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
                logInfo("Closing session " + this.sessionId)
                if(this.redisClient != null){
                    this.redisClient.quit();
                }
                delete thisAdapter.connectedOutlets[this.sessionId];
                this.socket.destroy();
                this.isClosed = true;
            }
        },
        currentExecute:null,
        execute : function(messageObj){
            dprint("Executing message from socket: " + J(messageObj));
            if(this.pendingCmds != null){
                this.pendingCmds.push(messageObj);
            }
            else{
                this.currentExecute(messageObj);
            }
        },
        sendPendingCmds:function(){
            for (var i = 0; i < this.pendingCmds.length; i++) {
                this.currentExecute(this.pendingCmds[i]);

            }
            this.pendingCmds = null;
        },
        executeButNotAuthenticated : function (messageObj){
            if(messageObj.swarmingName != thisAdapter.loginSwarmingName ){
                logErr("Could not execute [" +messageObj.swarmingName +"] swarming without being logged in");
                this.close();
            }
            else{
                this.executeSafe(messageObj);
            }
        },
        checkPolicy:null,
        executeSafe : function (messageObj){
            if(messageObj.swarmingName == undefined || thisAdapter.compiledSwarmingDescriptions[messageObj.swarmingName] == undefined){
                logErr("Unknown swarm required by a client: [" + messageObj.swarmingName + "]");
                return;
            }

            beginContext(messageObj);

            try{
    //            if(messageObj.debug == true){
    //                cprint("Swarming message from socket " + J(messageObj));
    //            }
                if(messageObj.command == "start"){
                    var ctorName = "start";
                    if(messageObj.ctor != undefined){
                        ctorName = messageObj.ctor;
                    }
                    var swarming = thisAdapter.newSwarmingPhase(messageObj.swarmingName,ctorName);
                    var initVars = thisAdapter.compiledSwarmingDescriptions[messageObj.swarmingName].vars;
                    for (var i in initVars){
                        swarming[i] = initVars[i];
                    }
                    for (var i in messageObj){
                        swarming[i] = messageObj[i];
                    }
                    swarming.command = "phase";
                    var start = thisAdapter.compiledSwarmingDescriptions[messageObj.swarmingName][ctorName];
                    var args = messageObj.commandArguments;
                    delete swarming.commandArguments;


                    if(start != undefined){
                        start.apply(swarming,args);
                    }
                    else{
                        logErr("Unknown constructor [" + ctorName + "]");
                    }


                }
                else
                if(messageObj.command == "phase"){
                    var swarming = thisAdapter.newSwarmingPhase(messageObj.swarmingName,messageObj);
                    swarming.swarm(swarming.currentPhase);
                }
                else{
                    logErr("["+thisAdapter.nodeName +"] I don't know what to execute "+ JSON.stringify(messageObj));
                }
            }
            catch (err){
                logErr("Failing to start swarm: " + messageObj.swarmingName + " ctor: " + ctorName ,err);
            }
            endContext();
        } ,
        renameSession:function(newSession){
            var oldChannel = util.mkChannelUri(this.sessionId);
            var oldSessionId = this.sessionId;
            setTimeout(function(){
                //cleanings
                this.redisClient.unsubscribe(oldChannel);
                thisAdapter.connectedOutlets[oldSessionId] = null;
            },1000);
            this.sessionId = newSession;
            var channel = util.mkChannelUri(this.sessionId);
            outlet.redisClient.subscribe(channel);
        }
    };


    outlet.sessionId = uuid.v4();
    var indentifyCmd = {
        sessionId        : outlet.sessionId,
        swarmingName     : "login.js",
        command          : "identity"
    };

    util.writeObject(socketParam,indentifyCmd);
    thisAdapter.connectedOutlets[outlet.sessionId] = outlet;

    outlet.currentExecute = outlet.executeButNotAuthenticated;
    outlet.redisClient = redis.createClient(thisAdapter.redisPort,thisAdapter.redisHost);

    var channel = util.mkChannelUri(outlet.sessionId);
    dprint("Subscribing to channel " + channel );
    outlet.redisClient.subscribe(channel);
    outlet.redisClient.on("message",outlet.onChannelNewMessage.bind(outlet));

    outlet.parser = util.createFastParser(outlet.execute.bind(outlet));
    outlet.checkPolicy = checkPolicy;
    socketParam.on('data', function (data){
        var utfData = data.toString('utf8');
        if(this.checkPolicy != null){
            var check = this.checkPolicy;
            this.checkPolicy = null;
            if(check (utfData)){
                return;
            }
            //normal message,continue
        }
        this.parser.parseNewData(utfData);
    }.bind(outlet));

     outlet.redisClient.on("subscribe",function(){
            outlet.sendPendingCmds();
        }.bind(outlet));

    socketParam.on('error',outlet.close.bind(outlet));
    socketParam.on('close',outlet.close.bind(outlet));
    return outlet;
}




writePolicy = function(socket){
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

function checkPolicy(utfData){
    if(utfData.indexOf("<policy-file-request/>") != -1){
        writePolicy(this.socket);
        return true;
    }
    return false;
}
