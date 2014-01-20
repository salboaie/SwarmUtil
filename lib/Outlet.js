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

/**
 *  An Outlet is an object that contains information about current session, handling channels,sockets,etc
 *  TODO: break it to allow multiple socket and web socket connections for the same session
 *
 * */

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
        onSubscribe:null,
        tenantId:null,
        onChannelNewMessage:function (channel, message) {
            //var json = JSON.parse(message);
            //message = JSON.stringify(json);
            util.writeSizedString(this.socket,message);
        },
        successfulLogin:function (swarmingVariables) {
            this.loginSwarmingVariables = swarmingVariables;
            this.userId = swarmingVariables.userId;
            this.currentExecute = this.executeSafe;
            this.tenantId = swarmingVariables.getTenantId();
            this.onLoginCallback(this);
        },
        close:function () {
            if(!this.isClosed){
                logInfo("Closing session " + this.sessionId);
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
            if(this.onSubscribe){
                this.onSubscribe();
            }
        },
        executeButNotAuthenticated : function (messageObj){
            if(messageObj.meta.swarmingName != thisAdapter.loginSwarmingName ){
                logErr("Could not execute [" +messageObj.meta.swarmingName +"] swarming without being logged in");
                this.close();
            }
            else{
                this.executeSafe(messageObj);
            }
        },
        checkPolicy:null,
        executeSafe : function (messageObj){
            if(messageObj.meta.swarmingName == undefined || thisAdapter.compiledSwarmingDescriptions[messageObj.meta.swarmingName] == undefined){
                logErr("Unknown swarm required by a client: [" + messageObj.meta.swarmingName + "]");
                return;
            }

            beginExecutionContext(messageObj);

            try{
    //            if(messageObj.debug == true){
    //                cprint("Swarming message from socket " + J(messageObj));
    //            }
                if(messageObj.meta.command == "start"){
                    var ctorName = "start";
                    if(messageObj.meta.ctor != undefined){
                        ctorName = messageObj.meta.ctor;
                    }
                    var swarming = util.newSwarmPhase(messageObj.meta.swarmingName,ctorName, messageObj);

                    swarming.meta.command = "phase";
                    var start = thisAdapter.compiledSwarmingDescriptions[messageObj.meta.swarmingName][ctorName];
                    var args = messageObj.meta.commandArguments;
                    delete swarming.meta.commandArguments;


                    if(start != undefined){
                        start.apply(swarming,args);
                    }
                    else{
                        logErr("Unknown constructor [" + ctorName + "] for swarm: " +  messageObj.meta.swarmingName);
                    }
                }
                else
                if(messageObj.meta.command == "phase"){
                    //TODO: fix it, looks wrong.. not used yet, isn't it?
                    var swarming = util.newSwarmPhase(messageObj.meta.swarmingName,messageObj);
                    swarming.swarm(swarming.currentPhase);
                }
                else{
                    logErr("["+thisAdapter.nodeName +"] I don't know what to execute "+ JSON.stringify(messageObj));
                }
            }
            catch (err){
                logErr("Failing to start swarm: " + messageObj.meta.swarmingName + " ctor: " + ctorName ,err);
            }
            endExecutionContext();
        } ,
        renameSession:function(newSession,onSubscribe){
            var oldChannel = util.mkChannelUri(this.sessionId);
            var oldSessionId = this.sessionId;
            setTimeout(function(){
                //cleanings
                this.redisClient.unsubscribe(oldChannel);
                thisAdapter.connectedOutlets[oldSessionId] = null;
            }.bind(this),1000);
            this.sessionId = newSession;
            var channel = util.mkChannelUri(this.sessionId);
            outlet.redisClient.subscribe(channel);
            dprint("Subscribing to channel " + channel );
            this.pendingCmds =  new Array();
            this.onSubscribe =  onSubscribe;
        },
        getTenantId:function(){
            return this.tenantId;
        }
    };

    outlet.sessionId = uuid.v4();
    var indentifyCmd = {
        meta                 : {
            sessionId        : outlet.sessionId,
            swarmingName     : "login.js",
            command          : "identity"
        }
    };

    util.writeObject(socketParam,indentifyCmd);
    thisAdapter.connectedOutlets[outlet.sessionId] = outlet;

    outlet.currentExecute = outlet.executeButNotAuthenticated;
    outlet.redisClient = redis.createClient(thisAdapter.redisPort, thisAdapter.redisHost);

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
            if(check (utfData,socketParam)){
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

/**
 * Flex policy file
 * @param socket
 */

//TODO: make configurable, read that XML from a file
writePolicy = function(socket){
    var domains = ["*:3000-3001"];
    if (!socket.writable)
    {
        console.log("Cross-domain socket is not writable!");
        return;
    }
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

/**
 * Check if the data looks like a policy file, flash request. Write the answer
 * @param utfData
 * @return {Boolean}
 */
function checkPolicy(utfData,socket){
    if(utfData.indexOf("<policy-file-request/>") != -1){
        writePolicy(socket);
        return true;
    }
    return false;
}
