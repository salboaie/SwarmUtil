/**
 * Created by: sinica
 * Date: 6/7/12
 * Time: 11:36 PM
 */

var redis = require("redis");
var fs = require('fs');
var util = require("swarmutil");
var nutil = require("util");

function AdaptorBase(nodeName){
    this.nodeName = nodeName;
}

var BROADCAST_NODE_NAME = "BROADCAST";

exports.init = function(nodeName,redisHost,redisPort)
{

    console.log("Starting adaptor " + nodeName);
    thisAdaptor = new AdaptorBase(nodeName);
    redisClient = redis.createClient(redisPort,redisHost);
    pubsubRedisClient = redis.createClient(redisPort,redisHost);
    thisAdaptor.instanceUID = "UID:"+Date.now()+Math.random()+Math.random()+Math.random();

    thisAdaptor.compiledSwarmingDescriptions = [];
    thisAdaptor.msgCounter = 0;
    pubsubRedisClient.subscribe(nodeName);
    thisAdaptor.redisHost = redisHost;
    thisAdaptor.redisPort = redisPort;

    thisAdaptor.connectedOutlets = {};
    thisAdaptor.addGlobalErrorHandler();

    var cleanMessage = {
        scope:"broadcast",
        type:"start",
        nodeName:nodeName,
        instanceUID:thisAdaptor.instanceUID
    }

    // handle messages from redis
    pubsubRedisClient.on("message", function (channel, message) {
        //continue swarmingPhase
        var initVars = JSON.parse(message);
        if(initVars.scope == "broadcast"){
            thisAdaptor.onBroadcast(initVars);
        }
        else{
            thisAdaptor.onMessageFromQueue(initVars,message);
        }
    });

    thisAdaptor.loadSwarmingCode();
    return thisAdaptor;
}

AdaptorBase.prototype.onMessageFromQueue = function(initVars,rawMessage){
    var swarmingPhase = new SwarmingPhase(initVars.swarmingName,initVars.currentPhase);
    thisAdaptor.msgCounter++;
    for (var i in initVars){
        swarmingPhase[i] = initVars[i];
    }

    if(swarmingPhase.debug == "true"){
        console.log("[" +thisAdaptor.nodeName + "] received for [" + channel + "]: " + rawMessage);
    }
    var phaseFunction = thisAdaptor.compiledSwarmingDescriptions[swarmingPhase.swarmingName][swarmingPhase.currentPhase].code;
    if(phaseFunction != null){
        try{
            phaseFunction.apply(swarmingPhase);
        }
        catch (err){
            printPhaseError(err,swarmingPhase.swarmingName,swarmingPhase.currentPhase,swarmingPhase);
        }
    }
    else{
        if(thisAdaptor.onMessageCallback != null){
            thisAdaptor.onMessageCallback(message);
        }
        else{
                Console.log("DROPPING unknown message: " + rawMessage);
        }
    }
}

AdaptorBase.prototype.uploadDescriptions = function(descriptionsFolder){
    var files = fs.readdirSync(descriptionsFolder);

    files.forEach(function (fileName, index, array){
        var fullFileName = descriptionsFolder+"\\"+fileName;

        printDebugMessages("Uploading swarming:" + fileName);

        var content = fs.readFileSync(fullFileName);
        //console.log(this);
        redisClient.hset(thisAdaptor.mkUri("system","code"), fileName,content);

    });
}

AdaptorBase.prototype.readConfig = function(swarmingsFolder){
    var configContent = fs.readFileSync(swarmingsFolder+"\\core");
    thisAdaptor.adaptorConfig = JSON.parse(configContent);
    return thisAdaptor.adaptorConfig;
}

function printDebugMessages (msg){
    return false;
    console.log(msg);
}


AdaptorBase.prototype.mkUri = function(type,value){
    return "swarming://"+type+"/"+value;
}


AdaptorBase.prototype.loadSwarmingCode =  function(){
    redisClient.hgetall(thisAdaptor.mkUri("system","code"),
        function (err, hash){
            for (var i in hash){
                printDebugMessages("Loading swarming phase:" + i);
                try
                {
                    var obj = eval(hash[i]);
                    if(obj != null)
                    {
                        thisAdaptor.compiledSwarmingDescriptions[i] = obj;
                    }
                    else
                    {
                        console.log("Failed to load " + i);
                    }
                    //console.log(thisAdaptor.compiledWaves[i]);
                }
                catch(err)
                {
                    console.log(err);
                }
            }
        });
}

function SwarmingPhase(swarmingName,phase){
    this.swarmingName       = swarmingName;
    this.currentPhase    = phase;
}

SwarmingPhase.prototype.swarm = function(phaseName,nodeHint){
    if(this.debug == "swarm"){
        console.log("Swarm debug: " + this.swarmingName +" phase: " + phaseName);
    }

    this.currentPhase = phaseName;
    var targetNodeName = nodeHint;
    if(nodeHint == undefined){
        targetNodeName = thisAdaptor.compiledSwarmingDescriptions[this.swarmingName][phaseName].node;
    }
    if(this.debug == "swarm"){
            console.log("[" +thisAdaptor.nodeName + "] is sending command to [" + targetNodeName + "]: " + JSON.stringify(this));
    }
    redisClient.publish(targetNodeName,JSON.stringify(this));
};

AdaptorBase.prototype.swarmBegin = function (swarmingName){
    var swarming = new SwarmingPhase(swarmingName,"start");
    //console.log(thisAdaptor.compiledWaves[swarmingName]);
    var initVars = thisAdaptor.compiledSwarmingDescriptions[swarmingName].vars;
    for (var i in initVars){
        swarming[i] = initVars[i];
    }
    swarming.command = "phase";
    var start = thisAdaptor.compiledSwarmingDescriptions[swarmingName]["start"];
    var argsArray = Array.prototype.slice.call(arguments);
    argsArray.shift();
    try{
        start.apply(swarming,argsArray);
    }
    catch (err){
        printPhaseError(err,swarmingPhase.swarmingName,"start","none");
    }

}

SwarmingPhase.prototype.swarmBegin = AdaptorBase.prototype.swarm;


AdaptorBase.prototype.onBroadcast = function(message){
    if(message.type == "start" && message.instanceUID != thisAdaptor.instanceUID){
        console.log("["+thisAdaptor.nodeName+"] Forcing process exit because an node with the same name got alive!");
    process.exit(999);
    }
    if(thisAdaptor.onBroadcastCallback != null){
        thisAdaptor.onBroadcastCallback(message);
    }
}

//
//AdaptorBase.prototype.addOutlet = function(sessionId,outlet){
//    thisAdaptor.connectedOutlets[sessionId] = outlet;
//}

AdaptorBase.prototype.findOutlet = function(sessionId){
    return thisAdaptor.connectedOutlets[sessionId];
}



AdaptorBase.prototype.newOutlet = function(socketParam){
    var outlet={
        redisClient:null,
        socket:socketParam,
        sessionId:null,
        loginSwarmingVariables:null,
        waitingMsg:null,
        isClosed:false,
        onChannelNewMessage:function (channel, message) {
            //console.log("Waw: " + message);
            util.writeSizedString(this.socket,message);
        },
        successfulLogin:function (swarmingVariables) {

            this.loginSwarmingVariables = swarmingVariables;
            this.currentExecute = this.executeSafe;
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

            outlet.waitingMsg = messageObj;;
            this.redisClient.on("connect",function(){
                    this.executeButNotAuthenticated(this.waitingMsg);
            }.bind(this));
        },
        executeButNotAuthenticated : function (messageObj){
            if(messageObj.swarmingName != thisAdaptor.loginSwarmingName ){
                Console.log("Could not execute [" +messageObj.swarmingName +"] swarming without being logged in");
                this.close();
            }
            else{
                this.executeSafe(messageObj);
            }
        },
        executeSafe : function (messageObj){
                if(messageObj.command == "start"){
                    var swarming = new SwarmingPhase(messageObj.swarmingName,"start");
                    //console.log("Execute debug: " + JSON.stringify(messageObj))
                    //console.log(thisAdaptor.compiledWaves[swarmingName]);
                    var initVars = thisAdaptor.compiledSwarmingDescriptions[messageObj.swarmingName].vars;
                    for (var i in initVars){
                        swarming[i] = initVars[i];
                    }
                    for (var i in messageObj){
                        swarming[i] = messageObj[i];
                    }
                    swarming.command = "phase";
                        var start = thisAdaptor.compiledSwarmingDescriptions[messageObj.swarmingName]["start"];
                    var args = messageObj.commandArguments;
                    delete swarming.commandArguments;
                    try{
                        start.apply(swarming,args);
                    }
                    catch (err){
                        printPhaseError(err,messageObj.swarmingName,"start","none");
                    }
                }
                else
                if(messageObj.command == "phase"){
                    var swarming = new SwarmingPhase(messageObj.swarmingName,messageObj);
                    swarming.swarm(swarming.currentPhase);
                }
                else{
                    Console.log("["+thisAdaptor.nodeName +"] I don't know what to execute "+ JSON.stringify(messageObj));
                }
            }
    };
    outlet.currentExecute = outlet.executeButNotIdentified;
    var parser = util.createFastParser(outlet.execute.bind(outlet));

    socketParam.on('data', function (data){
        parser.parseNewData(data.toString('utf8'));
        //parser.parseNewData(data.toString('utf8');
    });

    socketParam.on('error',outlet.close.bind(outlet));
    socketParam.on('close',outlet.close.bind(outlet));

    return outlet;
}


AdaptorBase.prototype.addAPI = function(functionName,apiFunction){
    SwarmingPhase.prototype.functionName = apiFunction;
}

function printPhaseError(err,swarm,phase,myThis){
    console.log("ERROR caught in ["+ thisAdaptor.nodeName + "] for swarm \"" + swarm + "\" phase {"+ phase+"} Context:\n" + nutil.inspect(myThis));
    //if( err  instanceof ReferenceError){
        console.log(err.stack);
}

AdaptorBase.prototype.addGlobalErrorHandler = function(){
    process.on('uncaughtException', function(err) {
        console.log(err.stack);
    });
}

