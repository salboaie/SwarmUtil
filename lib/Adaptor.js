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

var thisAdaptor = null;

var BROADCAST_NODE_NAME = "BROADCAST";

exports.init = function(nodeName,redisHost,redisPort,descriptionFolder){
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
    addGlobalErrorHandler();

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
            onBroadcast(initVars);
        }
        else{
            onMessageFromQueue(initVars,message);
        }
    });

    if(nodeName == "Core"){
        uploadDescriptions(descriptionsFolder);
    }
    loadSwarmingCode();
    return thisAdaptor;
}

function onMessageFromQueue(initVars,rawMessage){
    var swarmingPhase = new SwarmingPhase(initVars.swarmingName,initVars.currentPhase);
    thisAdaptor.msgCounter++;
    for (var i in initVars){
        swarmingPhase[i] = initVars[i];
    }

    if(swarmingPhase.debug == "true"){
        cprint("[" +thisAdaptor.nodeName + "] received for [" + channel + "]: " + rawMessage);
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

function uploadDescriptions (descriptionsFolder){
    var files = fs.readdirSync(descriptionsFolder);

    files.forEach(function (fileName, index, array){
        var fullFileName = descriptionsFolder+"\\"+fileName;

        dprint("Uploading swarming:" + fileName);

        var content = fs.readFileSync(fullFileName);
        redisClient.hset(mkUri("system","code"), fileName,content);

    });
}

function mkUri(type,value){
    return "swarming://"+type+"/"+value;
}


function loadSwarmingCode(){
    redisClient.hgetall(mkUri("system","code"),
        function (err, hash){
            for (var i in hash){
                dprint("Loading swarming phase:" + i);
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
                    perror(err,"*** Syntax error in swarming description: " + i);
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
        cprint("Swarm debug: " + this.swarmingName +" phase: " + phaseName);
    }

    this.currentPhase = phaseName;
    var targetNodeName = nodeHint;
    if(nodeHint == undefined){
        targetNodeName = thisAdaptor.compiledSwarmingDescriptions[this.swarmingName][phaseName].node;
    }
    if(this.debug == "swarm"){
            cprint("[" +thisAdaptor.nodeName + "] is sending command to [" + targetNodeName + "]: " + JSON.stringify(this));
    }
    redisClient.publish(targetNodeName,JSON.stringify(this));
};

startSwarm = function(swarmingName,ctorName){

    var swarming = new SwarmingPhase(swarmingName,ctorName);
    //console.log(thisAdaptor.compiledWaves[swarmingName]);
    var initVars = thisAdaptor.compiledSwarmingDescriptions[swarmingName].vars;
    for (var i in initVars){
        swarming[i] = initVars[i];
    }
    swarming.command = "phase";
    var start = thisAdaptor.compiledSwarmingDescriptions[swarmingName][ctorName];
    var argsArray = Array.prototype.slice.call(arguments,1);
    argsArray.shift();
    try{
        start.apply(swarming,argsArray);
    }
    catch (err){
        perror(err,"Ctor error caught in ["+ thisAdaptor.nodeName + "] for swarm \"" + swarmingName + "\" phase {"+ ctorName+"} Context:\n" + nutil.inspect(swarming),true);
    }
}

//SwarmingPhase.prototype.swarmBegin = AdaptorBase.prototype.swarm;


 function onBroadcast(message){
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

findOutlet = function(sessionId){
    return thisAdaptor.connectedOutlets[sessionId];
}



function newOutlet(socketParam){
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

                    var ctorName = "start";
                    if(messageObj.ctor != undefined){
                        ctorName = messageObj.ctor;
                    }

                    var swarming = new SwarmingPhase(messageObj.swarmingName,ctorName);
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


/*
AdaptorBase.prototype.addAPI = function(functionName,apiFunction){
    SwarmingPhase.prototype.functionName = apiFunction;
}  */


function addGlobalErrorHandler (){
    process.on('uncaughtException', function(err) {
        perror(err);
    });
}

