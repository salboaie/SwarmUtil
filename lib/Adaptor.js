/**
 * Created by: sinica
 * Date: 6/7/12
 * Time: 11:36 PM
 */


var redis = require("redis");
var fs = require('fs');
var util = require("swarmutil");
var nutil = require("util");

function AdaptorBase(nodeName) {
    this.nodeName = nodeName;
}

var thisAdaptor = null;

var BROADCAST_NODE_NAME = "BROADCAST";

exports.init = function (nodeName, redisHost, redisPort, descriptionFolder) {
    console.log("Starting adaptor " + nodeName);
    thisAdaptor = new AdaptorBase(nodeName);
    redisClient = redis.createClient(redisPort, redisHost);
    pubsubRedisClient = redis.createClient(redisPort, redisHost);
    thisAdaptor.instanceUID = "UID:" + Date.now() + Math.random() + Math.random() + Math.random();

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
        if (initVars.scope == "broadcast") {
            onBroadcast(initVars);
        }
        else {
            onMessageFromQueue(initVars, message);
        }
    });

    if (nodeName == "Core") {
        uploadDescriptions(descriptionsFolder);
    }
    loadSwarmingCode();
    return thisAdaptor;
}

function onMessageFromQueue(initVars, rawMessage) {
    var swarmingPhase = new SwarmingPhase(initVars.swarmingName, initVars.currentPhase);
    thisAdaptor.msgCounter++;
    for (var i in initVars) {
        swarmingPhase[i] = initVars[i];
    }
    if (swarmingPhase.debug == "true") {
        cprint("[" + thisAdaptor.nodeName + "] received from channel [" + initVars.channel + "]: " + rawMessage);
    }
    var phaseFunction = thisAdaptor.compiledSwarmingDescriptions[swarmingPhase.swarmingName][swarmingPhase.currentPhase].code;
    if (phaseFunction != null) {
        try {
            phaseFunction.apply(swarmingPhase);
        }
        catch (err) {
            util.perror(err, swarmingPhase.swarmingName, swarmingPhase.currentPhase, swarmingPhase);
        }
    }
    else {
        if (thisAdaptor.onMessageCallback != null) {
            thisAdaptor.onMessageCallback(message);
        }
        else {
            Console.log("DROPPING unknown message: " + rawMessage);
        }
    }
}

function uploadDescriptions(descriptionsFolder) {
    var files = fs.readdirSync(descriptionsFolder);

    files.forEach(function (fileName, index, array) {
        var fullFileName = descriptionsFolder + "\\" + fileName;

        dprint("Uploading swarming:" + fileName);

        var content = fs.readFileSync(fullFileName);
        redisClient.hset(mkUri("system", "code"), fileName, content);

    });
}

function mkUri(type, value) {
    return "swarming://" + type + "/" + value;
}


function loadSwarmingCode() {
    redisClient.hgetall(mkUri("system", "code"),
        function (err, hash) {
            for (var i in hash) {
                dprint("Loading swarming phase:" + i);
                try {
                    var obj = eval(hash[i]);
                    if (obj != null) {
                        thisAdaptor.compiledSwarmingDescriptions[i] = obj;
                    }
                    else {
                        console.log("Failed to load " + i);
                    }
                    //console.log(thisAdaptor.compiledWaves[i]);
                }
                catch (err) {
                    perror(err, "*** Syntax error in swarming description: " + i);
                }
            }
        });
}

function SwarmingPhase(swarmingName, phase) {
    this.swarmingName = swarmingName;
    this.currentPhase = phase;
}

exports.SwarmingPhase = SwarmingPhase;
SwarmingPhase.prototype.swarm = function (phaseName, nodeHint) {
    try{
        if (this.debug == "swarm") {
            cprint("Swarm debug: " + this.swarmingName + " phase: " + phaseName);
        }

        this.currentPhase = phaseName;
        var targetNodeName = nodeHint;
        if (nodeHint == undefined) {
            targetNodeName = thisAdaptor.compiledSwarmingDescriptions[this.swarmingName][phaseName].node;
        }
        if (this.debug == "swarm") {
            cprint("[" + thisAdaptor.nodeName + "] is sending command to [" + targetNodeName + "]: " + JSON.stringify(this));
        }
        redisClient.publish(targetNodeName, JSON.stringify(this));
    }
    catch(err) {
        console.log("Error in Adaptor : "+ thisAdaptor.nodeName + "Phase: " + phaseName + " nodeHint: " + nodeHint );
        util.perror(err);
    }

};

startSwarm = function (swarmingName, ctorName) {

    var swarming = new SwarmingPhase(swarmingName, ctorName);
    //console.log(thisAdaptor.compiledWaves[swarmingName]);
    var initVars = thisAdaptor.compiledSwarmingDescriptions[swarmingName].vars;
    for (var i in initVars) {
        swarming[i] = initVars[i];
    }
    swarming.command = "phase";
    var start = thisAdaptor.compiledSwarmingDescriptions[swarmingName][ctorName];
    var argsArray = Array.prototype.slice.call(arguments, 1);
    argsArray.shift();
    try {
        start.apply(swarming, argsArray);
    }
    catch (err) {
        perror(err, "Ctor error caught in [" + thisAdaptor.nodeName + "] for swarm \"" + swarmingName + "\" phase {" + ctorName + "} Context:\n" + nutil.inspect(swarming), true);
    }
}

//SwarmingPhase.prototype.swarmBegin = AdaptorBase.prototype.swarm;


function onBroadcast(message) {
    if (message.type == "start" && message.instanceUID != thisAdaptor.instanceUID) {
        console.log("[" + thisAdaptor.nodeName + "] Forcing process exit because an node with the same name got alive!");
        process.exit(999);
    }
    if (thisAdaptor.onBroadcastCallback != null) {
        thisAdaptor.onBroadcastCallback(message);
    }
}

//
//AdaptorBase.prototype.addOutlet = function(sessionId,outlet){
//    thisAdaptor.connectedOutlets[sessionId] = outlet;
//}

findOutlet = function (sessionId) {
    return thisAdaptor.connectedOutlets[sessionId];
}

AdaptorBase.prototype.findOutlet = findOutlet;

/*
 AdaptorBase.prototype.addAPI = function(functionName,apiFunction){
 SwarmingPhase.prototype.functionName = apiFunction;
 }  */


function addGlobalErrorHandler() {
    process.on('uncaughtException', function (err) {
        util.perror(err);
    });
}

