/**
 * Created by: sinica
 * Date: 6/7/12
 * Time: 11:36 PM
 */


var redis = require("redis");
var fs = require('fs');
var util = require("swarmutil");
var nutil = require("util");
var uuid = require('node-uuid');

function AdaptorBase(nodeName) {
    this.nodeName = nodeName;
}

thisAdaptor = null;

var BROADCAST_NODE_NAME = "BROADCAST";

exports.init = function (nodeName, onReadyCallback,onSleepCanExecuteCallback) {
    cprint("Starting adaptor " + nodeName);

    thisAdaptor                 = new AdaptorBase(nodeName);
    thisAdaptor.onReadyCallback = onReadyCallback;

    thisAdaptor.instaceUID      =   uuid.v4();

    if(onSleepCanExecuteCallback == undefined){
        thisAdaptor.onSleepCanExecuteCallback = default_onSleepCanExecute;
    }
    thisAdaptor.isSleeping      = false;


    var basePath = process.env.SWARM_PATH;
    if(process.env.SWARM_PATH == undefined){
        console.log("Please set SWARM_PATH variable to your installation folder");
        process.exit(-1);
    }

    util.addGlobalErrorHandler();

    var basicConfigFile             = basePath + "/etc/config";
    thisAdaptor.config              = util.readConfig(basicConfigFile);
    thisAdaptor.redisHost           = thisAdaptor.config.Core.redisHost;
    thisAdaptor.redisPort           = thisAdaptor.config.Core.redisPort;
    thisAdaptor.coreId              = thisAdaptor.config.Core.coreId;

    redisClient             = redis.createClient(thisAdaptor.redisPort, thisAdaptor.redisHost);

    redisClient.on("error",onRedisError);
    redisClient.on("connect",onRedisConnect);

    pubsubRedisClient       = redis.createClient(thisAdaptor.redisPort, thisAdaptor.redisHost);

    pubsubRedisClient.on("error",onRedisError);
    pubsubRedisClient.on("connect",onRedisConnect);


    thisAdaptor.compiledSwarmingDescriptions    = [];
    thisAdaptor.connectedOutlets                ={};

    thisAdaptor.msgCounter                      = 0;

    var channel = thisAdaptor.coreId + nodeName;
    dprint("Subscribing to channel " + channel );
    pubsubRedisClient.subscribe(channel);


    // handle messages from redis
    pubsubRedisClient.on("message", function (channel, message) {
        //continue swarmingPhase
        var initVars = JSON.parse(message);
        if(!thisAdaptor.isSleeping || thisAdaptor.onSleepCanExecuteCallback(initVars)){
            onMessageFromQueue(initVars, message);
        }
    });

    if (nodeName == "Core") {
        thisAdaptor.descriptionsFolder = basePath + "/" + thisAdaptor.config.Core.swarmsfolder;
        uploadDescriptions();
        loadSwarmingCode();
    }

    return thisAdaptor;
}


function default_onSleepCanExecute(initVars){
    if(initVars.swarmingName == "NodeStart.js"){
        return true;
    }
    return false;
}


function onRedisError(event){
      cprint("Redis connection error! TODO: what? ");
}


var count=0;
function onRedisConnect(event) {
    count++;
    if(count == 2){
        if (thisAdaptor.nodeName != "Core") {

        loadSwarmingCode( function() {
            startSwarm("CodeUpdate.js","register",thisAdaptor.nodeName);
            });
        }
    }
}



function onMessageFromQueue(initVars, rawMessage) {
    beginContext(initVars);
    var swarmingPhase = new SwarmingPhase(initVars.swarmingName, initVars.currentPhase);

    for (var i in initVars) {
        swarmingPhase[i] = initVars[i];
    }

    if (swarmingPhase.debug == "true") {
        logDebug("[" + thisAdaptor.nodeName + "] received message: " + rawMessage);
    }

    var phaseFunction = thisAdaptor.compiledSwarmingDescriptions[swarmingPhase.swarmingName][swarmingPhase.currentPhase].code;
    if (phaseFunction != null) {
        try {
            phaseFunction.apply(swarmingPhase);
        }
        catch (err) {
            logErr("Syntax error when running swarm code!", err);
        }
    }
    else {
        if (thisAdaptor.onMessageCallback != null) {
            thisAdaptor.onMessageCallback(message);
        }
        else {
            logInfo("DROPPING unknown swarming message!", rawMessage);
        }
    }
    endContext();
}





function uploadDescriptions() {
    var files = fs.readdirSync(thisAdaptor.descriptionsFolder);

    files.forEach(function (fileName, index, array) {
        var fullFileName = thisAdaptor.descriptionsFolder + "/" + fileName;
        fs.watch(fullFileName, function (event,fileName){
            uploadFile(fileName);
            startSwarm("CodeUpdate.js","swarmChanged",fileName);
        });
        uploadFile(fileName);
    });
}

function uploadFile(fileName){
    try{
        var fullFileName = thisAdaptor.descriptionsFolder + "/" + fileName;
        var content = fs.readFileSync(fullFileName);
        redisClient.hset(mkUri("system", "code"), fileName, content);
    }
    catch(err){
        perror(err);
    }
}



function mkUri(type, value) {
    return "swarming://" + type + "/" + value;
}


function loadSwarmingCode(onEndFunction) {
    redisClient.hgetall(mkUri("system", "code"),
        function (err, hash) {
            for (var i in hash) {
                dprint("Loading swarming phase:" + i);
                compileSwarm(i,hash[i]);
            }

            if(onEndFunction != undefined){
                onEndFunction();
            }

            if(thisAdaptor.onReadyCallback != undefined){
                thisAdaptor.onReadyCallback();
            }

            startSwarm("NodeStart.js","boot");
        });
}


function compileSwarm(swarmName,swarmDescription){
    try {
        var obj = eval(swarmDescription);
        if (obj != null) {
            thisAdaptor.compiledSwarmingDescriptions[swarmName] = obj;
        }
        else {
            logErr("Failed to load swarming description: " + swarmName);
        }
    }
    catch (err) {
        logErr(" Syntax error in swarming description: " + swarmName,err);
    }
}

AdaptorBase.prototype.reloadSwarm = function(swarmName){
    redisClient.hget(mkUri("system", "code"),swarmName,function (err, value) {
        compileSwarm(swarmName,value);
    });
}


function SwarmingPhase(swarmingName, phase) {
    this.swarmingName = swarmingName;
    this.currentPhase = phase;
}

exports.SwarmingPhase = SwarmingPhase;
SwarmingPhase.prototype.swarm = function (phaseName, nodeHint) {
    try{
        /*
        if(this.tenantId == undefined || this.tenantId == null){
            this.tenantId = "WarningNoTenant";
            //logInfo("Warning: tenantId should be set. Hope this error don't happen in production mode!");
        }   */
        this.currentPhase = phaseName;
        var targetNodeName = nodeHint;
        if (nodeHint == undefined) {
            targetNodeName = thisAdaptor.compiledSwarmingDescriptions[this.swarmingName][phaseName].node;
        }
        if(targetNodeName != undefined){
            redisClient.publish(thisAdaptor.coreId+targetNodeName, JSON.stringify(this),function (err,res){
                if(err != null){
                    perror(err);
                }
            });
        }
        else{
            logInfo("Unknown phase " + phaseName);
        }
    }
    catch(err) {
        logErr("Phase is {" + phaseName + "} nodeHint is {" + targetNodeName +"}" + J(thisAdaptor.compiledSwarmingDescriptions[this.swarmingName]),err);
    }

};

startSwarm = function (swarmingName, ctorName) {
    try {
        var swarming = new SwarmingPhase(swarmingName, ctorName);
        var initVars = thisAdaptor.compiledSwarmingDescriptions[swarmingName].vars;
        for (var i in initVars) {
            swarming[i] = initVars[i];
        }
        swarming.command    = "phase";
        swarming.tenantId   = getCurrentTenant();
        var start = thisAdaptor.compiledSwarmingDescriptions[swarmingName][ctorName];

        var args = []; // empty array
        // copy all other arguments we want to "pass through"
        for(var i = 2; i < arguments.length; i++){
            args.push(arguments[i]);
        }
        start.apply(swarming, args);
    }
    catch (err) {
        logErr("Error starting swarm "  + swarmingName + " you got some mistakes/errors in ctor's code", err);
    }
}


AdaptorBase.prototype.sleepExecution = function(){
    thisAdaptor.isSleeping = true;
}

AdaptorBase.prototype.awakeExecution = function(){
    thisAdaptor.isSleeping = false;
}

//SwarmingPhase.prototype.swarmBegin = AdaptorBase.prototype.swarm;

