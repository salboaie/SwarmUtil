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

thisAdapter = null;

var BROADCAST_NODE_NAME = "BROADCAST";

exports.init = function (nodeName, onReadyCallback,onSleepCanExecuteCallback) {
    cprint("Starting adapter " + nodeName);

    thisAdapter                 = new AdaptorBase(nodeName);
    thisAdapter.onReadyCallback = onReadyCallback;

    thisAdapter.instaceUID      =   uuid.v4();

    if(onSleepCanExecuteCallback == undefined){
        thisAdapter.onSleepCanExecuteCallback = default_onSleepCanExecute;
    }
    thisAdapter.isSleeping      = false;


    var basePath = process.env.SWARM_PATH;
    if(process.env.SWARM_PATH == undefined){
        console.log("Please set SWARM_PATH variable to your installation folder");
        process.exit(-1);
    }

    util.addGlobalErrorHandler();

    var basicConfigFile             = basePath + "/etc/config";
    thisAdapter.config              = util.readConfig(basicConfigFile);
    thisAdapter.redisHost           = thisAdapter.config.Core.redisHost;
    thisAdapter.redisPort           = thisAdapter.config.Core.redisPort;
    thisAdapter.coreId              = thisAdapter.config.Core.coreId;

    redisClient             = redis.createClient(thisAdapter.redisPort, thisAdapter.redisHost);

    redisClient.on("error",onRedisError);
    redisClient.on("connect",onRedisConnect);

    pubsubRedisClient       = redis.createClient(thisAdapter.redisPort, thisAdapter.redisHost);

    pubsubRedisClient.on("error",onRedisError);
    pubsubRedisClient.on("connect",onRedisConnect);


    thisAdapter.compiledSwarmingDescriptions    = [];
    thisAdapter.connectedOutlets                ={};

    thisAdapter.msgCounter                      = 0;

    var channel = thisAdapter.coreId + nodeName;
    dprint("Subscribing to channel " + channel );
    pubsubRedisClient.subscribe(channel);


    // handle messages from redis
    pubsubRedisClient.on("message", function (channel, message) {
        //continue swarmingPhase
        var initVars = JSON.parse(message);
        if(!thisAdapter.isSleeping || thisAdapter.onSleepCanExecuteCallback(initVars)){
            onMessageFromQueue(initVars, message);
        }
    });

    if (nodeName == "Core") {
        thisAdapter.descriptionsFolder = basePath + "/" + thisAdapter.config.Core.swarmsfolder;
        uploadDescriptions();
        loadSwarmingCode();
    }

    return thisAdapter;
}


function default_onSleepCanExecute(initVars){
    if(initVars.swarmingName == "NodeStart.js"){
        return true;
    }
    return false;
}


function onRedisError(event){
    logErr("Redis connection error! Please start redis server or check your configurations!",event);
}


var count=0;
function onRedisConnect(event) {
    count++;
    if(count == 2){
        if (thisAdapter.nodeName != "Core") {

        loadSwarmingCode( function() {
            startSwarm("CodeUpdate.js","register",thisAdapter.nodeName);
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

    if (swarmingPhase.debug == true || thisAdapter.verbose == true) {
        logDebug("[" + thisAdapter.nodeName + "] received message: " + rawMessage);
    }

    var phaseFunction = thisAdapter.compiledSwarmingDescriptions[swarmingPhase.swarmingName][swarmingPhase.currentPhase].code;
    if (phaseFunction != null) {
        try {
            phaseFunction.apply(swarmingPhase);
        }
        catch (err) {
            logErr("Syntax error when running swarm code! Phase: " + swarmingPhase.currentPhase, err);
        }
    }
    else {
        if (thisAdapter.onMessageCallback != null) {
            thisAdapter.onMessageCallback(message);
        }
        else {
            logInfo("DROPPING unknown swarming message!", rawMessage);
        }
    }
    endContext();
}





function uploadDescriptions() {
    var files = fs.readdirSync(thisAdapter.descriptionsFolder);

    files.forEach(function (fileName, index, array) {
        var fullFileName = thisAdapter.descriptionsFolder + "/" + fileName;
        fs.watch(fullFileName, function (event,fileName){
            uploadFile(fileName);
            startSwarm("CodeUpdate.js","swarmChanged",fileName);
        });
        uploadFile(fileName);
    });
}

function uploadFile(fileName){
    try{
        var fullFileName = thisAdapter.descriptionsFolder + "/" + fileName;
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

            if(thisAdapter.onReadyCallback != undefined){
                thisAdapter.onReadyCallback();
            }

            startSwarm("NodeStart.js","boot");
        });
}


function compileSwarm(swarmName,swarmDescription){
    try {
        var obj = eval(swarmDescription);
        if (obj != null) {
            thisAdapter.compiledSwarmingDescriptions[swarmName] = obj;
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

AdaptorBase.prototype.newSwarmingPhase = function (swarmingName, phase){
  return new SwarmingPhase(swarmingName, phase);
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
            targetNodeName = thisAdapter.compiledSwarmingDescriptions[this.swarmingName][phaseName].node;
        }

        if(this.debug == true || thisAdapter.verbose == true){
            logInfo("Starting a swarm towards " + targetNodeName + " , Message: " + J(this));
        }

        if(targetNodeName != undefined){
            redisClient.publish(thisAdapter.coreId+targetNodeName, JSON.stringify(this),function (err,res){
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
        logErr("Phase is {" + phaseName + "} nodeHint is {" + targetNodeName +"}" + J(thisAdapter.compiledSwarmingDescriptions[this.swarmingName]),err);
    }

};

startSwarm = function (swarmingName, ctorName) {
    try {
        var swarming = new SwarmingPhase(swarmingName, ctorName);
        var initVars = thisAdapter.compiledSwarmingDescriptions[swarmingName].vars;
        for (var i in initVars) {
            swarming[i] = initVars[i];
        }
        swarming.command    = "phase";
        swarming.tenantId   = getCurrentTenant();
        var start = thisAdapter.compiledSwarmingDescriptions[swarmingName][ctorName];

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
    thisAdapter.isSleeping = true;
}

AdaptorBase.prototype.awakeExecution = function(){
    thisAdapter.isSleeping = false;
}

//SwarmingPhase.prototype.swarmBegin = AdaptorBase.prototype.swarm;

