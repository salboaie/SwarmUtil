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

exports.init = function (nodeName, onReadyCallback,onSleepCanExecuteCallback,verbose) {
    globalVerbosity = verbose;
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
        setTimeout(function (){
            process.exit(-1);
        },1000);
    }

    util.addGlobalErrorHandler();

    var basicConfigFile             = basePath + "/etc/config";
    thisAdapter.config              = util.readConfig(basicConfigFile);
    thisAdapter.redisHost           = thisAdapter.config.Core.redisHost;
    thisAdapter.redisPort           = thisAdapter.config.Core.redisPort;
    thisAdapter.coreId              = thisAdapter.config.Core.coreId;

    redisClient             = redis.createClient(thisAdapter.redisPort, thisAdapter.redisHost);

    redisClient.on("error",onRedisError);
    redisClient.on("ready",onCmdRedisReady);

    pubsubRedisClient       = redis.createClient(thisAdapter.redisPort, thisAdapter.redisHost);

    pubsubRedisClient.on("error",onRedisError);



    thisAdapter.compiledSwarmingDescriptions    = [];
    thisAdapter.connectedOutlets                = {};

    thisAdapter.msgCounter                      = 0;

    var channel = thisAdapter.coreId + nodeName;
    dprint("Subscribing to channel " + channel );
    pubsubRedisClient.subscribe(channel);
    pubsubRedisClient.on("subscribe",onPubSubRedisReady);


    // handle messages from redis
    pubsubRedisClient.on("message", function (channel, message) {
        //continue swarmingPhase
        var initVars = JSON.parse(message);
        if(!thisAdapter.isSleeping || thisAdapter.onSleepCanExecuteCallback(initVars)){
            onMessageFromQueue(initVars);
        }
    });

    thisAdapter.swarmingCodeLoaded = false;
    return thisAdapter;
}


function default_onSleepCanExecute(initVars){
    if(initVars.swarmingName == "NodeStart.js"){
        return true;
    }
    return false;
}


function onRedisError(event){
    aprint("Redis connection error! Please start redis server or check your configurations!" + event.stack);
}



function loadSwarms(){
    loadSwarmingCode( function() {
        startSwarm("CodeUpdate.js","register",thisAdapter.nodeName);
        startSwarm("NodeStart.js","boot");
        if(thisAdapter.onReadyCallback != undefined){
            thisAdapter.onReadyCallback();
        }
        thisAdapter.swarmingCodeLoaded = true;
    });

    setTimeout(function (){
        if(thisAdapter.swarmingCodeLoaded == false){
            cprint("Trying to load swarms descriptions....");
            loadSwarms();
        }
    },1000);
}

var count=0;
function onCmdRedisReady(event) {
    count++;
    if (thisAdapter.nodeName == "Core") {
        uploadDescriptions();
    }else{
        loadSwarms();
    }
    if(count == 2){  // both redis connections are ready
        thisAdapter.readyForSwarm = true;
    }
}

function onPubSubRedisReady(event) {
    count++;
    if(count == 2){  // both redis connections are ready
        thisAdapter.readyForSwarm = true;
    }
}


function onMessageFromQueue(initVars) {
    var swarmingPhase = new SwarmingPhase(initVars.swarmingName, initVars.currentPhase);
    for (var i in initVars) {
        swarmingPhase[i] = initVars[i];
    }

    var cswarm = thisAdapter.compiledSwarmingDescriptions[swarmingPhase.swarmingName];
    if(swarmingPhase.swarmingName == undefined || cswarm == undefined){
        logErr("Unknown swarm requested by another node: " + swarmingPhase.swarmingName);
        return;
    }

    beginContext(initVars);
    try{

        if (swarmingPhase.debug == true || thisAdapter.verbose == true) {
            logDebug("[" + thisAdapter.nodeName + "] received message: " + J(initVars));
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
                logInfo("DROPPING unknown swarming message!" + J(initVars));
            }
        }
    }
    catch(err){
        logErr("Error running swarm : " + swarmingPhase.swarmingName, err);
    }
    endContext();
}

function uploadDescriptions() {

    var folders = thisAdapter.config.Core.paths;


    for(var i=0; i<folders.length; i++){

        if(folders[i].enabled == undefined || folders[i].enabled == true){
            var descriptionsFolder =  folders[i].folder;

            var files = fs.readdirSync(getSwarmFilePath(descriptionsFolder));
            files.forEach(function (fileName, index, array) {

                var fullFileName = getSwarmFilePath(descriptionsFolder+"/"+fileName);
                fs.watch(fullFileName, function (event,fileName){
                    uploadFile(fullFileName,fileName);
                    startSwarm("CodeUpdate.js","swarmChanged",fileName);
                });
                uploadFile(fullFileName,fileName);
            });
        }
    }
    //startSwarm("NodeStart.js","boot");
}

function uploadFile(fullFileName,fileName){
    try{
        var content = fs.readFileSync(fullFileName);
        redisClient.hset(mkUri("system", "code"), fileName, content);
        dprint("Uploading swarm: " + fileName);
        compileSwarm(fileName, content.toString());
        //cprint(fileName + " \n "+ content);
    }
    catch(err){
        logErr("Failed uploading swarm file ", err);
    }
}



function mkUri(type, value) {
    var uri = thisAdapter.coreId + ":" + type + ":" + value;
    cprint("URI: " + uri);
    return uri;
}


function loadSwarmingCode(onEndFunction) {
    redisClient.hgetall(mkUri("system", "code"),
        function (err,hash) {
            if(err != null){
                logErr("Error loadig swarms descriptions\n",err);
            }
            else{
                cprint("Loading swarms...");
            }

            for (var i in hash) {
                dprint("Loading swarm from Redis: " + i);
                compileSwarm(i,hash[i]);
            }
            if(onEndFunction != undefined){
                onEndFunction();
            }
        });
}



function compileSwarm(swarmName,swarmDescription){
    try {
        var obj = eval(swarmDescription);
        if (obj != null) {
            thisAdapter.compiledSwarmingDescriptions[swarmName] = obj;
            //cprint(J(obj));
        }
        else {
            logErr("Failed to load swarming description: " + swarmName);
        }
    }
    catch (err) {
        logErr(" Syntax error in swarming description: " + swarmName,err);
    }
    thisAdapter.readyForSwarm = true;
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
      /*

var queue = new Array();

function consumeSwarm(){
    var rec = queue.shift();
    swarm = rec.swarm;
    try{
        onMessageFromQueue(swarm);
        rec.funct(null,null);
    }
    catch(err){
        rec.funct(err,null);
    }
}


function publishSwarm(channel,swarm,funct){
    if(channel[0] == "#"){
        //local channel, just execute
        queue.push({"channel":channel,"swarm":swarm,"funct":funct});
        process.nextTick(consumeSwarm) ;
    }
    else{
        redisClient.publish(thisAdapter.coreId+channel, J(swarm),funct);
    }
} */


function consumeSwarm(channel,swarm,funct){
    return function(){
        try{
            onMessageFromQueue(swarm);
            funct(null,null);
        }
        catch(err){
            funct(err,null);
        }
    }
}
function publishSwarm(channel,swarm,funct){
    if(channel[0] == "#"){
        //local channel, just execute
        process.nextTick(consumeSwarm(channel,swarm,funct))
    }
    else{
        redisClient.publish(thisAdapter.coreId+channel, J(swarm),funct);
    }

}


exports.SwarmingPhase = SwarmingPhase;
SwarmingPhase.prototype.swarm = function (phaseName, nodeHint) {
    if(thisAdapter.readyForSwarm != true){
        cprint("Asynchonicity issue: redis is not ready for swarming " + phaseName);
        return;
    }
    try{
        if(thisAdapter.compiledSwarmingDescriptions[this.swarmingName] == undefined){
            logErr("Undefined swarm " + this.swarmingName);
            return;
        }

        this.currentPhase = phaseName;
        var targetNodeName = nodeHint;
        if (nodeHint == undefined) {
            targetNodeName = thisAdapter.compiledSwarmingDescriptions[this.swarmingName][phaseName].node;
        }

        if(this.debug == true || thisAdapter.verbose == true){
            logInfo("Starting swarm "+this.swarmingName +  " towards " + targetNodeName + ", Phase: "+ phaseName);
        }

        if(targetNodeName != undefined){
            publishSwarm(targetNodeName,this,function (err,res){
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
        logErr("Unknown error in phase {" + phaseName + "} nodeHint is {" + targetNodeName +"}" + J(thisAdapter.compiledSwarmingDescriptions[this.swarmingName]),err);
    }

};

SwarmingPhase.prototype.deleteTimeoutSwarm = function (timerRef) {
    //cleanTimeout(timerRef);
}


SwarmingPhase.prototype.timeoutSwarm = function (timeOut,phaseName, nodeHint) {
    var timeoutId = -1;
    try{
        var targetNodeName = nodeHint;
        if (nodeHint == undefined) {
            targetNodeName = thisAdapter.compiledSwarmingDescriptions[this.swarmingName][phaseName].node;
        }
        if(nodeHint == thisAdapter.nodeName ){
            var callBack =  thisAdapter.compiledSwarmingDescriptions[this.swarmingName][phaseName].code;
            if(typeof callBack == "function"){
                timeoutId = setTimeout(callBack.bind(this),timeOut);
            }else{
                logErr("Failed in setting timeout in swarm " + this.swarmingName + " because " +phaseName + " is not a phase", err);
            }
        }else{
            timeoutId = setTimeout(function (){
                this.swarm(phaseName,nodeHint);
            }.bind(this),timeOut);
        }
    }
    catch(err){
        logErr("Failed in setting timeout in swarm " + this.swarmingName, err);
    }
    return timeoutId;
}

startSwarm = function (swarmingName, ctorName) {
    try {
        var swarming = new SwarmingPhase(swarmingName, ctorName);
        if(thisAdapter.compiledSwarmingDescriptions[swarmingName] == undefined){
                logErr("Unknown swarm  "  + swarmingName );
            return ;
        }
        var initVars = thisAdapter.compiledSwarmingDescriptions[swarmingName].vars;
        for (var i in initVars) {
            swarming[i] = initVars[i];
        }
        swarming.command    = "phase";
        swarming.tenantId   = getCurrentTenant();
        var start = thisAdapter.compiledSwarmingDescriptions[swarmingName][ctorName];

        if(start == undefined){
            logErr("Unknown ctor  "  + ctorName + " in swarm " + swarmingName  );
            return ;
        }

        var args = []; // empty array
        // copy all other arguments we want to "pass through"
        for(var i = 2; i < arguments.length; i++){
            args.push(arguments[i]);
        }

        start.apply(swarming, args);
    }
    catch (err) {
        logErr("Error starting new swarm "  + swarmingName + " ctor:" + ctorName , err);
    }
}


AdaptorBase.prototype.sleepExecution = function(){
    thisAdapter.isSleeping = true;
}

AdaptorBase.prototype.awakeExecution = function(){
    thisAdapter.isSleeping = false;
}

//SwarmingPhase.prototype.swarmBegin = AdaptorBase.prototype.swarm;

