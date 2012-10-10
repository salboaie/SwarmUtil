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


    var basePath = process.env.SWARM_PATH;
    if(process.env.SWARM_PATH == undefined){
        util.delayExit("Please set SWARM_PATH variable to your installation folder",1000);
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

    var channel = util.mkChannelUri(nodeName);
    dprint("Subscribing to channel " + channel );
    pubsubRedisClient.subscribe(channel);
    pubsubRedisClient.on("subscribe",onPubSubRedisReady);

    // handle messages from redis
    pubsubRedisClient.on("message", function (channel, message) {
        //continue swarmingPhase
        var initVars = JSON.parse(message);
        if(thisAdapter.nodeName == "Null*"){
            cprint("Error: Null adapter received " + message);
        }
        else
        if(!thisAdapter.isSleeping || thisAdapter.onSleepCanExecuteCallback(initVars)){
            onMessageFromQueue(initVars);
        }
    });

    thisAdapter.swarmingCodeLoaded = false;
    return thisAdapter;
}


function default_onSleepCanExecute(initVars){
    if(initVars.meta.swarmingName == "NodeStart.js"){
        return true;
    }
    return false;
}


function onRedisError(event){
    aprint("Redis connection error! Please start redis server or check your configurations!" + event.stack);
}



function loadSwarms(){
    if(thisAdapter.swarmingCodeLoaded == false){
        cprint("Requesting swarm descriptions....");
        loadSwarmingCode( function() {
            startSwarm("CodeUpdate.js","register",thisAdapter.nodeName);
            startSwarm("NodeStart.js","boot");
            if(thisAdapter.onReadyCallback != undefined){
                thisAdapter.onReadyCallback();
            }
            thisAdapter.swarmingCodeLoaded = true;
        });
    }

    setTimeout(function (){
        if(thisAdapter.swarmingCodeLoaded == false){
            loadSwarms();
        }
    },500);
}



function loadSwarmingCode(onEndFunction) {
    //redisClient.set("a","b");
/*    redisClient.echo("blabala",function (err,ret) {
        cprint(ret);
    });
*/
    redisClient.hgetall(util.mkUri("system", "code"),
        function (err,hash) {
            if(err != null){
                logErr("Error loadig swarms descriptions\n",err);
            }
            else{
                cprint("Processing swarms descriptions...");
            }

            for (var i in hash) {
                compileSwarm(i,hash[i]);
            }
            if(onEndFunction != undefined){
                onEndFunction();
            }
        });
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



function uploadDescriptions() {

    var folders = thisAdapter.config.Core.paths;


    for(var i=0; i<folders.length; i++){

        if(folders[i].enabled == undefined || folders[i].enabled == true){
            var descriptionsFolder =  folders[i].folder;

            var files = fs.readdirSync(getSwarmFilePath(descriptionsFolder));
            files.forEach(function (fileName, index, array) {

                var fullFileName = getSwarmFilePath(descriptionsFolder+"/"+fileName);
                fs.watch(fullFileName, function (event,fileName){
                    if(uploadFile(fullFileName,fileName)){
                        startSwarm("CodeUpdate.js","swarmChanged",fileName);
                    }
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
        redisClient.hset(util.mkUri("system", "code"), fileName, content);
        dprint("Uploading swarm: " + fileName);
        compileSwarm(fileName, content.toString());
        //cprint(fileName + " \n "+ content);
    }
    catch(err){
        return false;
        //logErr("Failed uploading swarm file ", err);
    }
    return true;
}


function compileSwarm(swarmName,swarmDescription,verbose){
    dprint("Loading swarm " + swarmName);
    try {
        if(verbose){
            cprint(swarmDescription);
        }
        var obj = eval(swarmDescription);
        if (obj != null) {
              /*
                var xn = null;
                if(obj.meta != undefined){

                    xn = obj.meta.name;
                }
                else{
                    xn = "noMeta";
                }
                if(xn != swarmName){
                    cprint("Name should be " + swarmName + "but is " + xn);
                }
            */
            thisAdapter.compiledSwarmingDescriptions[swarmName] = obj;
        }
        else {
            logErr("Failed to load swarming description: " + swarmName);
        }
    }
    catch (err) {
        logErr("Syntax error in swarm description: " + swarmName + "\n"+swarmDescription,err);
    }
    thisAdapter.readyForSwarm = true;
}

AdaptorBase.prototype.reloadSwarm = function(swarmName){
    redisClient.hget(util.mkUri("system", "code"),swarmName,function (err, value) {
        compileSwarm(swarmName,value,true);
    });
}


function onMessageFromQueue(initVars) {
    var swarmingPhase = util.newSwarmPhase(initVars.meta.swarmingName, initVars.meta.currentPhase);
    for (var i in initVars) {
        swarmingPhase[i] = initVars[i];
    }

    if (swarmingPhase.meta.debug == true) {
        cprint("[" + thisAdapter.nodeName + "] executing message: \n" + M(initVars));
    }


    var reportSucces = swarmingPhase.meta.pleaseConfirm;
    swarmingPhase.meta.pleaseConfirm = false;
    //swarmingPhase.meta.fromNode = thisAdapter.nodeName;

    var cswarm = thisAdapter.compiledSwarmingDescriptions[swarmingPhase.meta.swarmingName];
    if(swarmingPhase.meta.swarmingName == undefined || cswarm == undefined){
        logErr("Unknown swarm requested by another node: " + swarmingPhase.meta.swarmingName);
        return;
    }

    beginExecutionContext(initVars);
    try{
        var phaseFunction = thisAdapter.compiledSwarmingDescriptions[swarmingPhase.meta.swarmingName][swarmingPhase.meta.currentPhase].code;
        if (phaseFunction != null) {
            try {
                phaseFunction.apply(swarmingPhase);
            }
            catch (err) {
                logErr("Syntax error when running swarm code! Phase: " + swarmingPhase.meta.currentPhase, err);
                reportSucces = false;
            }
        }
        else {
            if (thisAdapter.onMessageCallback != null) {
                thisAdapter.onMessageCallback(message);
            }
            else {
                logInfo("DROPPING unknown swarming message!" + J(initVars));
                reportSucces = false;
            }
        }
    }
    catch(err){
        logErr("Error running swarm : " + swarmingPhase.meta.swarmingName + " Phase:" + swarmingPhase.meta.currentPhase, err);
        reportSucces = false;
    }
    endExecutionContext();

    if(reportSucces == true){
        startSwarm("ConfirmExecution.js","confirm",swarmingPhase);
    }
}
exports.onMessageFromQueueCallBack = onMessageFromQueue;


startRemoteSwarm = function (targetAdapter, targetSession, swarmingName, ctorName, calback ){
    startSwarm("startRemoteSwarm.js", "start", targetAdapter, targetSession, swarmingName, ctorName, calback, arguments);
}

startSwarm = function (swarmingName, ctorName) {
    try {
        var swarming = util.newSwarmPhase(swarmingName, ctorName);
        if(thisAdapter.compiledSwarmingDescriptions[swarmingName] == undefined){
                logErr("Unknown swarm  "  + swarmingName );
            return ;
        }
        swarming.meta.command    = "phase";
        swarming.meta.tenantId   = getCurrentTenant();
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



