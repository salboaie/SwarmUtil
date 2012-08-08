
exports.adapter = require("./Adapter.js");
exports.createAdapter = exports.adapter.init;
var fs = require("fs");

exports.decimalToHex = function (d, padding) {
    var hex = Number(d).toString(16);
    padding = typeof (padding) === "undefined" || padding === null ? padding = 8 : padding;

    while (hex.length < padding) {
        hex = "0" + hex;
    }
    return "0x"+hex;
}


exports.newOutlet = require ("./Outlet.js").newOutlet;
exports.createClient = require ("./SwarmClient.js").createClient;

exports.createFastParser = function(callBack) {
    return new FastJSONParser(callBack);
}

var READ_SIZE  = 1;
var READ_JSON = 3;

var JSONMAXSIZE_CHARS_HEADER=11; //8+3    // 0xFFFFFFFF\n is the max size    : 0x00000001\n is the min size, ( not a valid JSON!)

function FastJSONParser(callBack){
    this.state = READ_SIZE;
    this.buffer = "";
    this.nextSize = 0;
    this.callBack = callBack;
}

FastJSONParser.prototype.parseNewData = function(data) {
    this.buffer += data;
    var doAgain = true;
    while(doAgain) {
        doAgain = false;
        if(this.state == READ_SIZE){
            if(this.buffer.length >= JSONMAXSIZE_CHARS_HEADER){
                this.nextSize = parseInt(this.buffer.substr(0,JSONMAXSIZE_CHARS_HEADER));
                this.buffer = this.buffer.substring(JSONMAXSIZE_CHARS_HEADER);
                this.state = READ_JSON;
            }
        }

        if(this.state == READ_JSON){
            if(this.buffer.length >= this.nextSize){
                var json = JSON.parse(this.buffer.substr(0,this.nextSize));
                this.callBack(json);
                this.buffer = this.buffer.substring(this.nextSize+1); // a new line should be passed after json
                doAgain = true;
                this.state = READ_SIZE;
            }
        }
    }
}

exports.writeObject = function(sock,object) {
 var str=JSON.stringify(object);
 exports.writeSizedString (sock,str);
}

exports.writeSizedString=function(sock,str) { //write size and JSON serialised form of the object
	var sizeLine=exports.decimalToHex(str.length)+"\n";
	sock.write(sizeLine);	
	sock.write(str+"\n");
    //console.log("Writing " + str);
}

var util = require("util");
cprint = console.log;

printf = function(){
    var args = []; // empty array
    // copy all other arguments we want to "pass through"
    for(var i = 0; i < arguments.length; i++){
        args.push(arguments[i]);
    }
    var out = util.format.apply(this,args);
    console.log(out);
}

sprintf = function(){
    var args = []; // empty array
    for(var i = 0; i < arguments.length; i++){
        args.push(arguments[i]);
    }
    return util.format.apply(this,args);
}


cprint = console.log;

dprint = function(txt){
    var verbose  = false;
    if(thisAdapter != undefined){
        verbose = thisAdapter.verbose;
    }
    if(verbose == true){
        console.log("DEBUG: ["+ thisAdapter.nodeName + "]:"+ txt);
    }
}

/*
perror = function(err, textOnly,printStack){
    if(textOnly != undefined){
        console.log("(*) Error in Adaptor : "+ thisAdapter.nodeName + "\n (**)Environment info [ " + textOnly + "]");
        console.log(" (Err)"+err);
        if(printStack != false){
            console.log(" (Stack)"+err.stack);
        }
        return;
    }
    console.log("Adaptor:" + thisAdapter.nodeName +"\n");
    console.log("\n---------------\nError: " + err);
    console.log(err);
    if(err && printStack == undefined){
        console.log(err.stack);
    }
    console.log("\n---------------\n");
} */

var executionContext={};
executionContext.tenantId = null;

beginContext = function (swarm){
    //printf("Begin context. SessionId: %s Swarm: %s Tenant: %s",swarm.sessionId, swarm.swarmingName,swarm.tenantId);
    executionContext.sessionId = swarm.sessionId;
    executionContext.swarmingName = swarm.swarmingName;
    executionContext.tenantId  = swarm.tenantId;
}

endContext = function(){
    executionContext.sessionId  = null;
    executionContext.tenantId   = null;
    executionContext.swarmingName = null;
}

getCurrentSession = function(){
    return executionContext.sessionId;
}

getCurrentTenant = function(){
    return executionContext.tenantId;
}

getCurrentSwarm = function(){
    return executionContext.swarmingName;
}


function VariablesContext(){

}

VariablesContext.prototype.getArray = function (name){
    if(this[name] == undefined){
        this[name] = [];
    }
    return this[name];
}

VariablesContext.prototype.getObject = function (name){
    if(this[name] == undefined){
        this[name] = {};
    }
    return this[name];
}


tenantsContexts = {};
globalContexts = {};

getContext = function(contextId,global){
    if(contextId == undefined){
        contextId = "thisAdapter";
    }

    if(global == true){
        var retCtxt = globalContexts[contextId];
        if(retCtxt == undefined){
            retCtxt = globalContexts[contextId] = new VariablesContext();
        }
        return retCtxt;
    }

    if(executionContext.tenantId != null){
        var tenantContext = tenantsContexts[executionContext.tenantId];
        if(tenantContext == undefined ){
            tenantContext = tenantsContexts[executionContext.tenantId] = new VariablesContext();
        }
        var retCtxt = tenantContext[contextId];
        if(retCtxt == undefined){
            retCtxt = tenantContext[contextId] = new VariablesContext();
        }
        return retCtxt;
    }
    return null;
}

var swarmToSwarmLevel = 0;

logErr = function(message,err){
    var errStr;
    var stack;
    cprint("Logging error: \n" + message);
    if(err != null && err != undefined){
        errStr = err.toString();
        stack = err.stack;
        console.log(stack);
    }
    cprint("\n");
    fs.appendFileSync(getSwarmFilePath(thisAdapter.config.logsFolder + "/" + thisAdapter.nodeName+".err"),errStr+"\n"+stack+"\n");

    swarmToSwarmLevel++;
    if(swarmToSwarmLevel <= 2){
        startSwarm("log.js","err","ERROR",message,errStr,stack,getCurrentSwarm());
        swarmToSwarmLevel--;
    }
}

logDebug = function(message,details,aspect){
    if(aspect == undefined){
        aspect = "DEBUG";
    }
    dprint("(**) Logging debug info: " + message);

    swarmToSwarmLevel++;
    if(swarmToSwarmLevel <= 2){
        startSwarm("log.js","debug",aspect, message, details,getCurrentSwarm());
        swarmToSwarmLevel--;
    }
}


logInfo = function(message,details,aspect){
    if(aspect == undefined){
        aspect = "INFO";
    }
    dprint("(*) Logging info: " + message);
    swarmToSwarmLevel++;
    if(swarmToSwarmLevel <= 2){
        startSwarm("log.js","info",aspect, message, details,getCurrentSwarm());
        swarmToSwarmLevel--;
    }
}

inspect = function (object){
    var out =   "----------------------------------------------------------------------\n"+
                util.inspect(object) +
                "----------------------------------------------------------------------\n";
    util.puts(out);
}

exports.inspect = inspect;

J = function(obj){
    return JSON.stringify(obj);
}

exports.readConfig = function(configFile) {
    try{
        var configContent = fs.readFileSync(configFile);
        cfg =  JSON.parse(configContent);
        return cfg;
    }
    catch(err){
        console.log("Syntax error on parsing config file: " + configFile + " |: " + err.toString());
        //process.exit(-2);
    }
}


exports.addGlobalErrorHandler = function() {
    process.on('uncaughtException', function (err) {
        logErr("uncaughtException",err);
        //logErr('uncaughtException',err);
    });
}


swarmSettings = {authentificationMethod:"default"};

getMyConfig = function(){
    return thisAdapter.config[thisAdapter.nodeName];
}

getSwarmFilePath = function(possibleRelativePath){
    var basePath = process.env.SWARM_PATH;
    if(possibleRelativePath[0] == "/" || possibleRelativePath[0] == ":"){
        return possibleRelativePath;
    }
    return basePath + "/" + possibleRelativePath;
}


