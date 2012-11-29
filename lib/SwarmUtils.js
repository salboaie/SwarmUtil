
exports.adapter = require("./Adapter.js");
var clone = require('clone');

exports.createAdapter = exports.adapter.init;



var fs = require("fs");
var uuid = require('node-uuid');

exports.decimalToHex = function (d, padding) {
    var hex = Number(d).toString(16);
    padding = typeof (padding) === "undefined" || padding === null ? padding = 8 : padding;

    while (hex.length < padding) {
        hex = "0" + hex;
    }
    return "0x"+hex;
}


exports.newOutlet = require ("./Outlet.js").newOutlet;
exports.newSwarmPhase = require ("./SwarmingPhase.js").newSwarmPhase;

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
    dprint("Writing to socket: " +  str);
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
globalVerbosity = false;

dprint = function(txt){
    if(globalVerbosity == true){
        if(thisAdapter != undefined){
            console.log("DEBUG: ["+ thisAdapter.nodeName + "]: "+ txt);
        }
        else{
            console.log("DEBUG: "+ txt);
        }
    }
}



aprint = function(txt){
    console.log("DEBUG: ["+ thisAdapter.nodeName + "]: "+ txt);
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

var executionContext = {};
//var executionContext.tenantId = null;

var executionStack = [];

beginExecutionContext = function (swarm){
    executionStack.push(executionContext);
    var ctxt = {};
    ctxt.currentSwarm   = swarm;
    ctxt.sessionId      = swarm.meta.sessionId;
    ctxt.swarmingName   = swarm.meta.swarmingName;
    ctxt.tenantId       = swarm.meta.tenantId;
    ctxt.responseURI    = swarm.meta.responseURI;
    executionContext = ctxt;

}

endExecutionContext = function(){
    executionContext =  executionStack.pop();
}

createSwarmCallback = function (callBack){
    var thisContext ={};
    thisContext.meta = clone(executionContext);
    return function(){
        beginExecutionContext(thisContext);
        var args = []; // empty array
        for(var i = 0; i < arguments.length; i++){
            args.push(arguments[i]);
        }
        callBack.apply(this,args);
        endExecutionContext();
    }
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

getCurrentResponseURI = function(){
    return executionContext.responseURI;
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
globalContexts  = {};


getGlobalContext = function(contextId){
    var retCtxt = globalContexts[contextId];
    if(retCtxt == undefined){
        retCtxt = globalContexts[contextId] = new VariablesContext();
    }
    return retCtxt;
}

getContext = function(contextId){
    if(contextId == undefined){
        contextId = "thisAdapter";
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
    cprint("Error: getContext called without an execution tenant active");
    return null;
}


removeContext = function(contextId){
    if(executionContext.tenantId != null){
        var tenantContext = tenantsContexts[executionContext.tenantId];
        delete tenantContext[contextId];
    }
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
    }else{
        errStr = message;
        stack="";
    }
    fs.appendFileSync(getSwarmFilePath(thisAdapter.config.logsPath + "/" + thisAdapter.nodeName+".err"),errStr+"\n"+stack+"\n");

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
        startSwarm("log.js","info",aspect, message, details,getCurrentSwarm());
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
        process.exit(-2);
    }
}


exports.addGlobalErrorHandler = function() {
    process.on('uncaughtException', function (err) {
        logErr("uncaughtException",err);
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


generateUID = function(){
    return uuid.v4()
}



exports.mkUri = function(type, value) {
    var uri = thisAdapter.coreId + ":" + type + ":" + value;
    //cprint("URI: " + uri);
    return uri;
}

exports.mkChannelUri = function(nodeName) {
    var uri = thisAdapter.coreId + ":" + nodeName;
    //cprint("URI: " + uri);
    return uri;
}

exports.delayExit = function(msg,timeout) {
    if(msg != undefined){
        cprint(msg);
    }
    setTimeout(function (){
        process.exit(-2);
    },timeout);
}

M = function(obj){
    var meta={};
    var ctrl={};
    meta.swarmingName   = obj.meta.swarmingName;
    meta.currentPhase   = obj.meta.currentPhase;
    meta.tenantId       = obj.meta.tenantId;
    meta.sessionId      = obj.meta.sessionId;
    if(obj.meta.pleaseConfirm != undefined){
        ctrl.pleaseConfirm      = obj.meta.pleaseConfirm;
    }
    if(obj.meta.phaseExecutionId != undefined){
        ctrl.phaseExecutionId      = obj.meta.phaseExecutionId;
    }
    if(obj.meta.confirmationNode != undefined){
        ctrl.confirmationNode      = obj.meta.confirmationNode;
    }

    var vars            = {};
    for (var i in obj){
        if(i != "meta"){
            vars[i] = obj[i];
        }
    }

    return "\t{\n\t\tMETA: "+J(meta) +
        "\n\t\tCTRL: " +J(ctrl) +
        "\n\t\tVARS: " + J(vars) +
        "\n\t\tDUMP: "+J(obj)+"\n\t}";
}



exports.wsStartSwarm = function(host, port, callBack,sessionId, swarmName, ctor) {
    var args= [];
    // copy all other arguments we want to "pass through"
    for(var i = 6; i < arguments.length; i++){
        args.push(arguments[i]);
    }

    var dataObj = { "targetAdapter":undefined,
                    "session":sessionId ,
                    "swarm" : swarmName,
                    "ctor": ctor,
                    "args":args
                };

    var options = {
        host: host,
        port: port,
        path: "/startSwarm/",
        method: 'POST'
    };

    var result = "";
    var req = require("http").request(options, function(res) {
        //console.log('STATUS: ' + res.statusCode);
        //console.log('HEADERS: ' + JSON.stringify(res.headers));
        res.setEncoding('utf8');
        res.on('data', function (chunk) {
            result += chunk;
        });

        res.on('end', function () {
            callBack(JSON.parse(result));
        });
    });

    req.on('error', function(e) {
        console.log('Problem with request: ' + e.message);
    });

    // write data to request body
    req.write(J(dataObj));
    req.end();
}
