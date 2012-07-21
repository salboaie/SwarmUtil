
exports.createAdaptor = require("./Adaptor.js").init;
exports.adaptor = require("./Adaptor.js");

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
printf = function(format){
    var out = util.format(format,arguments);
    util.puts(out);
}


cprint = console.log;

dprint = function(txt){
    if(thisAdaptor != undefined){
        var verbose = thisAdaptor.config.verbose;
    }
    if(verbose == true){
        console.log(txt);
    }
}


perror = function(err, textOnly,printStack){
    if(textOnly != undefined){
        console.log("(*) Error in Adaptor : "+ thisAdaptor.nodeName + "\n (**)Environment info [ " + textOnly + "]");
        console.log(" (Err)"+err);
        if(printStack != false){
            console.log(" (Stack)"+err.stack);
        }
        return;
    }
    console.log("Adaptor:" + thisAdaptor.nodeName +"\n");
    console.log("\n---------------\nError: " + err);
    console.log(err);
    if(err && printStack == undefined){
        console.log(err.stack);
    }
    console.log("\n---------------\n");
}

var executionContext={};

beginContext = function (swarm){
    executionContext.sessionId = swarm.sessionId;
    executionContext.tenantId  = swarm.tenantId;
}

endContext = function(){
    executionContext.sessionId  = null;
    executionContext.tenantId   = null;
}

getCurrentSession = function(){
    return executionContext.sessionId;
}

getCurrentTenant = function(){
    return executionContext.tenantId;
}

logErr = function(message,err){
    var errStr;
    var stack;
    dprint("Logging error: " + message);
    if(err != null && err != undefined){
        errStr = err.toString();
        stack = err.stack;
    }
    startSwarm("log.js","err",["ERROR",message,errStr,stack]);
}

logDebug = function(message,details,aspect){
    if(aspect == undefined){
        aspect = "DEBUG";
    }
    dprint("(**) Logging debug info: " + message);
    startSwarm("log.js","info",aspect, message, details);
}


logInfo = function(message,details,aspect){
    if(aspect == undefined){
        aspect = "INFO";
    }
    dprint("(*) Logging info: " + message);
    startSwarm("log.js","info",aspect, message, details);
}

inspect = function (object){
    var out =   "----------------------------------------------------------------------\n"+
                util.inspect(object) +
                "----------------------------------------------------------------------\n";
    util.puts(out);
}

exports.perror = perror;
exports.inspect = inspect;

J = function(obj){
    return JSON.stringify(obj);
}

exports.readConfig = function(configFile) {
    var configContent = require('fs').readFileSync(configFile);
    cfg =  JSON.parse(configContent);
    return cfg;
}

exports.addGlobalErrorHandler = function() {
    process.on('uncaughtException', function (err) {
        perror(err,"uncaughtException");
        logErr('uncaughtException',err);
    });
}



