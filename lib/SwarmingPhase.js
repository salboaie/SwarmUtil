var util = require("swarmutil");

function SwarmingPhase(swarmingName, phase, model) {
    var meta        = thisAdapter.compiledSwarmingDescriptions[swarmingName].meta;
    var initVars    = thisAdapter.compiledSwarmingDescriptions[swarmingName].vars;

    this.meta = new Object();
    if(meta != undefined){
        for (var i in meta) {
            this.meta[i] = meta[i];
        }
    }

    if(initVars != undefined){
        for (var i in initVars) {
            this[i] = initVars[i];
        }
    }

    if(model != undefined && model != null ){
        for (var i in model){
            if(i != "meta"){
                this[i] = model[i];
            }else{
                if(model.meta!= undefined){
                    for (var j in model.meta){
                        this.meta[j] = model.meta[j];
                    }
                }
            }
        }
    }

    this.meta.swarmingName = swarmingName;
    this.meta.currentPhase = phase;
}

SwarmingPhase.prototype.getSwarmName = function(){
    return this.meta.swarmingName;
}

SwarmingPhase.prototype.swarm = function (phaseName, nodeHint ) {
    if(thisAdapter.readyForSwarm != true){
        cprint("Asynchronicity issue: Redis connection is not ready for swarming " + phaseName);
        return;
    }
    try{
        if(thisAdapter.compiledSwarmingDescriptions[this.meta.swarmingName] == undefined){
            logErr("Undefined swarm " + this.meta.swarmingName);
            return;
        }

        this.meta.currentPhase = phaseName;
        var targetNodeName = nodeHint;
        if (nodeHint == undefined) {
            var phase = thisAdapter.compiledSwarmingDescriptions[this.meta.swarmingName][phaseName];
            if(phase == undefined){
                logErr("Undefined phase " + phaseName + " in swarm " + this.meta.swarmingName);
                return;
            }
            targetNodeName = phase.node;
        }

        if(this.meta.debug == true){
            dprint("Starting swarm "+this.meta.swarmingName +  " towards " + targetNodeName + ", Phase: "+ phaseName);
        }

        if(targetNodeName != undefined){
            publishSwarm(targetNodeName,this);
        }
        else{
            logInfo("Unknown phase " + phaseName);
        }
    }
    catch(err) {
        logErr("Unknown error in phase {" + phaseName + "} nodeHint is {" + targetNodeName +"} Dump: " + J(thisAdapter.compiledSwarmingDescriptions[this.swarmingName]),err);
    }
};


SwarmingPhase.prototype.sendFail = function() {
    var phase = this.meta.onError;
    if( phase != undefined){
        this.swarm(phase,this.meta.confirmationNode);
    }
}

function getWaitingContext(swarm,desiredPhaseName, nodeHint, timeOut, retryTimes, phaseExecutionId){
    return function(){
        beginExecutionContext(swarm);
        this.meta.phaseExecutionId = phaseExecutionId;
        var ctxt = getContext(phaseExecutionId);
        if(ctxt.confirmedExecution == true){
            dprint("Confirmed " + phaseExecutionId);
            var phase = this.meta.onSucces;
            if( phase != undefined){
                this.swarm(phase,this.meta.confirmationNode);
            }
            removeContext(phaseExecutionId);
        }
        else{
            if(retryTimes == 0){
                dprint("Sending fail notification "+ phaseExecutionId );
                this.sendFail();
            }else{
                cprint("Retrying safe swarm  " + phaseExecutionId);
                this.safeSwarm(desiredPhaseName,nodeHint,timeOut,retryTimes-1,false);
            }
        }
        endExecutionContext();
    }.bind(swarm);
}

SwarmingPhase.prototype.safeSwarm = function (phaseName, nodeHint,timeOut,retryTimes,persistent) {
    var cloneSwarm = util.newSwarmPhase(this.getSwarmName(),phaseName,this);
    if(timeOut == undefined ){
        timeOut = 300;
    }
    if(retryTimes == undefined){
        retryTimes = 0;
    }
    cloneSwarm.meta.phaseExecutionId = generateUID();
    cloneSwarm.meta.confirmationNode = thisAdapter.nodeName;
    cloneSwarm.meta.pleaseConfirm = true;

    dprint("New phaseExecutionId " + cloneSwarm.meta.phaseExecutionId + M(cloneSwarm));

    setTimeout(getWaitingContext(cloneSwarm, phaseName, nodeHint, timeOut, retryTimes, cloneSwarm.meta.phaseExecutionId), timeOut);
    cloneSwarm.swarm(phaseName,nodeHint);
}

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
                logErr("Failed in setting timeout in swarm " + this.meta.swarmingName + " because " +phaseName + " is not a phase", err);
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


exports.newSwarmPhase = function(swarmingName, phase, model){
    return new SwarmingPhase(swarmingName, phase, model);
}

SwarmingPhase.prototype.currentSession = function(){
    return this.meta.sessionId;
}

SwarmingPhase.prototype.getSessionId = SwarmingPhase.prototype.currentSession;

SwarmingPhase.prototype.setSessionId = function(session){
    this.meta.sessionId = session;
}


SwarmingPhase.prototype.getTenantId = function(){
    return this.meta.tenantId;
}

SwarmingPhase.prototype.setTenantId = function(tenant){
    this.meta.tenantId = tenant;
    beginExecutionContext(this);
}

function consumeSwarm(channel,swarm,funct){
    return function(){
        try{
            util.adapter.onMessageFromQueueCallBack(swarm);
            funct(null,null);
        }
        catch(err){
            funct(err,null);
        }
    }
}

function safeSwarmPublish(redisClient,channel, swarm){
    redisClient.publish(channel,J(swarm),function(err,ret){
        if(err != null){
            logErr(err.message,err);
        }

        if(ret == 0){ //no one is listening...
            cprint("Retrying swarm propagation towards "  +channel + ": "+J(swarm));
            if(swarm.meta.timout == undefined){
                swarm.meta.timout = 100;
            }
            else{
                swarm.meta.timout = 1.5* swarm.meta.timout;
            }
            var maxTmout = swarm.meta.maxtimout;
            if(maxTmout == undefined){
                maxTmout = 1000*60*60; //one hour
            }
            if(swarm.meta.timout < maxTmout){
                setTimeout(function(){
                    safeSwarmPublish(redisClient,channel, swarm);
                }, swarm.meta.timout);
            } else {
             cprint("Dropping swarm " + J(swarm));
             startSwarm("saveSwarm.js","drop",swarm);
            }
        }
    });
}


function publishSwarm(channel,swarm){
    if(channel[0] == "#"){
        //local channel, just execute
        process.nextTick(consumeSwarm(channel,swarm,funct))
    }
    else{
        safeSwarmPublish(redisClient,util.mkChannelUri(channel), swarm);
    }
}


/* alternative implementation for local nodes
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

