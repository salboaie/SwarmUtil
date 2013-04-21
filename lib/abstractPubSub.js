
var topology = require('./topology.js').getTopology();
exports.getPubSub = function (){
    return new AbstractPubSub();
}

function AbstractPubSub(realPublish, realSubscribe){
    var uid = 0;
    function getUID(){
        uid++;
        return uid;
    }

    AbstractPubSub.prototype.broadcast = function(groupName){
        topology.mapNodes(realPublish);
    }

    AbstractPubSub.prototype.safePublish = function(channel){
        var publishId = topology.getNode ;

        while findNode(target,publishId)  do
            if(nativePub(this)) begin
        return true;
        end
        endwhile
        return false;
        end
    }
}


