/*
*  topology class keeps information about groups, addresses.
* */
function SwarmNodesTopology(){
    var nodes = {};
    var channels = {};

    function NodeInfo(uuid){
        this.uuid = uuid;
    }

    function lookupNode(uuid){
        var node = nodes[uuid];
        if( node == undefined){
            node = new NodeInfo(uuid);
            nodes[uuid] =  node;
        }
        return node;
    }

    function GroupInfo(name){
        channels[name] = this;
        var roundRobinIndex = 0;
        var members = {};
        var names = [];
        var leader;

        this.addMember = function(uuid,node){
            index.push(node);
            members[uuid] = node;
            if(names.indexOf(uuid) == -1){
                names.push(uuid);
            }
        }

        this.deleteMember = function(uuid){
            var position = names.indexOf(uuid);
            if(position != -1){
                names.splice(position, 1);
            }
            delete members[uuid];
        }

        this.getNextMember = function(uidNumber){
            if(roundRobinIndex >= names.length){
                roundRobinIndex = 0;
            }
            return members[names[roundRobinIndex]];
        }

        this.mapInfo = function(info, callBack){
            for(var v in members){
                callBack(members[v][info]);
            }
        }

        this.mapNodes = function(callBack){
            for(var v in members){
                callBack(members[v]);
            }
        }
    }

    function lookupGroup(groupName){
        var g = channels[groupName];
        if(g == undefined){
            new GroupInfo(groupName);
        }
        return g;
    }

    this.addNodeInfo = function(uuid, info, value){
        var node = lookupNode(uuid);
        node[info] =  value;
    }

    this.getNodeInfo = function(uuid, info){
        var node = lookupNode(uuid);
        return node[info];
    }

    this.addNodeInGroup = function (groupName,uuid){
        var group = lookupGroup(groupName);
        group.addMember(uuid,lookupNode(uuid));
    }

    this.deleteNodeInGroup = function (groupName,uuid){
        var group = lookupGroup(groupName);
        group.deleteMember(uuid);
    }

    this.chooseGroupMember = function(groupName){
         var group = lookupGroup(groupName);
         return group.getNextMember();
     }

    this.mapInfo = function(groupName, info , callBack){
        var group = lookupGroup(groupName);
        group.map(info,callBack);
    }

    this.mapNodes = function(groupName, callBack){
        var group = lookupGroup(groupName);
        group.mapNodes(callBack);
    }
}

var topology = new SwarmNodesTopology();

exports.getTopology = function(){
    return topology;
}

function NodeAddresses(str){
    var addresses = {};
    this.asString = function(){
        var ret = "";
        for(var pn in addresses){
            if(ret != ""){
                ret += ",";
            }
            ret += pn;
        }
        return ret;
    }

    function createAddr(str){
        var str = cores.split(":");
        var host = addr[0];
        var post = parseInt(addr[1]);
        return {"host":host, "port":port};
    }

    this.merge = function(str){
        var addrs = str.split(",");
        for(var i=0; i < addrs.length; i++){
            addresses[addrs[i]] = createAddr(addrs[i]);
        }
    }

    this.remove = function(str){
        var addrs = str.split(",");
        for(var i=0; i < addrs.length; i++){
            delete addresses[addrs[i]];
        }
    }
    this.merge(str);
}
