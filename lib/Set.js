
function SwarmSet(from){
    var set = {};

    this.union = function(value){
        if (value instanceof Array){
            for(var i = 0, len = value.length; i< len; i++ ){
               set[value[i]] = value[i];
            }
        } else {
            for(var v in value){
                set[v] = value[v];
            }
        }
    }

    this.keys = function(){
        var ret = [];
        for(var n in set){
            ret.push(n);
        }
        return ret;
    }

    this.values = function(){
        var ret = [];
        for(var n in set){
            ret.push(set[n]);
        }
        return ret;
    }

    this.insert = function(stringValue){
        set[stringValue] = stringValue;
    }

    if(from){
        this.union(from);
    }
}

exports.newSet = function(value){
    return new SwarmSet(value);
}
