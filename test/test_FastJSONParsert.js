
var parser = require("../lib/SwarmUtils").createFastParser(callBack);
var util = require("../lib/SwarmUtils");
var assert = require('assert');

var sum=0;
function callBack(object){
    if(object.id != null){
        sum+=parseInt(object.id);
    }
}

function createSockMock()
{
    return {
       buffer:"",
       write:function(str){
           this.buffer+=str;
       }
    };
}


parser.parseNewData("0x00000002\n");
parser.parseNewData("{}\n");

parser.parseNewData("0x0000000B\n");
parser.parseNewData("{\n\"id\":\"1\"}\n");

parser.parseNewData("0x0000000B\n");
parser.parseNewData("{\n\"id\":\"2\"}\n");

parser.parseNewData("0x0000000D\n");
parser.parseNewData("{\n\"id\":\"97\"\n}\n");

assert.equal(sum,100);

x = util.decimalToHex(10,4);
assert.equal(x,"0x000a");

var sock = createSockMock();
obj={id:"1"};
util.writeObject(sock,obj);
assert.equal(sock.buffer,"0x0000000a\n{\"id\":\"1\"}\n");

sock = createSockMock();
str="abc";
util.writeSizedString(sock,str);
assert.equal(sock.buffer,"0x00000003\nabc\n");


