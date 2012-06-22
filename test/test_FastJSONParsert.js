
var parser = require("../../FastJSONParser").createFastParser(callBack);

var sum=0;
function callBack(object){
    if(object.id != null){
        sum+=parseInt(object.id);
    }
}

parser.parseNewData("0x00000002\n");
parser.parseNewData("{}\n");

parser.parseNewData("0x0000000B\n");
parser.parseNewData("{\n\"id\":\"1\"}\n");

parser.parseNewData("0x0000000B\n");
parser.parseNewData("{\n\"id\":\"2\"}\n");

parser.parseNewData("0x0000000D\n");
parser.parseNewData("{\n\"id\":\"97\"\n}\n");

if(sum != 100){
    console.log("Test failed");
}
else{
    console.log("Test passed");
}