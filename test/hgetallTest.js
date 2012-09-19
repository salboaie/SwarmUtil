//before running this test,please run in redis command line: hset testkey a b

var util = require("swarmutil");

var redisClient = require("redis").createClient();

redisClient.on("error",onRedisError);
redisClient.on("ready",onRedisReady);

var redisClientPubSub = require("redis").createClient();

redisClientPubSub.on("error",onRedisError);

redisClientPubSub.subscribe("channel");
redisClientPubSub.on("subscribe",onRedisReady);

redisClientPubSub.on("message", function (channel, message) {

});

swarmingCodeLoaded = false;
function loadSwarms(){
    if(swarmingCodeLoaded == false){
        cprint("Loading swarms descriptions....");
        loadSwarmingCode( function() {
            swarmingCodeLoaded = true;
        });
    }

    setTimeout(function (){
        if(swarmingCodeLoaded == false){
            cprint("Trying to load swarms descriptions....");
            loadSwarms();
        }
    },1000);
}

function loadSwarmingCode(myfunct){
    if(gotAReturn == false){
        redisClient.set("a","b");
        redisClient.set("a","b");
        redisClient.hgetall("default_partition:Core",
            function (err,hash) {
                myfunct();
                gotAReturn = true;
                //util.delayExit("No bug this time",1000);
            });
    }
}

function onRedisReady(event){
    loadSwarms();
}

function onPSRedisReady(event){

}

function onRedisError(event){
    console.log("Redis connection error! Please start redis server or check your configurations!" + event.stack);
}
gotAReturn = false;

setTimeout(function (){
    if(gotAReturn == false){
        cprint("Bug reproduced....");
    }
    else{
        process.exit();
    }
},1000);
