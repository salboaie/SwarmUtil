

function loadSwarmingCode(funct){
    setTimeout(function (){
        funct();
        console.log("Processing swarms descriptions....");
        swarmingCodeLoaded = true;
    },1000);
}

swarmingCodeLoaded = false;

function loadSwarms(){
    loadSwarmingCode(function(){
        setTimeout(function (){
            if(swarmingCodeLoaded == false){
                console.log("Loading swarms descriptions....");
            }
        },10);

        setTimeout(function (){
            if(swarmingCodeLoaded == false){
                console.log("Trying to load swarms descriptions....");
                loadSwarms();
            }
        },2000);
    } );
}

loadSwarms();