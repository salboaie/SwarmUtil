Also, SwarmUtils includes core functionality for working with swarms.
SwarmUtils include various utility functions for working with sockets in js,logging,etc

## Install

    $ npm install swarmutil

## Create a new Swrm node (Adapter)

    thisAdapter = require('swarmutil').createAdapter("<<adapterName>>",false,false,true);

The "true" parameter enable verbose output and is usefull for debugging.
 
## Use FastJsonParser

   var parser = require("swarmutil").createFastParser(callBack);   
   var util = require("swarmutil");
   function callBack(objectFromJson){
	...
   }
      
   parser.parseNewData(... data from a socket);
   parser.parseNewData(... data from a socket.. or file);
   
   The callBack function will get called on each JSON object received from the socket
   
## Usage decimalToHex
   x = util.decimalToHex(10,4); => x == "0x000A"
   
## Usage writeObject,writeSizedString
   Example:
   obj={"id":"1"};
   util.writeObject(sock,obj); ==> 0x0000000A\n{\"id\":"1"}\n
         
   str="abc";
   util.writeSizedString(sock,str) ==> 0x00000003\nabc\n

   
   