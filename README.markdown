SwarmUtils include various utility functions for working with sockets,etc

## Install

    $ npm install swarmutil

    
## Usage for FastJsonParser 

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
   obj={id:"1"};
   util.writeObject(sock,obj); ==> 0x00000008\n{id:"1"}\n
         
   str="abc";
   util.writeSizedString(str) ==> 0x00000003\nabc\n

   
   