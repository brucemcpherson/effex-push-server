
/**
 * @namespace App
 * handles all communication with outside world
 */
module.exports = (function(nsa) {

  var appConfigs = require('./appConfigs.js');
  var express = require('express');
  var app = express();
  var server = require('http').createServer(app);
  var io = require('socket.io')(server);
  var GetEnvs = require ('./getenvs');
  var Pusher = require ('./pusher');

  
  // these will be the settings for the app
  nsa.settings = {};
  Pusher.connections = {};
  
  // the settings
  var secrets = require('./secrets'); 

  // move them into the settings
  Object.keys(secrets).forEach (function (d) {
    nsa.settings[d] = secrets[d];
  });
  
  // get operational settings
  nsa.settings.env =  GetEnvs.init();


  /**
   * just a promise version of a time out
   * @param {number} ms o many ms to wait
   * @param {*} id any kind of thing to pass when resolved
   */
  function pTimeout (ms,id) {
    return new Promise (function (resolve, reject) {
      setTimeout (function () {
        resolve (id);
      }, ms);
    });
  }
  /**
   * app start up
   */
  nsa.init = function () {

console.log('starting on port '+nsa.settings.env.socketPort);
    server.listen(nsa.settings.env.socketPort);
    
    // start watching for connections
    io.on ("connection", function (client) {

      console.log ("a client was seen"+ client.id);
      
      // keep track of this connection
      // there can be multiple watches on a single connection
      var connection = {
        passed:false,
        client:client,
        handled:false,
        pushId:""
      };
      
      // I'm keying the active connections on the pushId, 
      // since the socket id might change if it gets dropped and reconnects
      
      // remove any disconnecting sessions
      connection.client.on("disconnect", function (client) {
        var pc = Pusher.connections;
        Object.keys(pc).forEach (function (k) {
          if (connection.client && pc[k] && pc[k].connection && pc[k].connection.client.id === connection.client.id) {
            console.log('deleting connection',connection.client && connection.client.id ,k);
            delete pc[k];
          }
        });
      });
      
      // The connection process includes a password conversation
      connection.client.on ("pass", function (pack,fn) {

        // im doing it, not the timeout
        connection.handled = true;
        
        // received a pass attempt
        connection.pushId = pack.pushId;
        connection.passed = pack.pass === nsa.settings.env.socketPass && 
          pack.id === connection.client.id && 
          (pack.pushId ? true : false);
        
        // send an ack if we can
        if (fn && connection.passed) {
          Pusher.connections[connection.pushId] = {
            connection:connection
          };
          console.log ('added',connection.pushId,Pusher.connections);
          console.log ("client connected",connection.client.id);
          fn ({ok:connection.passed , connection:connection.client.id});
        }
        
        // disconnect if no good 
        if (!connection.passed) {
          console.log ("client unauth attempt",pack);
          connection.client.disconnect ("pass/socket id mismatch or missing pushId detected");
        }

      });


      // kick out if no response
      pTimeout(nsa.settings.env.socketPassTimeout,connection)
      .then (function (connection) {
        if (!connection.handled) {
          console.log('kicking out');
          connection.client.disconnect ("timeout getting passphrase");
        }
      });
    });    

  };

  return nsa;
})({});




