
/**
 * @namespace App
 * handles all communication with outside world
 */
module.exports = (function(nsa) {

  var appConfigs = require('./appConfigs.js');
  var express = require('express');
  var app = express();

  var GetEnvs = require ('./getenvs');
  var Pusher = require ('./pusher');

  
  // these will be the settings for the app
  nsa.settings = {};

  // the settings
  var secrets = require('./private/secrets'); 

  // move them into the settings
  Object.keys(secrets).forEach (function (d) {
    nsa.settings[d] = secrets[d];
  });
  
  // get operational settings
  nsa.settings.env =  GetEnvs.init();


  /**
   * app start up
   */
  nsa.init = function () {

  };

  return nsa;
})({});




