/**
 * push server for  effex
 * separated from the API to keep that stateless
 */
var App = require ('./pusherroutes');
var Pusher = require ('./pusher');

// start watching 
App.init();
Pusher.init();

