/**
 * @namespace Process
 * do the work of push notifications
 */
module.exports = (function(ns) {

  ns.settings = require('./private/secrets');
  var GetEnvs = require('./getenvs');
  var Useful = require('./useful');
  var axios = require('axios');
  var fb =require ('./firebase');
  
  var env_;
  var redisWatchable_, redisSubscribe_, redisLog_, redisWatchLog_, redisSyncPub_, redisSync_;

  // this is used to correct the push server/ server view of the time
  // on the cloud 9 server, the creation time of a watch event can seem to have happened
  // after it logs on the push server which cant happen
  var timeOffset = 0,
    timeLatency = 0,
    timeNobs = 0,
    smooth = .1,
    syncId = new Date().getTime().toString(32);


  /** this keeps asking the api server to tell me what time it thinks it is
   *  by creating a sync record
   * which i then look for in key change events
   * from that i can calculate the differnence in time between what the API server thinks, and what I think
   */
  ns.synchTime = function() {

    // I'm using recursive setTimout rather than setInterval
    function sched () {
      pub()
        .then(function() {
          return Useful.handyTimer(ns.settings.redisSyncFrequency);
        })        
        .then(function() {
          sched();
        });
    }
    
    // publish a timesync request    
    function pub() {
      return redisSyncPub_.publish(ns.settings.redisSyncChannel, JSON.stringify({
        requestAt: new Date().getTime(),
        syncId: syncId,
        expire:ns.settings.redisSyncFrequency * 2
      }));
    }
    
    // the first one will generate misleading initialization times, so it'll be ignored
    pub().then(function () { sched() });

  };
  
  /**
   * get called when a keyevent is raised to sync the time
   * @param {string} message
   */
  ns.synchTimeUpdate = function (message) {
    
    var now = new Date().getTime();
    
    // will look something like this '__keyevent@6__:set sy-.1bfgvorjj.1494145143663'
    var s = message.split (".");
    var sId = s[1];
    
    // check this is for me - multiple instances will cause this
    if (sId !== syncId) {
      return Promise.resolve(null);
    }
    
    // now get the item so the times can be processed
    return redisSync_.get (message)
      .then (function (res) { 
        // convert to an object
        var ob = Useful.obify (res).ob;

        // end to end time
        var latency = (now - ob.requestAt);
        
        // smooth the observations
        
        // ignore completely outlying latency as it could be a network blip
        if (latency < ns.settings.redisSyncNetWorkOutlier && (!timeLatency || latency < timeLatency * ns.settings.networkBlipFactor) ) {
        
          // api server is golden for time,

          // ignore the first one because it'll be misleading since it probably did some initialization
          if (timeNobs) {
            timeLatency = timeNobs > 1 ? latency * smooth + timeLatency * (1 - smooth) : latency;
            var offBy = ob.writtenAt - now - timeLatency/2; 
            var toof = timeOffset;
            timeOffset = timeNobs > 1 ? offBy * smooth + timeOffset * (1 - smooth) : offBy;
            if (Math.round(toof) !== Math.round(timeOffset)) {
              console.log ("settling timeoffset on ", timeOffset);
            }
          }

          timeNobs++;
        }

      });
    
    
  };
  /**
   * initially call to set everything up
   */
  ns.init = function() {

    // set up operational settings
    env_ = require("./getenvs").init();
    redisWatchable_ = GetEnvs.redisConf('watchable', env_);
    redisSubscribe_ = GetEnvs.redisConf('subscribe', env_);
    redisLog_ = GetEnvs.redisConf('log', env_);
    redisWatchLog_ = GetEnvs.redisConf('rate', env_);
    redisSyncPub_ = GetEnvs.redisConf('ts', env_);
    redisSync_ = GetEnvs.redisConf('ts', env_);

    var st = ns.settings;
    ns.synchTime();
    
    // get firebase going
    fb.init();
    
    // set up keyspace event monitoring - watching out for del, expired and set on data items and time syncing
    redisSubscribe_.config('set', 'notify-keyspace-events', 'Exg$z')
      .then(function() {
        // subscribe to  datbase changes
        return redisSubscribe_.psubscribe('__keyevent@[' + st.db.client + st.db.log + st.db.ts + ']__:*');
      })
      .then(function() {

        redisSubscribe_.on('pmessage', function(pattern, channel, message) {

          var method = channel.replace(/.*__:/, '');
          var db = parseInt(channel.replace(/.*@/, "").replace(/__.*/, ""), 10);
          var now = new Date().getTime();

          // I'm logging all interesting events -- written data
          // but relying on watching for the logevent to be done before doing a push

          // an item changes
          if (db === st.db.client && st.watchable.events[method] && message.slice(0, st.itemPrefix.length) === st.itemPrefix) {
            ns.logEvent(method, message, "redis", now + timeOffset );
          }
          
          // a log file records an event
          else if (db === st.db.log && method === "zadd" && message.slice(0, st.logPrefix.length) === st.logPrefix) {
            ns.pushEvent(message);
          }
          
          // used for synching time between instances
          else if (db === st.db.ts && method === "set" && message.slice(0, st.syncPrefix.length) === st.syncPrefix) {
            ns.synchTimeUpdate (message);
          }
          
          // a watchable expires - i use this to keep firebase clean
          else if (db === st.db.watchable && method === "expired" && message.slice(0, st.watchablePrefix.length) === st.watchablePrefix) {
            ns.cleanFirebase (message);
          }

        });
      });

  };


  /**
   * get the event list for this log key
   * @param {string} lkey the log key
   * @return {Promise}
   */
  ns.getLoggedEvents = function(lkey) {
    return redisLog_.zrangebyscore(lkey, 0, Infinity)
      .then(function(r) {
        return (r || []).map(function(d) {
          return parseInt(d, 10);
        });
      });
  };

  /**
   * get the qualifying keys
   * @param {[number]} values the list of event timestamps
   * @param {object} sx the watch items 
   * @return {[number]} the filtered list of values
   */
  ns.getQualifyingKeys = function(values, sx) {

    var v = values.filter(function(d, i) {
      return d  >= sx.nextevent;
    });

    return v;
  };

  /**
   * make the packet
   * @param {[number]} values the list of event timestamps that qualify
   * @param {object} sx the watch items 
   * @param {string} sxKey the eatch item key
   * @return {[number]} the filtered list of values
   */
  ns.makeSxPacket = function(values, sx, sxKey) {

    // this is the packet we'll send
    return {
      id: sx.id,
      alias: sx.alias || "",
      value: values,
      watchable: sxKey,
      event: sx.event,
      message: sx.options.message || "",
      pushId: sx.options.pushid,
      nextevent: sx.nextevent,
      type: sx.options.type,
      uq: sx.options.uq
    };
  };


  /**
   * a log event has happened, now push it to whoever cares
   * @param {string} lkey the log  key
   */
  ns.pushEvent = function(lkey) {

    var logTime = new Date().getTime();
    var st = ns.settings;

    // get the other interesting keys from that
    var sp = lkey.split(".");

    var itemKey = sp[0].replace(st.logPrefix, "");
    var method = sp[1];
    
    // kick off the search for anybody watching this - its key will contain the item's private key
    var wkey = st.watchablePrefix + "*" + itemKey + "." + method + "*";

    // service any watchers
    // all times stored in values have already been tweaked to match server time when event was logged
    return Promise.all([ns.getLoggedEvents(lkey), Useful.getMatchingObs(redisWatchable_, wkey)])
      .then(function(r) {

        var values = r[0];
        var sxs = r[1];

        // look to see who is on line
        return Promise.all(sxs.map(function(sxo) {

          var sxKey = sxo.key.split(".")[0].replace(ns.settings.watchablePrefix, "");
          var sx = sxo.data;


          // reduce to keys that havent been reported yet
          var nv = ns.getQualifyingKeys(values, sx);


          // if there's anything to report
          if (nv.length) {

            // the next event will be one ms along from where are
            sx.nextevent = nv[nv.length - 1] + 1;
            
            // this shouldn't be relevant as it self adjusts and is already applied, but may be useful debugging
            sx.timeoffset = timeOffset;
            
            var packet = ns.makeSxPacket(nv, sx, sxKey);

            // and this is what we'll log
            var watchLog = {
              id: packet.id,
              alias: packet.alias,
              watchable: packet.watchable,
              event: packet.event,
              pushId: sx.options.pushid,
              error: "pending",
              type: sx.options.type,
              ok: false,
              code: ns.settings.errors.CREATED,
              value:packet.value,
              uq:sx.options.uq
            };

            // if its a push type and the recipient is currently connected    
            if (sx.options.type === "push") {
              
              // fb doesnt allow $
              var fbKey = sxKey.replace ("$","___");
              // push to firebase and let him worry about it
              fb.set (fbKey + "/" + sx.options.uq, JSON.stringify(packet.value))
              .then (function () {
                watchLog.error = "emitted";
                watchLog.ok = true;
                return updateSx (sxo.key, sx, watchLog);
              })
              .then (function () {
                return ns.logWatchLog(sxKey, logTime, watchLog);
              })
              .catch(function (err) {
                watchLog.error = err;
                console.log ('failed fb', err);
                return ns.logWatchLog(sxKey, logTime, watchLog);
              });
            }
            else if (sx.options.type === "url") {
              //prepare
              
              var url = sx.options.url + (sx.options.url.indexOf("?") !== -1 ? "&" : "?") + "watchable=" + sxKey;
              var method = sx.options.method.toLowerCase();
              var data = method === "post" || method === "put" || method === "patch" ? packet : null;
              if (!data) url += ("&data=" + encodeURIComponent(JSON.stringify(packet)));
              
              watchLog.url = url;
              watchLog.method = method;
              
              var ac = {
                method: method,
                url: url
              };
              if (data) ac.data = data;

              //post
              axios(ac)
                .then(function(result) {
                  watchLog.error = result.data.error || "emitted";
                  watchLog.ok = result.data.ok;
   
                  // the difference between sxo.key(the entire key) and sxKey (the coupon code)
                  if (result.data.ok) {
                    updateSx (sxo.key, sx, watchLog)
                    .then (function () {
                      ns.logWatchLog(sxKey, logTime, watchLog);
                    });
                  }
                  else {
                    // and expire this push notification shortly so it doesn't bother us again
                    // no need to wait
                    redisWatchable_.expire(sxo.key, ns.settings.expireOnDroppedConnection)
                      .then(function(t) {
                        if (!t) {
                          console.log("failed to expire after disconnection", sxo.key);
                        }
                      });
                  }
                      
                })
                .catch(function(err) {
                  watchLog.error = err.response.statusText;
                  watchLog.code = err.response.status;
                  ns.logWatchLog(sxKey, logTime, watchLog);
                  console.log('error url push',url,err.response.statusText);
                });

            }
            else {
              watchLog.error = "unknown watch type " + sx.options.type;
              watchLog.code = ns.settings.errors.INTERNAL;
              ns.logWatchLog(sxKey, logTime, watchLog);
            }
          }

        }));

      });
      

      function updateSx (key, sx, watchLog ) {
        return Useful.updateItem(redisWatchable_, key , sx, 0)
          .then(function(r) {
            if (r !== "OK") {
              watchLog.error = "failed to update next event";
              watchLog.code = ns.settings.errors.INTERNAL;
            }
          });
      }

  };

  /**
   * watchlog .. useful for finding errors and tracking
   * @param {string} sxKey the watch key
   * @param {number} logTime when it happemed
   * @param {object} packet the data to log
   * @return {Promise} the result
   */
  ns.logWatchLog = function(sxKey, logTime, packet) {
    // expiry times for watchlog are fixed
    // a watch packet
    // key - the sxkey + the key that created it (for security) + the of the event + some random to avoid clashes at same ms.
    var key = ns.settings.watchLogPrefix + [sxKey, logTime, Math.round(Math.random() * 1000)].join(".");
    return redisWatchLog_.set(key, JSON.stringify({
      packet: packet,
      logTime: logTime
    }), "EX", ns.settings.watchLogLifetime);
  };

  /**
   * when a watchable item expires
   * use the opportunity to remove it from firebase
   * @param {string} the subscription message
   * @return {Promise}
   */
  ns.cleanFirebase = function(message) {
    
    // get the actual key & remove it
    var key = message.split(".")[0].slice (ns.settings.watchablePrefix.length);
    return fb.remove (key);
    
  };

  /**
   * logs an event against a data item
   * @param {string} method the method reported by redis (set/del/expired)
   * @param {string} key the provate encoded data item key
   * @param {string} who which session is provoking the event.. not implemented for now - maybe later
   * @param {string} [logTime=now] the logtime if required
   */
  ns.logEvent = function(method, key, who, logTime) {
    // expiry times for log event are not related to data lifetime
    // they are the maximum subscription history time

    logTime = logTime || new Date().getTime();

    // log events are keyed  by their private item key
    var lkey = ns.settings.logPrefix + key + "." + method;

    // add an observation for this log entry
    return redisLog_.zadd(lkey, logTime, logTime)
      .then(function(result) {
        // set an expire time some standard amount from now
        return redisLog_.expire(lkey, ns.settings.logLifetime);
      });

  };

  return ns;
})({});
