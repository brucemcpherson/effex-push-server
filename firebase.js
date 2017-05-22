//Loading Firebase Package
var FB = (function(ns) {

  var fb = require("firebase-admin");
  var app;
  ns.base = "/push";
  
  /**
   * initialize firebase with SA
   * @return {Promise}
   */
  ns.init = function() {

    // the file should be the service account
    // created in the firebase console with the role of "editor"

    /*
    * access to database is being limited by this rule
    {
      "rules": {
          ".read": "auth != null",
          ".write": "auth.uid === 'effex-push-server'"
      }
    }
    */

    app = fb.initializeApp({
      credential: fb.credential.cert(require("./private/fbserviceaccount.json")),
      databaseURL: "https://effex-push.firebaseio.com",
      databaseAuthVariableOverride: {
        uid: "effex-push-server"
      }
    });
    
    if (!app) {
      console.log ('failed to init fb');
    }
    // set up things we'll need
    ns.db = app.database();
    ns.baseRef = ns.db.ref(ns.base);
    return ns;
  };
    
  /**
   * write something
   * @param {string} key the key to write
   * @param {object} value the value to write
   * @return {Promise}
   */
  ns.set = function(key, value) {
    return ns.baseRef.child(key)
    .set(value)
      .then(function() { 
        return key;
      })
      .catch (function (err) {
        console.log ('failed to set fb', key, value,err);
      });
  };

 /**
   * remove something
   * @param {string} key the key to remove
   * @return {Promise}
   */
  ns.remove = function(key) {
    return ns.baseRef.child(key)
    .remove ()
      .then(function(x) { 
        return key;
      })
      .catch (function (err) {
        console.log ('failed to remove fb', key,err);
      });
  };
  
  return ns;
})({});
module.exports = FB;
