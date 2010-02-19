// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

var resolveModule = function (name, parent, ddoc) {
  if (parent == undefined && name[0] == '.') {
    throw "Invalid name. View functions cannot perform relative require() imports."
  } 
  var split = name.split('/');
  var current = ddoc;
  if (name[0] != '.') {
    parent = ddoc;
  }
  for (i=0;i<split.length;i+=1) {
    var s = split[i];
    if (s == '.') {
      // Do nothing, at this point we should already be relative to the proper parent.
    } else if ( s == '..') {
      parent = parent.parent;
    } else {
      if (!parent[s]) {
        current = current[s];
        current.parent = parent;
        parent = current;
      } else {
        throw 'Object has no property "'+s+'". '+JSON.stringify(current);
      }
    }
  }
  if (typeof current != "string") {
    throw 'Require name did not resolve to a string value "'+name+'". '+JSON.stringify(current);
  }
  return [current, current.parent];
}

var Couch = {
  // moving this away from global so we can move to json2.js later
  toJSON : function (val) {
    return JSON.stringify(val);
  },
  compileFunction : function(source, ddoc) {    
    if (!source) throw(["error","not_found","missing function"]);
    try {
      if (sandbox) {
        if (ddoc) {
          var require = function (name, parent) {
            var exports = {};
            var resolved = resolveModule(name, parent, ddoc);
            var source = resolved[0]; parent = resolved[1];
            var s = "function (exports, require) { " + source + " }";
            try {
              var func = sandbox ? evalcx(s, sandbox) : eval(s);
              func.apply(sandbox, [exports, function (name) {return require(name, parent)}]);
            } catch(e) { throw {message:"Module require("+name+") cause exception",e:e}; }
            return exports;
          }
          sandbox.require = require;
        }
        var functionObject = evalcx(source, sandbox);
      } else {
        var functionObject = eval(source);
      }
    } catch (err) {
      throw(["error", "compilation_error", err.toSource() + " (" + source + ")"]);
    };
    if (typeof(functionObject) == "function") {
      return functionObject;
    } else {
      throw(["error","compilation_error",
        "Expression does not eval to a function. (" + source.toSource() + ")"]);
    };
  },
  recursivelySeal : function(obj) {
    // seal() is broken in current Spidermonkey
    seal(obj);
    for (var propname in obj) {
      if (typeof doc[propname] == "object") {
        recursivelySeal(doc[propname]);
      }
    }
  }
}

// prints the object as JSON, and rescues and logs any toJSON() related errors
function respond(obj) {
  try {
    print(Couch.toJSON(obj));
  } catch(e) {
    log("Error converting object to JSON: " + e.toString());
    log("error on obj: "+ obj.toSource());
  }
};

function log(message) {
  // idea: query_server_config option for log level
  if (typeof message != "string") {
    message = Couch.toJSON(message);
  }
  respond(["log", message]);
};
