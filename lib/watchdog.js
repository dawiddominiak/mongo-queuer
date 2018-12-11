var EventEmitter, HEALTHY_STATUS, QueuerErrors, UNHEALTHY_STATUS, Watchdog, async, ref,
  extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  hasProp = {}.hasOwnProperty;

async = require('async');

EventEmitter = require('events').EventEmitter;

QueuerErrors = require('./errors');

ref = require('./healthcheck_statuses'), HEALTHY_STATUS = ref.HEALTHY_STATUS, UNHEALTHY_STATUS = ref.UNHEALTHY_STATUS;

Watchdog = (function(superClass) {
  extend(Watchdog, superClass);

  function Watchdog(Model, arg) {
    var ref1;
    this.pollInterval = (ref1 = (arg != null ? arg : {}).pollInterval) != null ? ref1 : 500;
    if (Model == null) {
      throw new Error("'Model' must be passed to MongoQueuer.Watchdog constructor");
    }
    if (Model._mongoQueuerOptions == null) {
      throw new Error("'Model' must use the MongoQueuer.TaskPlugin");
    }
    this.Model = Model;
    this.isShuttingDown = false;
  }

  Watchdog.prototype.notifyError = function(err) {
    if (err instanceof QueuerErrors.ShutdownError) {
      return console.log('Process gone into shutdown mode. Not polling anymore.');
    } else {
      console.log(err);
      return this.emit('error', err);
    }
  };

  Watchdog.prototype.shutdown = function(done) {
    var _cleanedUpFn;
    if (this.isShuttingDown) {
      return;
    }
    _cleanedUpFn = (function(_this) {
      return function() {
        console.log('Worker quitting');
        _this.emit('quit');
        return done();
      };
    })(this);
    this.isShuttingDown = true;
    return _cleanedUpFn();
  };

  Watchdog.prototype.run = function() {
    return async.forever((function(_this) {
      return function(done) {
        if (_this.isShuttingDown) {
          _this.notifyHealthStatusChange(UNHEALTHY_STATUS);
          return done(new QueuerErrors.ShutdownError());
        }
        _this.notifyHealthStatusChange(HEALTHY_STATUS);
        return _this.Model._failTimedOutOne(function(err, object) {
          if (err != null) {
            _this.notifyError(err);
            return setTimeout(done, _this.pollInterval);
          }
          if (object == null) {
            return setTimeout(done, _this.pollInterval);
          }
          return done();
        });
      };
    })(this), (function(_this) {
      return function(err) {
        return _this.notifyError(err);
      };
    })(this));
  };

  Watchdog.prototype.notifyHealthStatusChange = function(status) {
    this.healthStatus = status;
    return this.emit(status);
  };

  return Watchdog;

})(EventEmitter);

module.exports = Watchdog;

//# sourceMappingURL=data:application/json;charset=utf8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibGliL3dhdGNoZG9nLmpzIiwic291cmNlcyI6WyJsaWIvd2F0Y2hkb2cuY29mZmVlIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQUFBLElBQUEsa0ZBQUE7RUFBQTs7O0FBQUEsS0FBQSxHQUFRLE9BQUEsQ0FBUSxPQUFSOztBQUVSLFlBQUEsR0FBZSxPQUFBLENBQVEsUUFBUixDQUFpQixDQUFDOztBQUNqQyxZQUFBLEdBQWUsT0FBQSxDQUFRLFVBQVI7O0FBQ2YsTUFBdUMsT0FBQSxDQUFRLHdCQUFSLENBQXZDLEVBQUUsbUNBQUYsRUFBa0I7O0FBRVo7OztFQUNTLGtCQUFDLEtBQUQsRUFBUSxHQUFSO0FBQ1gsUUFBQTtJQURxQixJQUFDLENBQUEsK0VBQWU7SUFDckMsSUFBcUYsYUFBckY7QUFBQSxZQUFNLElBQUksS0FBSixDQUFVLDREQUFWLEVBQU47O0lBQ0EsSUFBc0UsaUNBQXRFO0FBQUEsWUFBTSxJQUFJLEtBQUosQ0FBVSw2Q0FBVixFQUFOOztJQUdBLElBQUMsQ0FBQSxLQUFELEdBQVM7SUFHVCxJQUFDLENBQUEsY0FBRCxHQUFrQjtFQVJQOztxQkFXYixXQUFBLEdBQWEsU0FBQyxHQUFEO0lBQ1gsSUFBRyxHQUFBLFlBQWUsWUFBWSxDQUFDLGFBQS9CO2FBQ0UsT0FBTyxDQUFDLEdBQVIsQ0FBWSx1REFBWixFQURGO0tBQUEsTUFBQTtNQUdFLE9BQU8sQ0FBQyxHQUFSLENBQVksR0FBWjthQUNBLElBQUMsQ0FBQSxJQUFELENBQU0sT0FBTixFQUFlLEdBQWYsRUFKRjs7RUFEVzs7cUJBUWIsUUFBQSxHQUFVLFNBQUMsSUFBRDtBQUNSLFFBQUE7SUFBQSxJQUFVLElBQUMsQ0FBQSxjQUFYO0FBQUEsYUFBQTs7SUFFQSxZQUFBLEdBQWUsQ0FBQSxTQUFBLEtBQUE7YUFBQSxTQUFBO1FBQ2IsT0FBTyxDQUFDLEdBQVIsQ0FBWSxpQkFBWjtRQUNBLEtBQUMsQ0FBQSxJQUFELENBQU0sTUFBTjtlQUVBLElBQUEsQ0FBQTtNQUphO0lBQUEsQ0FBQSxDQUFBLENBQUEsSUFBQTtJQU1mLElBQUMsQ0FBQSxjQUFELEdBQWtCO1dBQ2xCLFlBQUEsQ0FBQTtFQVZROztxQkFhVixHQUFBLEdBQUssU0FBQTtXQUNILEtBQUssQ0FBQyxPQUFOLENBQWMsQ0FBQSxTQUFBLEtBQUE7YUFBQSxTQUFDLElBQUQ7UUFDWixJQUFHLEtBQUMsQ0FBQSxjQUFKO1VBQ0UsS0FBQyxDQUFBLHdCQUFELENBQTBCLGdCQUExQjtBQUNBLGlCQUFPLElBQUEsQ0FBSyxJQUFJLFlBQVksQ0FBQyxhQUFqQixDQUFBLENBQUwsRUFGVDs7UUFJQSxLQUFDLENBQUEsd0JBQUQsQ0FBMEIsY0FBMUI7ZUFFQSxLQUFDLENBQUEsS0FBSyxDQUFDLGdCQUFQLENBQXdCLFNBQUMsR0FBRCxFQUFNLE1BQU47VUFDdEIsSUFBRyxXQUFIO1lBQ0UsS0FBQyxDQUFBLFdBQUQsQ0FBYSxHQUFiO0FBQ0EsbUJBQU8sVUFBQSxDQUFXLElBQVgsRUFBaUIsS0FBQyxDQUFBLFlBQWxCLEVBRlQ7O1VBSUEsSUFBTyxjQUFQO0FBQ0UsbUJBQU8sVUFBQSxDQUFXLElBQVgsRUFBaUIsS0FBQyxDQUFBLFlBQWxCLEVBRFQ7O0FBR0EsaUJBQU8sSUFBQSxDQUFBO1FBUmUsQ0FBeEI7TUFQWTtJQUFBLENBQUEsQ0FBQSxDQUFBLElBQUEsQ0FBZCxFQWdCRSxDQUFBLFNBQUEsS0FBQTthQUFBLFNBQUMsR0FBRDtlQUNBLEtBQUMsQ0FBQSxXQUFELENBQWEsR0FBYjtNQURBO0lBQUEsQ0FBQSxDQUFBLENBQUEsSUFBQSxDQWhCRjtFQURHOztxQkFvQkwsd0JBQUEsR0FBMEIsU0FBQyxNQUFEO0lBQ3hCLElBQUMsQ0FBQSxZQUFELEdBQWdCO1dBQ2hCLElBQUMsQ0FBQSxJQUFELENBQU0sTUFBTjtFQUZ3Qjs7OztHQXJETDs7QUF5RHZCLE1BQU0sQ0FBQyxPQUFQLEdBQWlCIiwic291cmNlc0NvbnRlbnQiOlsiYXN5bmMgPSByZXF1aXJlKCdhc3luYycpXG5cbkV2ZW50RW1pdHRlciA9IHJlcXVpcmUoJ2V2ZW50cycpLkV2ZW50RW1pdHRlclxuUXVldWVyRXJyb3JzID0gcmVxdWlyZSgnLi9lcnJvcnMnKVxueyBIRUFMVEhZX1NUQVRVUywgVU5IRUFMVEhZX1NUQVRVUyB9ID0gcmVxdWlyZSgnLi9oZWFsdGhjaGVja19zdGF0dXNlcycpXG5cbmNsYXNzIFdhdGNoZG9nIGV4dGVuZHMgRXZlbnRFbWl0dGVyXG4gIGNvbnN0cnVjdG9yOiAoTW9kZWwsIHsgQHBvbGxJbnRlcnZhbCA9IDUwMCB9ID0ge30pIC0+XG4gICAgdGhyb3cgbmV3IEVycm9yKFwiJ01vZGVsJyBtdXN0IGJlIHBhc3NlZCB0byBNb25nb1F1ZXVlci5XYXRjaGRvZyBjb25zdHJ1Y3RvclwiKSB1bmxlc3MgTW9kZWw/XG4gICAgdGhyb3cgbmV3IEVycm9yKFwiJ01vZGVsJyBtdXN0IHVzZSB0aGUgTW9uZ29RdWV1ZXIuVGFza1BsdWdpblwiKSB1bmxlc3MgTW9kZWwuX21vbmdvUXVldWVyT3B0aW9ucz9cblxuICAgICMgUGFyYW1zXG4gICAgQE1vZGVsID0gTW9kZWxcblxuICAgICMgSW50ZXJuYWwgc3RhdGVcbiAgICBAaXNTaHV0dGluZ0Rvd24gPSBmYWxzZVxuXG4gICMgRXJyb3Igbm90aWZpY2F0aW9uc1xuICBub3RpZnlFcnJvcjogKGVycikgLT5cbiAgICBpZiBlcnIgaW5zdGFuY2VvZiBRdWV1ZXJFcnJvcnMuU2h1dGRvd25FcnJvclxuICAgICAgY29uc29sZS5sb2cgJ1Byb2Nlc3MgZ29uZSBpbnRvIHNodXRkb3duIG1vZGUuIE5vdCBwb2xsaW5nIGFueW1vcmUuJ1xuICAgIGVsc2VcbiAgICAgIGNvbnNvbGUubG9nIGVyclxuICAgICAgQGVtaXQgJ2Vycm9yJywgZXJyXG5cbiAgIyBTaHV0ZG93blxuICBzaHV0ZG93bjogKGRvbmUpIC0+XG4gICAgcmV0dXJuIGlmIEBpc1NodXR0aW5nRG93blxuXG4gICAgX2NsZWFuZWRVcEZuID0gKCkgPT5cbiAgICAgIGNvbnNvbGUubG9nICdXb3JrZXIgcXVpdHRpbmcnXG4gICAgICBAZW1pdCAncXVpdCdcblxuICAgICAgZG9uZSgpXG5cbiAgICBAaXNTaHV0dGluZ0Rvd24gPSB0cnVlXG4gICAgX2NsZWFuZWRVcEZuKClcblxuICAjIEZldGNoIHRhc2tzXG4gIHJ1bjogKCkgLT5cbiAgICBhc3luYy5mb3JldmVyIChkb25lKSA9PlxuICAgICAgaWYgQGlzU2h1dHRpbmdEb3duXG4gICAgICAgIEBub3RpZnlIZWFsdGhTdGF0dXNDaGFuZ2UgVU5IRUFMVEhZX1NUQVRVU1xuICAgICAgICByZXR1cm4gZG9uZSBuZXcgUXVldWVyRXJyb3JzLlNodXRkb3duRXJyb3IoKVxuXG4gICAgICBAbm90aWZ5SGVhbHRoU3RhdHVzQ2hhbmdlIEhFQUxUSFlfU1RBVFVTXG5cbiAgICAgIEBNb2RlbC5fZmFpbFRpbWVkT3V0T25lIChlcnIsIG9iamVjdCkgPT5cbiAgICAgICAgaWYgZXJyP1xuICAgICAgICAgIEBub3RpZnlFcnJvciBlcnJcbiAgICAgICAgICByZXR1cm4gc2V0VGltZW91dCBkb25lLCBAcG9sbEludGVydmFsXG5cbiAgICAgICAgdW5sZXNzIG9iamVjdD9cbiAgICAgICAgICByZXR1cm4gc2V0VGltZW91dCBkb25lLCBAcG9sbEludGVydmFsXG5cbiAgICAgICAgcmV0dXJuIGRvbmUoKVxuICAgICwgKGVycikgPT5cbiAgICAgIEBub3RpZnlFcnJvciBlcnJcblxuICBub3RpZnlIZWFsdGhTdGF0dXNDaGFuZ2U6IChzdGF0dXMpIC0+XG4gICAgQGhlYWx0aFN0YXR1cyA9IHN0YXR1c1xuICAgIEBlbWl0IHN0YXR1c1xuXG5tb2R1bGUuZXhwb3J0cyA9IFdhdGNoZG9nXG4iXX0=
