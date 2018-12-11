async = require('async')

EventEmitter = require('events').EventEmitter
QueuerErrors = require('./errors')
{ HEALTHY_STATUS, UNHEALTHY_STATUS } = require('./healthcheck_statuses')

class Watchdog extends EventEmitter
  constructor: (Model, { @pollInterval = 500 } = {}) ->
    throw new Error("'Model' must be passed to MongoQueuer.Watchdog constructor") unless Model?
    throw new Error("'Model' must use the MongoQueuer.TaskPlugin") unless Model._mongoQueuerOptions?

    # Params
    @Model = Model

    # Internal state
    @isShuttingDown = false

  # Error notifications
  notifyError: (err) ->
    if err instanceof QueuerErrors.ShutdownError
      console.log 'Process gone into shutdown mode. Not polling anymore.'
    else
      console.log err
      @emit 'error', err

  # Shutdown
  shutdown: (done) ->
    return if @isShuttingDown

    _cleanedUpFn = () =>
      console.log 'Worker quitting'
      @emit 'quit'

      done()

    @isShuttingDown = true
    _cleanedUpFn()

  # Fetch tasks
  run: () ->
    async.forever (done) =>
      if @isShuttingDown
        @notifyHealthStatusChange UNHEALTHY_STATUS
        return done new QueuerErrors.ShutdownError()

      @notifyHealthStatusChange HEALTHY_STATUS

      @Model._failTimedOutOne (err, object) =>
        if err?
          @notifyError err
          return setTimeout done, @pollInterval

        unless object?
          return setTimeout done, @pollInterval

        return done()
    , (err) =>
      @notifyError err

  notifyHealthStatusChange: (status) ->
    @healthStatus = status
    @emit status

module.exports = Watchdog
