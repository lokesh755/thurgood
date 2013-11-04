var ObjectID = require('mongodb').ObjectID;
var amqp = require('amqp');
var syslogProducer = require('glossy').Produce;
var glossy = new syslogProducer({ type: 'BSD' });
var Q = require("q");
var papertrail = require("../lib/papertrail");
var crypto = require('crypto');
var request = require('request');


/**
 * GET /jobs
 * GET /jobs/:id
 */
exports.action = {
  name: "jobsFetch",
  description: "Returns a list of jobs, or a specific one if id is defined. Method: GET",
  inputs: {
    required: [],
    optional: ['q', 'fields', 'sort', 'limit', 'skip', 'id'],
  },
  authenticated: false,
  outputExample: {},
  version: 1.0,
  run: function(api, connection, next) {
    api.mongo.get(api, connection, next, api.mongo.collections.jobs);
  }
};

/**
 * GET /jobs/:id/complete
 */
exports.jobsComplete = {
  name: "jobsComplete",
  description: "Sets a job's status as completed. Method: GET",
  inputs: {
    required: ['id'],
    optional: [],
  },
  authenticated: false,
  outputExample: {},
  version: 1.0,
  run: function(api, connection, next) {
    var selector, collection = api.mongo.collections.loggerSystems;

    // Validate id and build selector
    try {
      selector = { _id: new ObjectID(connection.params.id) };
    } catch(err) {
      api.response.badRequest(connection, "Id is not a valid ObjectID");
      return next(connection, true);
    }

    var newDoc = {
      status: 'complete',
      endTime: new Date().getTime(),
      updatedAt: new Date().getTime()
    };

    // Modify document
    api.mongo.collections.jobs.findAndModify(selector, {}, { $set: newDoc }, { new: true, w:1 }, function(err, job) {
      if (!err && job) {
        var newDoc = {
          jobId: null,
          status: 'available',
          updatedAt: new Date().getTime()
        };

        // Find server and release it
        api.mongo.collections.servers.findAndModify({ jobId: job._id }, {}, { $set: newDoc }, { new: true, w:1 }, function(err, server) {
          if (!err) {
            api.response.success(connection, "Job updated and server released");
          } else {
            api.response.error(connection, err);
          }

          next(connection, true);
        });
      } else if (!job) {
        api.response.error(connection, "Job not found");
        next(connection, true);
      } else {
        api.response.error(connection, err);
        next(connection, true);
      }
    });
  }
};

/**
 * POST /jobs
 */
exports.jobsCreate = {
  name: "jobsCreate",
  description: "Creates a new job. Method: POST",
  inputs: {
    required: ['email', 'platform', 'language', 'userId', 'codeUrl'],
    optional: ['loggerId', 'logger', 'options','papertrailId'],
  },
  authenticated: false,
  outputExample: {},
  version: 1.0,
  run: function(api, connection, next) {
    // If logger name is provided, use it to search for id
    if (!connection.params.loggerId && connection.params.logger) {
      api.mongo.collections.loggerSystems.findOne({ name: connection.params.logger }, { _id:1 }, function(err, logger) {
        if (!err && logger) {
          connection.params.loggerId = new String(logger._id);
          api.mongo.create(api, connection, next, api.mongo.collections.jobs, api.mongo.schema.job);
        } else if (!logger) {
           

            var accountDoc = api.mongo.schema.new(api.mongo.schema.loggerAccount);
            accountDoc.name = connection.params.userId;
            accountDoc.email = connection.params.email;
            accountDoc.papertrailId = connection.params.papertrailId || accountDoc.name;

            var params = {
              id: accountDoc.papertrailId,
              name: accountDoc.name,
              plan: "free",
              user: {
                id: accountDoc.name,
                email: accountDoc.email
              }
            };
          
          request.post({ url: api.configData.papertrail.accountsUrl , form: params, auth: api.configData.papertrail.auth }, function (err, response, body) {
            if (err) {
              api.response.error(connection, err);
              next(connection, true);
            } else {
              
              body = JSON.parse(body);
              if (!body.id || !body.api_token) {
                // Check if the account already exists
                api.mongo.collections.loggerAccounts.findOne({ name: connection.params.userId }, function(err, account) {
                  if (!err && account) {
                    console.log("Account already exists");
                    verifyLoggerAccount();
                  } else {
                    api.response.error(connection, body.message);
                  }

                  next(connection, true);
                });
              } else 
              {
                accountDoc.papertrailId = body.id;
                accountDoc.papertrailApiToken = body.api_token;
                
                // Insert document into the database
                api.mongo.collections.loggerAccounts.insert(accountDoc, { w:1 }, function(err, result) {
                  if (!err) {
                    console.log("Account created successfully");
                    verifyLoggerAccount();
                  } else {
                    api.response.error(connection, "Account couldn't be created "+err, undefined, 404);
                  }

                  
                });
              }
            }
          });

          
          

          //api.response.error(connection, "Logger not found url ", undefined, 404);
          next(connection, true);
        } else {
          api.response.error(connection, err);
          next(connection, true);
        }
      });
    } else {
      api.mongo.create(api, connection, next, api.mongo.collections.jobs, api.mongo.schema.job);
    }


  function verifyLoggerAccount()
  {
    api.mongo.collections.loggerAccounts.findOne({ name: connection.params.userId}, { _id:1 }, function(err, loggerAccount) {
          if (!err && loggerAccount) {
             
             console.log("Corresponding logger account is found");
             connection.params.loggerAccountId = new String(loggerAccount._id);
             createLogger();

             
          }
          else if (!loggerAccount) {
          
             console.log("corresponding loggeraccount is not found");
             api.response.error(connection, "Account couldn't be found "+err, undefined, 404);
          
          }  
          else
          {
            api.response.error(connection, err);
          }
         });
  }
   
// Create a logger 
    // 1. create logger on papertrail
    // 2. create logger in the db
    // 3. respond with the created logger
    function createLogger() {
      Q.all([api, buildLogger()])
        .spread(papertrail.createLogger)
        .then(insertLogger)
        .then(insertJob);
    }

    function buildLogger() {
      var logger = api.mongo.schema.new(api.mongo.schema.loggerSystem);
      logger.name = connection.params.logger;
      logger.loggerAccountId = connection.params.loggerAccountId;
      logger.papertrailId = connection.params.papertrailId || crypto.randomBytes(16).toString('hex');
      return logger;
    }

    // Insert document into the database
    function insertLogger(logger) {
       console.log("[LoggerCreate]", "Insert Logger to DB : " + logger);
      // var deferred = Q.defer();
      // collection.insert(logger, deferred.makeNodeResolver());
      // return deferred.promise;

      api.mongo.collections.loggerSystems.insert(logger, { w:1 }, function(err, result) {
                  if (!err) {
                    console.log("loggerSystem created successfully");
                  } else {
                    api.response.error(connection, "loggerSystem couldn't be created "+err, undefined, 404);
                  }

                  
                });
    }

    function insertJob(logger) {
      console.log("[LoggerSystemCreate]", "Inserting Job to DB : " + logger);
      console.log("Querying for loggerSystem");
             api.mongo.collections.loggerSystems.findOne({ name: connection.params.logger }, { _id:1 }, function(err, loggerSystem) {
             
               if (!err && loggerSystem) {
                  console.log("Started Creating job");
                  connection.params.loggerId = new String(loggerSystem._id);
                  api.mongo.create(api, connection, next, api.mongo.collections.jobs, api.mongo.schema.job);
                  console.log("Job created successfully");
                } else if (!logger) {
                  api.response.error(connection, err);
                }
             
             });
    }

    function respondOk(logger) {
      api.response.success(connection, undefined, logger);
      next(connection, true);
    }

    function respondError(err) {
      api.response.error(connection, err);
      next(connection, true);
    }



  }
};

/**
 * POST /jobs/:id/message
 */
exports.jobsMessage = {
  name: "jobsMessage",
  description: "Sends a message to the job's logger. Method: POST",
  inputs: {
    required: ['id', 'message'],
    optional: ['facility', 'severity'],
  },
  authenticated: false,
  outputExample: {},
  version: 1.0,
  run: function(api, connection, next) {
    var selector;

    // Validate id and build selector
    try {
      selector = { _id: new ObjectID(connection.params.id) };
    } catch(err) {
      api.response.badRequest(connection, "Id is not a valid ObjectID");
      return next(connection, true);
    }

    // Find job's logger
    api.mongo.collections.jobs.findOne(selector, function(err, job) {
      if (!err && job) {
        api.mongo.collections.loggerSystems.findOne({ _id: job.loggerId }, function(err, logger) {
          if (!err && logger) {
            // Send message
            glossy.produce({
              facility: connection.params.facility,
              severity: connection.params.severity || 'info',
              host: logger.syslogHostname + ":" + logger.syslogPort,
              date: new Date(),
              message: connection.params.message
            }, function(syslogMsg){
              api.response.success(connection, syslogMsg);
              next(connection, true);
            });
          } else if (!logger) {
            api.response.error(connection, "Logger not found");
            next(connection, true);
          } else {
            api.response.error(connection, err);
            next(connection, true);
          }
        });
      } else if (!job) {
        api.response.error(connection, "Job not found");
        next(connection, true);
      } else {
        api.response.error(connection, err);
        next(connection, true);
      }
    });
  }
};

/**
 * POST /jobs/:id/submit
 */
exports.jobsSubmit = {
  name: "jobsSubmit",
  description: "Submits a job. Method: PUT",
  inputs: {
    required: ['id'],
    optional: [],
  },
  authenticated: false,
  outputExample: {},
  version: 1.0,
  run: function(api, connection, next) {
    var selector;

    // Validate id and build selector
    try {
      selector = { _id: new ObjectID(connection.params.id) };
    } catch(err) {
      api.response.badRequest(connection, "Id is not a valid ObjectID");
      return next(connection, true);
    }

    // Find document
    api.mongo.collections.jobs.findOne(selector, function(err, doc) {
      if (!err && doc) {
        // can submit a job no matter the current status
        var serverSelector = {
          languages: doc.language,
          platform: doc.platform,
          status: 'available'
        };

        var newDoc = {
          jobId: doc._id,
          status: 'reserved',
          updatedAt: new Date().getTime()
        };

        // Find server and reserve it
        api.mongo.collections.servers.findAndModify(serverSelector, {}, { $set: newDoc }, { new: true, w:1 }, function(err, server) {
          if (!err && server) {
            var message = {
              job_id: server.jobId,
              type: doc.language
            };

            var newDoc = {
              status: 'submitted',
              updatedAt: new Date().getTime()
            };

            // Set job status to submitted
            api.mongo.collections.jobs.update({ _id: doc._id }, { $set: newDoc }, { w:1 }, function(err, result) {
              if (!err) {
                // Publish message
                api.configData.rabbitmq.connection.publish(api.configData.rabbitmq.queue, message);
                api.response.success(connection, "Job has been successfully submitted");
              } else {
                api.response.error(connection, err);
              }

              next(connection, true);
            });
          } else if (!server) {
            api.response.error(connection, "Could not find any available servers. Try again in a few minutes");
            next(connection, true);
          } else {
            api.response.error(connection, err);
            next(connection, true);
          }
        });
      } else if (!doc) {
        api.response.error(connection, "Job not found");
        next(connection, true);
      } else {
        api.response.error(connection, err);
        next(connection, true);
      }
    });
  }
};

/**
 * PUT /jobs/:id
 */
exports.jobsUpdate = {
  name: "jobsUpdate",
  description: "Updates a job. Method: PUT",
  inputs: {
    required: ['id'],
    optional: ['status', 'email', 'platform', 'language', 'papertrailSystem', 'userId', 'codeUrl', 'options', 'startTime', 'endTime'],
  },
  authenticated: false,
  outputExample: {},
  version: 1.0,
  run: function(api, connection, next) {
    api.mongo.update(api, connection, next, api.mongo.collections.jobs, api.mongo.schema.job);
  }
};

/**
 * POST /jobs/:id/publish
 */
exports.jobsMessage = {
  name: "jobsPublish",
  description: "Publishes a message to the queue. Method: POST",
  inputs: {
    required: ['message'],
    optional: [],
  },
  authenticated: false,
  outputExample: {},
  version: 1.0,
  run: function(api, connection, next) {
    api.configData.rabbitmq.connection.publish(api.configData.rabbitmq.queue, connection.params.message);  
    api.response.success(connection, "Message successfully published.");
    next(connection, true);
  }
};

