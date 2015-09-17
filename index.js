
var crypto = require("crypto");

var redis = require("redis");
var minimist = require("minimist");
var loggerLib = require("logger");

// TODO compare current RPUSH+LPOP with LPUSH+RPOP
// TODO prevent possible message lost: "RPOPLPUSH messages processing.<nodeId>" -> handle -> "DEL processing.<nodeId>"

// parse arguments:
var argv = minimist(process.argv.slice(2), {
    string: [
        "nodeId",
        "logFilePath",
        "loggingLevel"
    ],
    boolean: [
        "getErrors"
    ],
    "default": {
        nodeId: generateRandomNodeId(),
        logFilePath: null,
        loggingLevel: "info",
        interval: 500,
        maxGenerated: 1000000
    }
});

// define error codes:
const ERROR_UNCAUGHT_EXCEPTION = 1;
const ERROR_REDIS_UNEXPECTED_ERROR = 2;
const ERROR_CANT_FETCH_ERROR_MESSAGES = 3;
const ERROR_SENDING_MESSAGE = 4;
const ERROR_RECEIVING_MESSAGE = 5;
const ERROR_SAVING_ERRORMESSAGE = 6;
const ERROR_CANT_SAVE_ALIVE_STATUS = 7;
const ERROR_CATCHING_LOCK = 8;

// define node roles:
const ROLE_UNKNOWN = 0;
const ROLE_PRODUCER = 1;
const ROLE_CONSUMER = 2;

/** message generation frequency */
const producingInterval = parseInt(argv.interval, 10);
/** how many messages to generate */
const maxGenerated = parseInt(argv.maxGenerated, 10);

// some constants:
/** @type {number} if producer don't update it's time more than `MAX_TIMEOUT` ms then it is probably dead */
const MAX_TIMEOUT = Math.max(1500, producingInterval / 2);
/** @type {number} save current time once in `KEEP_ALIVE_INTERVAL` ms */
const KEEP_ALIVE_INTERVAL = MAX_TIMEOUT / 4;

// used redis keys:
/** value is a timestamp (when producer last notified it's alive) */
const KEY_PRODUCER_LOCK = "producer.lock";
/** queue of messages */
const KEY_MESSAGES = "messages";
/** list of messages handled with error */
const KEY_ERROR_MESSAGES = "errorMessages";

//---[ MAIN ]----------------------------------------------------------------------------

var logger = createCustomizedLogger(argv.loggingLevel, argv.logFilePath);

// set initial role is unknown
var role = ROLE_UNKNOWN;

// define timers:
var keepAliveTimer;
var consumingTimer;
var producingTimer;

// handle signals:
process.on("SIGTERM", onExit);
process.on("SIGINT", onExit);
process.on("SIGQUIT", onExit);
// handle exceptions:
process.on("uncaughtException", (err) => {
    console.error("UncaughtException: ", err, err.stack);
    onExit(ERROR_UNCAUGHT_EXCEPTION);
});


// create redis client
var client = redis.createClient();

// start when redis connection ready
client.on("ready", () => {

    logger.debug("Redis connection is ready");

    if (argv.getErrors) {
        showErrors();
    } else {
        startProcessing();
    }
});

// handle redis unexpected errors
client.on("error", (err) => {
    logger.error("Redis error: ", err, err.stack);
    onExit(ERROR_REDIS_UNEXPECTED_ERROR);
});

//---[ SUBROUTINES ]---------------------------------------------------------------------

/**
 * Generates new message
 * @returns {number} message
 */
function getMessage() {
    this.cnt = this.cnt || 0;
    return this.cnt++;
}

/**
 * Handle passed message
 * @param {number} msg
 * @param {function(Error?, string?)} callback
 */
function eventHandler(msg, callback) {

    function onComplete() {
        var error = Math.random() > 0.85;
        callback(error, msg);
    }

    // processing takes time...
    setTimeout(onComplete, Math.floor(Math.random() * 1000));
}


/**
 * Start performing alive-status updates.
 * Sends to redis "PSETEX node.myNodeId (2x MAX_MESSAGE_HANDLING_DURATION) currentTimeMs" every `KEEP_ALIVE_INTERVAL` ms
 */
function startProcessing() {

    if (keepAliveTimer) {
        return;
    }

    logger.info("started");

    var knownOldTime = null;

    saveAliveStatusWithProducerChecking();

    function saveAliveStatusWithProducerChecking() {
        logger.debug("save alive status");

        var now = currentTime();

        switch (role) {
            case ROLE_PRODUCER:

                // the key will be removed as expired after producer stop or crash
                client.multi()
                    .getset(KEY_PRODUCER_LOCK, now)
                    .pexpire(KEY_PRODUCER_LOCK, MAX_TIMEOUT)
                    .exec((err, reply) => {
                        if (err) {
                            return onExit(ERROR_CANT_SAVE_ALIVE_STATUS);
                        }

                        var realOldTime = reply[0];
                        if (knownOldTime !== realOldTime && knownOldTime) {
                            // some other node has changed "producer.lock" value
                            startMessageConsuming();
                        }

                        scheduleNextStatusSave();
                    });
                break;

            case ROLE_CONSUMER:
            case ROLE_UNKNOWN:

                // try save current time
                client.setnx(KEY_PRODUCER_LOCK, now, (err, reply) => {
                    if (err) {
                        logger.error("Error getting lock: ", err, err.stack);
                        return onExit(ERROR_CATCHING_LOCK);
                    }

                    if (reply === 1) { // current time saved successfully, this node is the new producer
                        startMessageProducing();
                    } else { // some other node is working as a producer
                        startMessageConsuming();
                    }
                    scheduleNextStatusSave();
                });
                break;
        }
    }

    function scheduleNextStatusSave() {
        keepAliveTimer = setTimeout(saveAliveStatusWithProducerChecking, KEEP_ALIVE_INTERVAL);
    }
}

/**
 * Stops alive-status updates
 */
function stopProcessing() {
    if (keepAliveTimer) {
        logger.debug("stopping alive-status saving");
        clearTimeout(keepAliveTimer);
        keepAliveTimer = null;
    }
}

/**
 * Starts working as a message producer
 */
function startMessageProducing() {
    stopMessageConsuming();

    if (producingTimer) {
        return;
    }

    logger.info("start message producing");
    role = ROLE_PRODUCER;

    sendSingleMessageAndScheduleNext();

    function sendSingleMessageAndScheduleNext() {
        var msg = getMessage();
        var startTime = currentTime();
        if (maxGenerated !== -1 && msg >= maxGenerated) {
            clearInterval(producingTimer);
            logger.info("generated", maxGenerated, "messages and do nothing now");
            return;
        }

        client.rpush(KEY_MESSAGES, msg, (err) => {
            if (err) {
                logger.error("sending message:", msg, " failed:", err, err.stack);
                return onExit(ERROR_SENDING_MESSAGE);
            }
            logger.info("sending message:", msg, "success");

            var passedTime = currentTime() - startTime;
            producingTimer = setTimeout(sendSingleMessageAndScheduleNext, Math.max(0, producingInterval - passedTime));
        });
    }
}

/**
 * Stops working as a message producer
 */
function stopMessageProducing() {
    if (producingTimer) {
        logger.info("stop message producing");
        clearInterval(producingTimer);
        producingTimer = null;
        role = ROLE_UNKNOWN;
    }
}

/**
 * Starts working as a message consumer/handler
 */
function startMessageConsuming() {

    stopMessageProducing();

    if (consumingTimer) {
        return;
    }

    logger.info("start message consuming");
    role = ROLE_CONSUMER;

    var receivingTimeout = producingInterval + 1;

    consumingTimer = setTimeout(receiveAndHandleSingleMessage, receivingTimeout);

    function receiveAndHandleSingleMessage() {
        logger.debug("trying to get next message");

        if (role !== ROLE_CONSUMER) { // is not consumer any more
            return;
        }

        //console.time(nodeId + ":lpop:time");
        client.lpop(KEY_MESSAGES, (err, msg) => {
            //console.timeEnd(nodeId + ":lpop:time");

            if (err) {
                logger.error("Error receiving message: ", err, err.stack);
                return onExit(ERROR_RECEIVING_MESSAGE);
            }

            if (!msg) {
                logger.debug("messages queue is empty");
                receivingTimeout = receivingTimeout * 4 / 3;
                consumingTimer = setTimeout(receiveAndHandleSingleMessage, receivingTimeout);
                return;
            }

            logger.debug("start handling message: ", msg);
            receivingTimeout = Math.max(producingInterval, receivingTimeout / 2);

            // handle message
            eventHandler(msg, (err, msg) => {
                if (err) {
                    logger.info("handling message: ", msg, "fail");
                    client.rpush(KEY_ERROR_MESSAGES, msg, (err) => {
                        if (err) {
                            logger.error("Error saving error: ", err, err.stack);
                            return onExit(ERROR_SAVING_ERRORMESSAGE);
                        }

                        receiveAndHandleSingleMessage();
                    });
                    return;
                }

                logger.info("handling message: ", msg, "success");

                receiveAndHandleSingleMessage();
            })
        });
    }
}

/**
 * Stops working as a message consumer
 */
function stopMessageConsuming() {
    if (consumingTimer) {
        logger.info("stop message consuming");
        clearTimeout(consumingTimer);
        consumingTimer = null;
        role = ROLE_UNKNOWN;
    }
}

/**
 * called when it's time to exit
 * @param code
 */
function onExit(code) {
    stopMessageConsuming();
    stopMessageProducing();
    stopProcessing();
    client.quit();

    if (code === undefined) {
        code = 0;
    }
    logger.info("finishing with exit-code:", code);
    process.exit(code);
}

/**
 * read all error-handled messages from redis and print them out
 */
function showErrors() {

    logger.debug("started with 'getErrors' option");

    client.multi()
        .lrange(KEY_ERROR_MESSAGES, 0, -1)
        .ltrim(KEY_ERROR_MESSAGES, 1, 0)
        .exec((err, reply) => {
            if (err) {
                logger.debug("Error getting error messages: %s", err, err.stack);
                onExit(ERROR_CANT_FETCH_ERROR_MESSAGES);
            }
            var errorMessages = reply[0];
            if (errorMessages.length) {
                errorMessages.forEach((msg) => {
                    console.log(msg);
                });
            } else {
                console.log("No error messages so far");
            }
            client.quit();
        });
}

/**
 * @returns {number} current time in ms
 */
function currentTime() {
    return new Date().getTime();
}

/**
 * @returns {string} random string of 8 symbols,
 * may contain: digits, upper-case letters, lower-case letters, underscore and tilda (a-la base64)
 */
function generateRandomNodeId() {
    const ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_~";
    const ALEN = ALPHABET.length;

    var buf = crypto.randomBytes(8);
    var res = "";
    for (var i = 0; i < buf.length; i++) {
        var byte = buf.readUInt8(i);
        res += ALPHABET.charAt(byte % ALEN);
    }
    return res;
}

/**
 * factory-method for logger instance
 * @param {string} loggingLevel logging level (warn, info, debug, etc.)
 * @param {string} logFilePath path to log file
 */
function createCustomizedLogger(loggingLevel, logFilePath) {
    var logger = loggerLib.createLogger(logFilePath);
    logger.setLevel(loggingLevel);
    // customize logger formatter
    logger._format = logger.format;
    logger.format = function customLogMessageFormat(level, date, message) {
        // format date-time as "yyyy-MM-dd HH:mm:ss.fff"
        function lpad(val, size) {
            var str = val.toString(), l = str.length;
            size = size || 2;
            return l < size ? "000".substr(0, size - l) + str : str;
        }

        var dateFormatted = `${date.getFullYear()}-${lpad(date.getMonth())}-${lpad(date.getDate())} ` +
            `${lpad(date.getHours())}:${lpad(date.getMinutes())}:${lpad(date.getSeconds())}.` +
            `${lpad(date.getMilliseconds(), 3)}`;

        return this._format(level, dateFormatted, `[${argv.nodeId}]\t${message}`);
    };
    return logger;
}
