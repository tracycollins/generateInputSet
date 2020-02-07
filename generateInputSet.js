const os = require("os");

let hostname = os.hostname();
hostname = hostname.replace(/.local/g, "");
hostname = hostname.replace(/.home/g, "");
hostname = hostname.replace(/.at.net/g, "");
hostname = hostname.replace(/.fios-router.home/g, "");
hostname = hostname.replace(/word0-instance-1/g, "google");
hostname = hostname.replace(/word-1/g, "google");
hostname = hostname.replace(/word/g, "google");

const MAX_TEST_INPUTS = 10000

const MODULE_NAME = "generateInputSets";
const MODULE_ID_PREFIX = "GIS";
const MODULE_ID = MODULE_ID_PREFIX + "_node_" + hostname;

const DEFAULT_GENERATE_BOTH_USER_PROFILE_ONLY_AND_ALL_HISTOGRAMS_INPUTS = true;
const DEFAULT_USER_PROFILE_ONLY_FLAG = false;
const DEFAULT_VERBOSE_MODE = false;

const ONE_SECOND = 1000;
const ONE_MINUTE = ONE_SECOND*60;

const GLOBAL_TEST_MODE = false; // applies to parent and all children
const STATS_UPDATE_INTERVAL = ONE_MINUTE;

const DEFAULT_INPUTS_FILE_PREFIX = "inputs";

const DEFAULT_MIN_DOM_MIN = 0.375;

const DEFAULT_MIN_TOTAL_MIN = 3;

const DEFAULT_MIN_TOTAL_MIN_TYPE_HASHMAP = {};
DEFAULT_MIN_TOTAL_MIN_TYPE_HASHMAP.emoji = 75;
DEFAULT_MIN_TOTAL_MIN_TYPE_HASHMAP.hashtags = 75;
DEFAULT_MIN_TOTAL_MIN_TYPE_HASHMAP.ngrams = 10;
DEFAULT_MIN_TOTAL_MIN_TYPE_HASHMAP.images = 1000;
DEFAULT_MIN_TOTAL_MIN_TYPE_HASHMAP.locations = 40;
DEFAULT_MIN_TOTAL_MIN_TYPE_HASHMAP.places = 3;
DEFAULT_MIN_TOTAL_MIN_TYPE_HASHMAP.sentiment = 1;
DEFAULT_MIN_TOTAL_MIN_TYPE_HASHMAP.urls = 3;
DEFAULT_MIN_TOTAL_MIN_TYPE_HASHMAP.userMentions = 70;
DEFAULT_MIN_TOTAL_MIN_TYPE_HASHMAP.words = 1300;

const DEFAULT_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP = {};
DEFAULT_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP.emoji = 50;
DEFAULT_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP.hashtags = 50;
DEFAULT_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP.ngrams = 5;
DEFAULT_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP.images = 750;
DEFAULT_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP.locations = 20;
DEFAULT_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP.places = 1;
DEFAULT_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP.sentiment = 1;
DEFAULT_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP.urls = 1;
DEFAULT_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP.userMentions = 20;
DEFAULT_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP.words = 500;

const DEFAULT_MIN_INPUTS_GENERATED = 1500;
const DEFAULT_MAX_INPUTS_GENERATED = 2000;
const DEFAULT_MAX_NUM_INPUTS_PER_TYPE = 200;
const DEFAULT_MIN_NUM_INPUTS_PER_TYPE = 100;

let configuration = {};

configuration.verbose = DEFAULT_VERBOSE_MODE;

configuration.inputsFilePrefix = DEFAULT_INPUTS_FILE_PREFIX;

configuration.generateBothUserProfileOnlyAndAllHistogramsInputs = DEFAULT_GENERATE_BOTH_USER_PROFILE_ONLY_AND_ALL_HISTOGRAMS_INPUTS;
configuration.userProfileOnlyFlag = DEFAULT_USER_PROFILE_ONLY_FLAG;
configuration.testMode = GLOBAL_TEST_MODE;
configuration.statsUpdateIntervalTime = STATS_UPDATE_INTERVAL;

configuration.minTotalMin = DEFAULT_MIN_TOTAL_MIN;
configuration.minTotalMinHashMap = DEFAULT_MIN_TOTAL_MIN_TYPE_HASHMAP;
configuration.minTotalMinUserProfileHashMap = DEFAULT_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP;
configuration.minDominantMin = DEFAULT_MIN_DOM_MIN;

configuration.minInputsGenerated = DEFAULT_MIN_INPUTS_GENERATED;
configuration.maxInputsGenerated = DEFAULT_MAX_INPUTS_GENERATED;
configuration.minNumInputsPerType = DEFAULT_MIN_NUM_INPUTS_PER_TYPE;
configuration.maxNumInputsPerType = DEFAULT_MAX_NUM_INPUTS_PER_TYPE;

configuration.keepaliveInterval = Number(ONE_MINUTE)+1;
configuration.quitOnComplete = true;

global.wordAssoDb = require("@threeceelabs/mongoose-twitter");

const tcuChildName = MODULE_ID_PREFIX + "_TCU";
const ThreeceeUtilities = require("@threeceelabs/threecee-utilities");
const tcUtils = new ThreeceeUtilities(tcuChildName);

const moment = require("moment");

let defaultConfiguration = {}; // general configuration for TFE
let hostConfiguration = {}; // host-specific configuration for TFE

const PRIMARY_HOST = process.env.PRIMARY_HOST || "google";

const DEFAULT_INPUT_TYPES = [
  "emoji",
  "friends",
  "hashtags",  
  "images", 
  "locations", 
  "media", 
  "ngrams", 
  "places", 
  "sentiment", 
  "urls", 
  "userMentions", 
  "words"
];

DEFAULT_INPUT_TYPES.sort();

const USER_PROFILE_INPUT_TYPES = [
  "emoji",
  "hashtags",  
  "images", 
  "locations", 
  "ngrams", 
  "places", 
  "sentiment", 
  "urls", 
  "userMentions", 
  "words"
];

USER_PROFILE_INPUT_TYPES.sort();

const compactDateTimeFormat = "YYYYMMDD_HHmmss";

const INIT_DOM_MIN = 0.999999;
const INIT_TOT_MIN = 100;

const fs = require("fs");
const JSONStream = require("JSONStream");
const path = require("path");
const merge = require("deepmerge");
const util = require("util");
const _ = require("lodash");
const treeify = require("treeify");
const dot = require("dot-object");

const async = require("async");
const debug = require("debug")("gis");

const deepcopy = require("deep-copy");
const table = require("text-table");

const chalk = require("chalk");
const chalkBlue = chalk.blue;
const chalkBlueBold = chalk.blue.bold;
const chalkError = chalk.bold.red;
const chalkAlert = chalk.red;
const chalkLog = chalk.gray;
const chalkInfo = chalk.black;

const inputsIdSet = new Set();

let DROPBOX_ROOT_FOLDER;

const defaultInputsConfigFile = "default_networkInputsConfig.json";

if (hostname === "google") {
  DROPBOX_ROOT_FOLDER = "/home/tc/Dropbox/Apps/wordAssociation";
}
else {
  DROPBOX_ROOT_FOLDER = "/Users/tc/Dropbox/Apps/wordAssociation";
}

// ==================================================================
// DROPBOX
// ==================================================================
configuration.DROPBOX = {};

configuration.DROPBOX.DROPBOX_CONFIG_FILE = process.env.DROPBOX_CONFIG_FILE || MODULE_NAME + "Config.json";
configuration.DROPBOX.DROPBOX_STATS_FILE = process.env.DROPBOX_STATS_FILE || MODULE_NAME + "Stats.json";

const configDefaultFolder = path.join(DROPBOX_ROOT_FOLDER, "config/utility/default");
const configHostFolder = path.join(DROPBOX_ROOT_FOLDER, "config/utility", hostname);

const configDefaultFile = "default_" + configuration.DROPBOX.DROPBOX_CONFIG_FILE;
const configHostFile = hostname + "_" + configuration.DROPBOX.DROPBOX_CONFIG_FILE;

//=========================================================================
// SLACK
//=========================================================================

const slackChannel = "gis";
const HashMap = require("hashmap").HashMap;
const channelsHashMap = new HashMap();

const slackOAuthAccessToken = "xoxp-3708084981-3708084993-206468961315-ec62db5792cd55071a51c544acf0da55";
const slackConversationId = "D65CSAELX"; // wordbot
const slackRtmToken = "xoxb-209434353623-bNIoT4Dxu1vv8JZNgu7CDliy";

let slackRtmClient;
let slackWebClient;

async function slackSendRtmMessage(msg){
  console.log(chalkBlue(MODULE_ID_PREFIX + " | SLACK RTM | SEND: " + msg));

  const sendResponse = await slackRtmClient.sendMessage(msg, slackConversationId);

  console.log(chalkLog(MODULE_ID_PREFIX + " | SLACK RTM | >T\n" + jsonPrint(sendResponse)));
  return sendResponse;
}

async function slackSendWebMessage(msgObj){
  const token = msgObj.token || slackOAuthAccessToken;
  const channel = msgObj.channel || configuration.slackChannel.id;
  const text = msgObj.text || msgObj;

  const message = {
    token: token, 
    channel: channel,
    text: text
  };

  if (msgObj.attachments !== undefined) {
    message.attachments = msgObj.attachments;
  }

  if (slackWebClient && slackWebClient !== undefined) {
    const sendResponse = await slackWebClient.chat.postMessage(message);
    return sendResponse;
  }
  else {
    console.log(chalkAlert(MODULE_ID_PREFIX + " | SLACK WEB NOT CONFIGURED | SKIPPING SEND SLACK MESSAGE\n" + jsonPrint(message)));
    return;
  }
}

function slackMessageHandler(message){
  return new Promise(function(resolve, reject){

    try {

      console.log(chalkInfo(MODULE_ID_PREFIX + " | MESSAGE | " + message.type + " | " + message.text));

      if (message.type !== "message") {
        console.log(chalkAlert("Unhandled MESSAGE TYPE: " + message.type));
        return resolve();
      }

      const text = message.text.trim();
      const textArray = text.split("|");

      const sourceMessage = (textArray[2]) ? textArray[2].trim() : "NONE";

      switch (sourceMessage) {
        case "END FETCH ALL":
        case "ERROR":
        case "FETCH FRIENDS":
        case "FSM INIT":
        case "FSM FETCH_ALL":
        case "GEN AUTO CAT":
        case "INIT CHILD":
        case "INIT LANG ANALYZER":
        case "INIT MAX INPUT HASHMAP":
        case "INIT NNs":
        case "INIT RAN NNs":
        case "INIT RNT CHILD":
        case "INIT TWITTER USERS":
        case "INIT TWITTER":
        case "INIT UNFOLLOWABLE USER SET":
        case "INIT UNFOLLOWABLE":
        case "INIT":
        case "LOAD BEST NN":
        case "LOAD NN":
        case "MONGO DB CONNECTED":
        case "PONG":
        case "QUIT":
        case "QUITTING":
        case "READY":
        case "RESET":
        case "SAV NN HASHMAP":
        case "SLACK QUIT":
        case "SLACK READY":
        case "SLACK RTM READY":
        case "START":
        case "STATS":
        case "TEXT": 
        case "UPDATE HISTOGRAMS":
        case "UPDATE NN STATS":
        case "WAIT UPDATE STATS":
        case "END UPDATE STATS":
        case "UPDATE USER CAT STATS":
          resolve();
        break;
        case "STATSUS":
          console.log(chalkInfo(message.text));
          resolve();
        break;
        case "PING":
          console.log(chalkInfo("PING"));
          // slackSendWebMessage(hostname + " | " + MODULE_ID_PREFIX + " | PONG");
          resolve();
        break;
        case "NONE":
          resolve();
        break;
        default:
          console.log(chalkAlert(MODULE_ID_PREFIX + " | *** UNDEFINED SLACK MESSAGE: " + message.text));
          // reject(new Error("UNDEFINED SLACK MESSAGE TYPE: " + message.text));
          resolve({text: "UNDEFINED SLACK MESSAGE", message: message});
      }
    }
    catch(err){
      reject(err);
    }

  });
}

async function initSlackWebClient(){
  try {

    const { WebClient } = require("@slack/client");
    slackWebClient = new WebClient(slackRtmToken);

    const conversationsListResponse = await slackWebClient.conversations.list({token: slackOAuthAccessToken});

    conversationsListResponse.channels.forEach(async function(channel){

      console.log(chalkLog(MODULE_ID_PREFIX + " | CHANNEL | " + channel.id + " | " + channel.name));

      if (channel.name === slackChannel) {
        configuration.slackChannel = channel;

        const message = {
          token: slackOAuthAccessToken, 
          channel: configuration.slackChannel.id,
          text: "OP"
        };

        message.attachments = [];
        message.attachments.push({
          text: "INIT", 
          fields: [ 
            { title: "SRC", value: hostname + "_" + process.pid }, 
            { title: "MOD", value: MODULE_NAME }, 
            { title: "DST", value: "ALL" } 
          ]
        });

        await slackWebClient.chat.postMessage(message);
      }

      channelsHashMap.set(channel.id, channel);

    });

    return;
  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX + " | *** INIT SLACK WEB CLIENT ERROR: " + err));
    throw err;
  }
}

async function initSlackRtmClient(){

  const { RTMClient } = require("@slack/client");
  slackRtmClient = new RTMClient(slackRtmToken);

  await slackRtmClient.start();

  slackRtmClient.on("slack_event", async function(eventType, event){
    switch (eventType) {
      case "pong":
        debug(chalkLog(MODULE_ID_PREFIX + " | SLACK RTM PONG | " + getTimeStamp() + " | " + event.reply_to));
      break;
      default: debug(chalkInfo(MODULE_ID_PREFIX + " | SLACK RTM EVENT | " + getTimeStamp() + " | " + eventType + "\n" + jsonPrint(event)));
    }
  });


  slackRtmClient.on("message", async function(message){
    if (configuration.verbose) { console.log(chalkLog(MODULE_ID_PREFIX + " | RTM R<\n" + jsonPrint(message))); }
    debug(`TNN | SLACK RTM MESSAGE | R< | CH: ${message.channel} | USER: ${message.user} | ${message.text}`);

    try {
      await slackMessageHandler(message);
    }
    catch(err){
      console.log(chalkError(MODULE_ID_PREFIX + " | *** SLACK RTM MESSAGE ERROR: " + err));
    }

  });

  slackRtmClient.on("ready", async function(){
    if (configuration.verbose) { await slackSendRtmMessage(hostname + " | TNN | SLACK RTM READY"); }
    return;
  });
}

const globalInputsObj = {};
globalInputsObj.inputsId = ""; // will be generated after number of inputs determined
globalInputsObj.meta = {};
globalInputsObj.meta.type = {};
globalInputsObj.meta.numInputs = 0;
globalInputsObj.meta.histogramParseTotalMin = INIT_TOT_MIN;
globalInputsObj.meta.histogramParseDominantMin = INIT_DOM_MIN;
globalInputsObj.inputs = {};

let stdin;

const defaultInputsFolder = path.join(configDefaultFolder, "inputs"); 
const localInputsFolder = path.join(configHostFolder, "inputs"); 

const defaultHistogramsFolder = path.join(configDefaultFolder, "histograms"); 

const statsFolder = path.join(DROPBOX_ROOT_FOLDER, "stats", hostname); 
const statsFile = configuration.DROPBOX.DROPBOX_STATS_FILE;

const statsObj = {};
statsObj.hostname = hostname;
statsObj.startTimeMoment = moment();
statsObj.pid = process.pid;
statsObj.userAuthenticated = false;
statsObj.serverConnected = false;
statsObj.heartbeatsReceived = 0;
statsObj.lessThanMin = 0;
statsObj.moreThanMin = 0;

const GIS_RUN_ID = hostname 
  + "_" + statsObj.startTimeMoment.format(compactDateTimeFormat)
  + "_" + process.pid;

statsObj.fetchUsersComplete = false;
statsObj.runId = GIS_RUN_ID;

statsObj.elapsed = 0;

statsObj.bestNetworks = {};
statsObj.totalInputs = 0;

statsObj.histograms = {};

statsObj.normalization = {};
statsObj.normalization.score = {};
statsObj.normalization.magnitude = {};

statsObj.normalization.score.min = 1.0;
statsObj.normalization.score.max = -1.0;
statsObj.normalization.magnitude.min = 0;
statsObj.normalization.magnitude.max = -Infinity;

statsObj.numLangAnalyzed = 0;

const histograms = {};

// inputTypes.forEach(function(type){
//   statsObj.histograms[type] = {};
//   histograms[type] = {};
// });

let statsUpdateInterval;

const jsonPrint = function (obj){
  if (obj) {
    return treeify.asTree(obj, true, true);
  }
  else {
    return "UNDEFINED";
  }
};

const cla = require("command-line-args");

const minInputsGenerated = { name: "minInputsGenerated", type: Number};
const maxInputsGenerated = { name: "maxInputsGenerated", type: Number};

const minDominantMin = { name: "minDominantMin", type: Number};
const maxDominantMin = { name: "maxDominantMin", type: Number};

const minTotalMin = { name: "minTotalMin", type: Number};

const enableStdin = { name: "enableStdin", alias: "i", type: Boolean, defaultValue: true};
const quitOnComplete = { name: "quitOnComplete", alias: "Q", type: Boolean, defaultValue: false};
const quitOnError = { name: "quitOnError", alias: "q", type: Boolean, defaultValue: true};
const testMode = { name: "testMode", alias: "X", type: Boolean, defaultValue: false};

const optionDefinitions = [
  minInputsGenerated,
  maxInputsGenerated,
  minTotalMin, 
  // maxTotalMin, 
  minDominantMin, 
  maxDominantMin, 
  enableStdin, 
  quitOnComplete, 
  quitOnError, 
  testMode
];

const commandLineConfig = cla(optionDefinitions);

console.log(chalkInfo("GIS | COMMAND LINE CONFIG\n" + jsonPrint(commandLineConfig)));

console.log("GIS | COMMAND LINE OPTIONS\n" + jsonPrint(commandLineConfig));


process.title = "node_generateInputSets";
console.log("\n\nGIS | =================================");
console.log("GIS | HOST:          " + hostname);
console.log("GIS | PROCESS TITLE: " + process.title);
console.log("GIS | PROCESS ID:    " + process.pid);
console.log("GIS | RUN ID:        " + statsObj.runId);
console.log("GIS | PROCESS ARGS   " + util.inspect(process.argv, {showHidden: false, depth: 1}));
console.log("GIS | =================================");

process.on("exit", function() {
});

process.on("message", function(msg) {

  if ((msg === "SIGINT") || (msg === "shutdown")) {

    debug("\n\n!!!!! RECEIVED PM2 SHUTDOWN !!!!!\n\n***** Closing all connections *****\n\n");

    clearInterval(statsUpdateInterval);

    setTimeout(function() {
      showStats();
      console.log("GIS | QUITTING generateInputSets");
      process.exit(0);
    }, 300);

  }
});

function showStats(){

  statsObj.elapsed = moment().diff(statsObj.startTimeMoment);
  statsObj.timeStamp = moment().format(compactDateTimeFormat);

  console.log(chalkLog("\nGIS | STATS"
    + " | E: " + tcUtils.msToTime(statsObj.elapsed)
    + " | S: " + statsObj.startTimeMoment.format(compactDateTimeFormat)
  ));
}

const inputsDefault = function (inputsObj){
  return inputsObj;
};

function printInputsObj(title, iObj) {

  const inputsObj = inputsDefault(iObj);

  console.log(chalkBlue(title
    + " | " + inputsObj.inputsId
    + "\n" + jsonPrint(inputsObj.meta)
  ));
}

function sortedHashmap(params) {

  return new Promise(function(resolve, reject) {

    const keys = Object.keys(params.hashmap);

    const sortedKeys = keys.sort(function(a,b){
      // const objA = params.hashmap.get(a);
      // const objB = params.hashmap.get(b);
      const objAvalue = dot.pick(params.sortKey, params.hashmap[a]);
      const objBvalue = dot.pick(params.sortKey, params.hashmap[b]);
      return objBvalue - objAvalue;
    });

    if (keys !== undefined) {
      if (sortedKeys !== undefined) { 
        resolve({sortKey: params.sortKey, sortedKeys: sortedKeys.slice(0,params.max)});
      }
      else {
        console.log(chalkAlert("sortedHashmap NO SORTED KEYS? | SORT KEY: " + params.sortKey 
          + " | KEYS: " + keys.length 
          + " | SORTED KEYS: " + sortedKeys.length
        ));
        resolve({sortKey: params.sortKey, sortedKeys: []});
      }

    }
    else {
      console.error("sortedHashmap ERROR | params\n" + jsonPrint(params));
      reject(new Error("sortedHashmap ERROR | keys UNDEFINED"));
    }

  });
}

function generateInputSets(params) {

  return new Promise(function(resolve, reject){

    const newInputsObj = {};
    newInputsObj.inputsId = hostname + "_" + process.pid + "_" + moment().format(compactDateTimeFormat);
    newInputsObj.meta = {};
    newInputsObj.meta.type = {};
    newInputsObj.meta.numInputs = 0;
    newInputsObj.inputs = {};
    newInputsObj.inputsMinimum = {};

    const inTypes = Object.keys(params.histogramsObj.histograms);
    inTypes.sort();

    async.eachSeries(inTypes, async function(type){

      let minDominantMin = (params.minDominantMin && (params.minDominantMin[type] === undefined)) ? params.minDominantMin : configuration.minDominantMin;
      
      minDominantMin = (params.minDominantMin && params.minDominantMin[type]) ? params.minDominantMin[type] : minDominantMin;

      const totalTypeInputs = Object.keys(params.histogramsObj.histograms[type]).length;

      console.log("TYPE | " + type.toUpperCase() + " | " + totalTypeInputs);

      newInputsObj.meta.type[type] = {};

      // start with zero inputs of type if more than configuration.minNumInputsPerType
      newInputsObj.meta.type[type].numInputs = (totalTypeInputs > configuration.minNumInputsPerType) ? 0 : totalTypeInputs;
      newInputsObj.meta.type[type].underMinNumInputs = null;
      newInputsObj.meta.type[type].overMaxNumInputs = null;
      newInputsObj.meta.type[type].currentMaxNumInputs = 0;

      newInputsObj.inputs[type] = [];
      newInputsObj.inputsMinimum[type] = [];

      if (totalTypeInputs === 0) {
        return;
      }

      let results = {};

      try {
        results = await sortedHashmap({ sortKey: "total", hashmap: params.histogramsObj.histograms[type], max: 10000});

        async.eachSeries(results.sortedKeys, function(input, cb){

          // "total": 2,
          // "left": 2,
          // "leftRatio": 1,
          // "neutral": 0,
          // "neutralRatio": 0,
          // "right": 0,
          // "rightRatio": 0,
          // "positive": 0,
          // "positiveRatio": 0,
          // "negative": 0,
          // "negativeRatio": 0,
          // "none": 0,
          // "uncategorized": 0

          if (
               (params.histogramsObj.histograms[type][input].leftRatio >= minDominantMin)
            || (params.histogramsObj.histograms[type][input].neutralRatio >= minDominantMin)
            || (params.histogramsObj.histograms[type][input].rightRatio >= minDominantMin)
            ) 
          {
            newInputsObj.inputs[type].push(input);
            if (newInputsObj.inputs[type].length >= configuration.maxNumInputsPerType) {
              console.log(chalkBlue("GIS | " + type.toUpperCase() + " | MAX INPUTS REACHED " + newInputsObj.inputs[type].length));
              return cb("MAX");
            }
          }

          cb();

        }, function(err){
          if (err && (err !== "MAX")){
            console.log(chalkError("GIS | *** SORTED HASHMAP ERROR:", err));
            return err;
          }
          newInputsObj.inputs[type].sort();
          newInputsObj.meta.type[type].numInputs = newInputsObj.inputs[type].length;
          console.log(chalkBlue("GIS | " + type.toUpperCase() + " | " + newInputsObj.meta.type[type].numInputs + " INPUTS"));
          return;
        });

      }
      catch(err){
        console.log(chalkError("GIS | *** SORTED HASHMAP ERROR:", err));
        return err;
      }


    }, function(err){
      if (err) { 
        console.log("GIS | ERROR:", err);
        return reject(err); 
      }
      resolve(newInputsObj);
    });

  });
}

let quitWaitInterval;

function quit(cause){

  console.log( "\nGIS | ... QUITTING ..." );

  if (cause) {
    console.log( "GIS | CAUSE: " + cause );
  }

  quitWaitInterval = setInterval(function () {

    if (cause === "Q"){
      statsObj.elapsed = moment().diff(statsObj.startTimeMoment);

      clearInterval(statsUpdateInterval);
      clearInterval(quitWaitInterval);

      console.log(chalkAlert("\nGIS | *** FORCE QUITTING"));

      setTimeout(function(){
        process.exit();      
      }, 1000);
    }
    else {

      statsObj.elapsed = moment().diff(statsObj.startTimeMoment);

      clearInterval(statsUpdateInterval);
      clearInterval(quitWaitInterval);

      console.log(chalkAlert("\nGIS | ALL PROCESSES COMPLETE ... QUITTING"));

      setTimeout(function(){
        process.exit();      
      }, 1000);

    }

  }, 1000);
}

process.on( "SIGINT", function() {
  quit("SIGINT");
});

function getTimeStamp(inputTime) {
  let currentTimeStamp;

  if (inputTime === undefined) {
    currentTimeStamp = moment().format(compactDateTimeFormat);
    return currentTimeStamp;
  }
  else if (moment.isMoment(inputTime)) {
    currentTimeStamp = moment(inputTime).format(compactDateTimeFormat);
    return currentTimeStamp;
  }
  else if (moment.isDate(new Date(inputTime))) {
    currentTimeStamp = moment(new Date(inputTime)).format(compactDateTimeFormat);
    return currentTimeStamp;
  }
  else {
    currentTimeStamp = moment(parseInt(inputTime)).format(compactDateTimeFormat);
    return currentTimeStamp;
  }
}

async function connectDb(){

  try {

    statsObj.status = "CONNECTING MONGO DB";

    console.log(chalkBlueBold(MODULE_ID_PREFIX + " | CONNECT MONGO DB ..."));

    const db = await global.wordAssoDb.connect(MODULE_ID_PREFIX + "_" + process.pid);

    db.on("error", async function(err){
      statsObj.status = "MONGO ERROR";
      statsObj.dbConnectionReady = false;
      console.log(chalkError(MODULE_ID_PREFIX + " | *** MONGO DB CONNECTION ERROR"));
      db.close();
      quit({cause: "MONGO DB ERROR: " + err});
    });

    db.on("close", async function(err){
      statsObj.status = "MONGO CLOSED";
      statsObj.dbConnectionReady = false;
      console.log(chalkError(MODULE_ID_PREFIX + " | *** MONGO DB CONNECTION CLOSED"));
      quit({cause: "MONGO DB CLOSED: " + err});
    });

    db.on("disconnected", async function(){
      statsObj.status = "MONGO DISCONNECTED";
      statsObj.dbConnectionReady = false;
      console.log(chalkAlert(MODULE_ID_PREFIX + " | *** MONGO DB DISCONNECTED"));
      quit({cause: "MONGO DB DISCONNECTED"});
    });

    console.log(chalk.green(MODULE_ID_PREFIX + " | MONGOOSE DEFAULT CONNECTION OPEN"));

    statsObj.dbConnectionReady = true;

    return db;
  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX + " | *** MONGO DB CONNECT ERROR: " + err));
    throw err;
  }
}

function getElapsedTimeStamp(){
  statsObj.elapsedMS = moment().valueOf() - statsObj.startTimeMoment.valueOf();
  return tcUtils.msToTime(statsObj.elapsedMS);
}

function initStatsUpdate() {

  return new Promise(function(resolve){

    console.log(chalkLog(MODULE_ID_PREFIX + " | INIT STATS UPDATE INTERVAL | " + tcUtils.msToTime(configuration.statsUpdateIntervalTime)));

    statsObj.elapsed = getElapsedTimeStamp();
    statsObj.timeStamp = getTimeStamp();

    clearInterval(statsUpdateInterval);

    statsUpdateInterval = setInterval(async function () {

      statsObj.elapsed = getElapsedTimeStamp();
      statsObj.timeStamp = getTimeStamp();

      try{
        await tcUtils.saveFile({folder: statsFolder, file: statsFile, obj: statsObj});
        await showStats();
      }
      catch(err){
        console.log(chalkError(MODULE_ID_PREFIX + " | *** SHOW STATS ERROR: " + err));
      }
      
    }, configuration.statsUpdateIntervalTime);

    resolve();

  });
}

function initStdIn(){

  console.log("STDIN ENABLED");

  stdin = process.stdin;
  if(stdin.setRawMode !== undefined) {
    stdin.setRawMode( true );
  }
  stdin.resume();
  stdin.setEncoding( "utf8" );
  stdin.on( "data", function( key ){

    switch (key) {
      case "\u0003":
        process.exit();
      break;
      case "q":
      case "Q":
        quit(key);
      break;
      case "s":
        showStats();
      break;
      case "S":
        showStats(true);
      break;
      default:
        console.log(
          "\n" + "q/Q: quit"
          + "\n" + "s: showStats"
          + "\n" + "S: showStats verbose"
          );
    }
  });
}

function loadCommandLineArgs(){

  return new Promise(function(resolve){

    statsObj.status = "LOAD COMMAND LINE ARGS";

    const commandLineConfigKeys = Object.keys(commandLineConfig);

    async.each(commandLineConfigKeys, function(arg, cb){

      if (arg === "evolveIterations"){
        configuration.evolve.iterations = commandLineConfig[arg];
        console.log(MODULE_ID_PREFIX + " | --> COMMAND LINE CONFIG | " + arg + ": " + configuration.evolve.iterations);
      }
      else {
        configuration[arg] = commandLineConfig[arg];
        console.log(MODULE_ID_PREFIX + " | --> COMMAND LINE CONFIG | " + arg + ": " + configuration[arg]);
      }

      cb();

    }, function(){
      statsObj.commandLineArgsLoaded = true;
      resolve();
    });

  });
}

async function loadInputs(params) {

  statsObj.status = "LOAD INPUTS CONFIG";

  const folder = params.folder;
  const file = params.file;

  console.log(chalkLog("TFE | LOADING INPUTS CONFIG | " + folder + "/" + file + " ..."));

  try {

    const inputsConfigObj = await tcUtils.loadFile({folder: folder, file: file, noErrorNotFound: params.noErrorNotFound});

    if (!inputsConfigObj) {
      if (params.noErrorNotFound) {
        console.log(chalkAlert("TFE | !!! LOAD INPUTS CONFIG FILE ERROR | FILE NOT FOUND "));
        return;
      }
      console.log(chalkError("TFE | LOAD INPUTS CONFIG FILE ERROR | JSON UNDEFINED ??? "));
      throw new Error("LOAD INPUTS CONFIG FILE ERROR | JSON UNDEFINED");
    }

    const tempInputsIdSet = new Set(inputsConfigObj.INPUTS_IDS);

    for (const inputsId of tempInputsIdSet) {
      inputsIdSet.add(inputsId);
    }

    console.log(chalkBlue("TFE | LOADED INPUTS CONFIG"
      + "\nTFE | CURRENT FILE INPUTS IDS SET: " + tempInputsIdSet.size + " INPUTS IDS"
      + "\n" + jsonPrint([...tempInputsIdSet])
      + "\nTFE | FINAL INPUTS IDS SET: " + inputsIdSet.size + " INPUTS IDS"
      + "\n" + jsonPrint([...inputsIdSet])
    ));

    return;
  }
  catch(err){
    if ((err.status == 409) || (err.status == 404)) {
      console.log(chalkError("TFE | LOAD INPUTS CONFIG FILE NOT FOUND"));
      return;
    }
    console.log(chalkError("TFE | LOAD INPUTS CONFIG FILE ERROR: ", err));
    throw err;
  }
}

async function loadConfigFile(params) {

  const fullPath = path.join(params.folder, params.file);

  try {

    if (configuration.offlineMode) {
      await loadCommandLineArgs();
      return;
    }

    const newConfiguration = {};
    newConfiguration.evolve = {};

    const loadedConfigObj = await tcUtils.loadFile({folder: params.folder, file: params.file, noErrorNotFound: params.noErrorNotFound });

    if (loadedConfigObj === undefined) {
      if (params.noErrorNotFound) {
        console.log(chalkAlert(MODULE_ID_PREFIX + " | ... SKIP LOAD CONFIG FILE: " + params.folder + "/" + params.file));
        return newConfiguration;
      }
      else {
        console.log(chalkError(MODULE_ID_PREFIX + " | *** CONFIG LOAD FILE ERROR | JSON UNDEFINED ??? "));
        throw new Error("JSON UNDEFINED");
      }
    }

    if (loadedConfigObj instanceof Error) {
      console.log(chalkError(MODULE_ID_PREFIX + " | *** CONFIG LOAD FILE ERROR: " + loadedConfigObj));
    }

    console.log(chalkInfo(MODULE_ID_PREFIX + " | LOADED CONFIG FILE: " + params.file + "\n" + jsonPrint(loadedConfigObj)));

      if (loadedConfigObj.GIS_GENERATE_BOTH_USER_PROFILE_ONLY_AND_ALL_HISTOGRAMS_INPUTS !== undefined) {
        console.log(MODULE_ID_PREFIX + " | LOADED GIS_GENERATE_BOTH_USER_PROFILE_ONLY_AND_ALL_HISTOGRAMS_INPUTS: " + loadedConfigObj.GIS_GENERATE_BOTH_USER_PROFILE_ONLY_AND_ALL_HISTOGRAMS_INPUTS);
        if ((loadedConfigObj.GIS_GENERATE_BOTH_USER_PROFILE_ONLY_AND_ALL_HISTOGRAMS_INPUTS === true) || (loadedConfigObj.GIS_GENERATE_BOTH_USER_PROFILE_ONLY_AND_ALL_HISTOGRAMS_INPUTS === "true")) {
          newConfiguration.generateBothUserProfileOnlyAndAllHistogramsInputs = true;
          // GIS_USER_PROFILE_ONLY_FLAG and userProfileOnlyFlag will be ignored
        }
        if ((loadedConfigObj.GIS_GENERATE_BOTH_USER_PROFILE_ONLY_AND_ALL_HISTOGRAMS_INPUTS === false) || (loadedConfigObj.GIS_GENERATE_BOTH_USER_PROFILE_ONLY_AND_ALL_HISTOGRAMS_INPUTS === "false")) {
          newConfiguration.generateBothUserProfileOnlyAndAllHistogramsInputs = false;
        }
      }

      if (loadedConfigObj.GIS_USER_PROFILE_ONLY_FLAG !== undefined) {
        console.log(MODULE_ID_PREFIX + " | LOADED GIS_USER_PROFILE_ONLY_FLAG: " + loadedConfigObj.GIS_USER_PROFILE_ONLY_FLAG);
        if ((loadedConfigObj.GIS_USER_PROFILE_ONLY_FLAG === true) || (loadedConfigObj.GIS_USER_PROFILE_ONLY_FLAG === "true")) {
          newConfiguration.userProfileOnlyFlag = true;
        }
        if ((loadedConfigObj.GIS_USER_PROFILE_ONLY_FLAG === false) || (loadedConfigObj.GIS_USER_PROFILE_ONLY_FLAG === "false")) {
          newConfiguration.userProfileOnlyFlag = false;
        }
      }

      if (loadedConfigObj.GIS_MAX_NUM_INPUTS_PER_TYPE !== undefined){
        console.log("GIS | LOADED GIS_MAX_NUM_INPUTS_PER_TYPE: " + loadedConfigObj.GIS_MAX_NUM_INPUTS_PER_TYPE);
        newConfiguration.maxNumInputsPerType = loadedConfigObj.GIS_MAX_NUM_INPUTS_PER_TYPE;
      }

      if (loadedConfigObj.GIS_MIN_NUM_INPUTS_PER_TYPE !== undefined){
        console.log("GIS | LOADED GIS_MIN_NUM_INPUTS_PER_TYPE: " + loadedConfigObj.GIS_MIN_NUM_INPUTS_PER_TYPE);
        newConfiguration.minNumInputsPerType = loadedConfigObj.GIS_MIN_NUM_INPUTS_PER_TYPE;
      }

      if (loadedConfigObj.GIS_INPUTS_FILE_PREFIX !== undefined){
        console.log("GIS | LOADED GIS_INPUTS_FILE_PREFIX: " + loadedConfigObj.GIS_INPUTS_FILE_PREFIX);
        newConfiguration.inputsFilePrefix = loadedConfigObj.GIS_INPUTS_FILE_PREFIX;
      }

      if (loadedConfigObj.GIS_MAX_ITERATIONS !== undefined){
        console.log("GIS | LOADED GIS_MAX_ITERATIONS: " + loadedConfigObj.GIS_MAX_ITERATIONS);
        newConfiguration.maxIterations = loadedConfigObj.GIS_MAX_ITERATIONS;
      }

      if (loadedConfigObj.GIS_MIN_INPUTS_GENERATED !== undefined){
        console.log("GIS | LOADED GIS_MIN_INPUTS_GENERATED: " + loadedConfigObj.GIS_MIN_INPUTS_GENERATED);
        newConfiguration.minInputsGenerated = loadedConfigObj.GIS_MIN_INPUTS_GENERATED;
      }

      if (loadedConfigObj.GIS_MAX_INPUTS_GENERATED !== undefined){
        console.log("GIS | LOADED GIS_MAX_INPUTS_GENERATED: " + loadedConfigObj.GIS_MAX_INPUTS_GENERATED);
        newConfiguration.maxInputsGenerated = loadedConfigObj.GIS_MAX_INPUTS_GENERATED;
      }

      if (loadedConfigObj.GIS_MIN_TOTAL_MIN !== undefined){
        console.log("GIS | LOADED GIS_MIN_TOTAL_MIN: " + loadedConfigObj.GIS_MIN_TOTAL_MIN);
        newConfiguration.minTotalMin = loadedConfigObj.GIS_MIN_TOTAL_MIN;
      }

      if (loadedConfigObj.GIS_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP !== undefined){
        console.log("GIS | LOADED GIS_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP: " + loadedConfigObj.GIS_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP);
        newConfiguration.minTotalMinUserProfileHashMap = loadedConfigObj.GIS_MIN_TOTAL_MIN_USER_PROFILE_TYPE_HASHMAP;
      }

      if (loadedConfigObj.GIS_MIN_TOTAL_MIN_TYPE_HASHMAP !== undefined){
        console.log("GIS | LOADED GIS_MIN_TOTAL_MIN_TYPE_HASHMAP: " + loadedConfigObj.GIS_MIN_TOTAL_MIN_TYPE_HASHMAP);
        newConfiguration.minTotalMinHashMap = loadedConfigObj.GIS_MIN_TOTAL_MIN_TYPE_HASHMAP;
      }

      // if (loadedConfigObj.GIS_MAX_TOTAL_MIN !== undefined){
      //   console.log("GIS | LOADED GIS_MAX_TOTAL_MIN: " + loadedConfigObj.GIS_MAX_TOTAL_MIN);
      //   newConfiguration.maxTotalMin = loadedConfigObj.GIS_MAX_TOTAL_MIN;
      // }

      if (loadedConfigObj.GIS_MIN_DOMINANT_MIN !== undefined){
        console.log("LOADED GIS_MIN_DOMINANT_MIN: " + loadedConfigObj.GIS_MIN_DOMINANT_MIN);
        newConfiguration.minDominantMin = loadedConfigObj.GIS_MIN_DOMINANT_MIN;
      }

      if (loadedConfigObj.GIS_MAX_DOMINANT_MIN !== undefined){
        console.log("GIS | LOADED GIS_MAX_DOMINANT_MIN: " + loadedConfigObj.GIS_MAX_DOMINANT_MIN);
        newConfiguration.maxDominantMin = loadedConfigObj.GIS_MAX_DOMINANT_MIN;
      }

      if (loadedConfigObj.GIS_TEST_MODE !== undefined){
        console.log("GIS | LOADED GIS_TEST_MODE: " + loadedConfigObj.GIS_TEST_MODE);
        newConfiguration.testMode = loadedConfigObj.GIS_TEST_MODE;
      }

      if (loadedConfigObj.GIS_QUIT_ON_COMPLETE !== undefined){
        console.log("GIS | LOADED GIS_QUIT_ON_COMPLETE: " + loadedConfigObj.GIS_QUIT_ON_COMPLETE);
        newConfiguration.quitOnComplete = loadedConfigObj.GIS_QUIT_ON_COMPLETE;
      }

      if (loadedConfigObj.GIS_HISTOGRAM_PARSE_DOMINANT_MIN !== undefined){
        console.log("GIS | LOADED GIS_HISTOGRAM_PARSE_DOMINANT_MIN: " + loadedConfigObj.GIS_HISTOGRAM_PARSE_DOMINANT_MIN);
        newConfiguration.histogramParseDominantMin = loadedConfigObj.GIS_HISTOGRAM_PARSE_DOMINANT_MIN;
      }

      if (loadedConfigObj.GIS_HISTOGRAM_PARSE_TOTAL_MIN !== undefined){
        console.log("GIS | LOADED GIS_HISTOGRAM_PARSE_TOTAL_MIN: " + loadedConfigObj.GIS_HISTOGRAM_PARSE_TOTAL_MIN);
        newConfiguration.histogramParseTotalMin = loadedConfigObj.GIS_HISTOGRAM_PARSE_TOTAL_MIN;
      }

      if (loadedConfigObj.GIS_ENABLE_STDIN !== undefined){
        console.log("GIS | LOADED GIS_ENABLE_STDIN: " + loadedConfigObj.GIS_ENABLE_STDIN);
        newConfiguration.enableStdin = loadedConfigObj.GIS_ENABLE_STDIN;
      }

      if (loadedConfigObj.GIS_KEEPALIVE_INTERVAL !== undefined) {
        console.log("GIS | LOADED GIS_KEEPALIVE_INTERVAL: " + loadedConfigObj.GIS_KEEPALIVE_INTERVAL);
        newConfiguration.keepaliveInterval = loadedConfigObj.GIS_KEEPALIVE_INTERVAL;
      }

    return newConfiguration;
  }
  catch(err){
    console.error(chalkError(MODULE_ID_PREFIX + " | ERROR LOAD CONFIG: " + fullPath
      + "\n" + jsonPrint(err)
    ));
    throw err;
  }
}

async function loadAllConfigFiles(){

  statsObj.status = "LOAD CONFIG";

  const defaultConfig = await loadConfigFile({folder: configDefaultFolder, file: configDefaultFile});

  if (defaultConfig) {
    defaultConfiguration = defaultConfig;
    console.log(chalkInfo(MODULE_ID_PREFIX + " | <<< LOADED DEFAULT CONFIG " + configDefaultFolder + "/" + configDefaultFile));
  }
  
  const hostConfig = await loadConfigFile({folder: configHostFolder, file: configHostFile, noErrorNotFound: true});

  if (hostConfig) {
    hostConfiguration = hostConfig;
    console.log(chalkInfo(MODULE_ID_PREFIX + " | <<< LOADED HOST CONFIG " + configHostFolder + "/" + configHostFile));
  }

  await loadInputs({folder: configDefaultFolder, file: defaultInputsConfigFile, noErrorNotFound: false});
  
  const defaultAndHostConfig = merge(defaultConfiguration, hostConfiguration); // host settings override defaults
  const tempConfig = merge(configuration, defaultAndHostConfig); // any new settings override existing config

  configuration = deepcopy(tempConfig);

  configuration.twitterUsers = _.uniq(configuration.twitterUsers);

  return;
}

async function initConfig(cnf) {

  statsObj.status = "INIT CONFIG";

  console.log(chalkBlue(MODULE_ID_PREFIX + " | INIT CONFIG"));

  if (debug.enabled) {
    console.log("\nGIS | %%%%%%%%%%%%%%\nGIS |  DEBUG ENABLED \nGIS | %%%%%%%%%%%%%%\n");
  }

  cnf.processName = process.env.PROCESS_NAME || MODULE_ID;
  cnf.testMode = (process.env.TEST_MODE === "true") ? true : cnf.testMode;
  cnf.quitOnError = process.env.QUIT_ON_ERROR || false;
  cnf.enableStdin = process.env.ENABLE_STDIN || true;

  if (process.env.QUIT_ON_COMPLETE === "false") { cnf.quitOnComplete = false; }
  else if ((process.env.QUIT_ON_COMPLETE === true) || (process.env.QUIT_ON_COMPLETE === "true")) {
    cnf.quitOnComplete = true;
  }

  try {

    await loadAllConfigFiles();
    await loadCommandLineArgs();

    const configArgs = Object.keys(configuration);

    configArgs.forEach(function(arg){
      if (_.isObject(configuration[arg])) {
        console.log(MODULE_ID_PREFIX + " | _FINAL CONFIG | " + arg + "\n" + jsonPrint(configuration[arg]));
      }
      else {
        console.log(MODULE_ID_PREFIX + " | _FINAL CONFIG | " + arg + ": " + configuration[arg]);
      }
    });
    
    statsObj.commandLineArgsLoaded = true;

    if (configuration.enableStdin) { initStdIn(); }

    await initStatsUpdate();

    return configuration;

  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX + " | *** CONFIG LOAD ERROR: " + err ));
    throw err;
  }
}

function loadStream(params){

  return new Promise(function(resolve, reject){

    const streamPath = path.join(params.folder, params.file);
    const userProfileOnlyFlag = params.userProfileOnlyFlag || configuration.userProfileOnlyFlag;
    const minTotalMin = params.minTotalMin || configuration.minTotalMin;

    console.log(chalkInfo("GIS | LOAD STREAM"
      + " | USER PROFILE ONLY: " + userProfileOnlyFlag
      + " | MIN TOT MIN: " + minTotalMin
      + " | PATH: " + streamPath
    ));

    const pathExists = fs.existsSync(streamPath);

    if (!pathExists) {
      console.log(chalkAlert("GIS | !!! STREAM PATH DOES NOT EXIST ... SKIPPING LOAD: " + streamPath));
      return resolve();
      // return reject(new Error("PATH DOES NOT EXIST: " + streamPath));
    }

    const fileObj = {};

    let totalInputs = 0;
    let lessThanMin = 0;
    let moreThanMin = 0;

    let totalCategorized = 0;
    let maxTotalCategorized = 0;

    let pipeline;

    if (userProfileOnlyFlag){
      pipeline = fs.createReadStream(streamPath).pipe(JSONStream.parse("profileHistograms.$*.$*"));
    }
    else{
      pipeline = fs.createReadStream(streamPath).pipe(JSONStream.parse("histograms.$*.$*"));
    }

    pipeline.on("data", function(obj){

      totalInputs += 1;

      // VALUE
      // ├─ total: 16
      // ├─ left: 2
      // ├─ neutral: 0
      // ├─ right: 3
      // ├─ positive: 0
      // ├─ negative: 0
      // ├─ none: 0
      // └─ uncategorized: 11


      totalCategorized = obj.value.total - obj.value.uncategorized;
      maxTotalCategorized = Math.max(maxTotalCategorized, totalCategorized);

      if (totalCategorized >= minTotalMin) {

        moreThanMin += 1;

        debug(chalkLog("GIS | +++ INPUT | PROFILE ONLY: " + userProfileOnlyFlag
          + " [" + moreThanMin + "]"
          + " | " + params.type
          + " | " + obj.key
          + " | TOT CAT: " + totalCategorized 
          + " | MAX TOT CAT: " + maxTotalCategorized 
          // + "\nVALUE\n" + jsonPrint(obj.value)
        ));

        fileObj[obj.key] = obj.value;
      }
      else {
        lessThanMin += 1;
      }

      debug("data: " + jsonPrint(obj));

      if (totalInputs % 50000 === 0) {
        console.log(chalkLog("GIS | STREAM INPUTS | " + streamPath + " | INPUTS: " + totalInputs));
      }

      if (configuration.testMode && (totalInputs >= MAX_TEST_INPUTS) && (totalInputs % 100 === 0)) {
        console.log(chalkAlert("GIS | TEST MODE | END READ | TOTAL TEST INPUTS: " + totalInputs + " MAX: " + MAX_TEST_INPUTS));
        pipeline.destroy();
      }
    });

    pipeline.on("header", function(){
      console.log("GIS | HEADER");
    });

    pipeline.on("footer", function(){
      console.log("GIS | FOOTER");
    });

    pipeline.on("close", function(){
      if (configuration.verbose) { console.log(chalkInfo("GIS | STREAM CLOSED | INPUTS: " + totalInputs + " | " + streamPath)); }
      return resolve({ 
        obj: fileObj, 
        maxTotalCategorized: maxTotalCategorized, 
        totalInputs: totalInputs, 
        lessThanMin: lessThanMin, 
        moreThanMin: moreThanMin
      });
    });

    pipeline.on("end", function(){
      if (configuration.verbose) { console.log(chalkInfo("GIS | STREAM END | INPUTS: " + totalInputs + " | " + streamPath)); }
      return resolve({ 
        obj: fileObj, 
        maxTotalCategorized: maxTotalCategorized, 
        totalInputs: totalInputs, 
        lessThanMin: lessThanMin, 
        moreThanMin: moreThanMin
      });
    });

    pipeline.on("finish", function(){
      if (configuration.verbose) { console.log(chalkInfo("GIS | STREAM FINISH | INPUTS: " + totalInputs + " | " + streamPath)); }
      return resolve({ 
        obj: fileObj, 
        maxTotalCategorized: maxTotalCategorized, 
        totalInputs: totalInputs, 
        lessThanMin: lessThanMin, 
        moreThanMin: moreThanMin
      });
    });

    pipeline.on("error", function(err){
      console.log(chalkError("GIS | STREAM ERROR | INPUTS: " + totalInputs + " | " + streamPath));
      console.log(chalkError("GIS | *** LOAD FILE ERROR: " + err));
      return reject(err);
    });

  });
}

function runMain(){

  return new Promise(function(resolve, reject){

    statsObj.status = "RUN MAIN";

    console.log(chalkLog("GIS | USER PROFILE HISTOGRAM ONLY: " + configuration.userProfileOnlyFlag));

    console.log(chalkInfo(
        "\n--------------------------------------------------------"
      + "\n" + MODULE_ID_PREFIX + " | USER PROFILE HISTOGRAM ONLY: " + configuration.userProfileOnlyFlag
      + "\n--------------------------------------------------------"
    ));

    const genInParams = {};

    genInParams.minTotalMin = {};
    genInParams.minTotalMin = configuration.userProfileOnlyFlag ? configuration.minTotalMinUserProfileHashMap : configuration.minTotalMinHashMap;

    genInParams.minDominantMin = {};
    genInParams.minDominantMin.emoji = configuration.minDominantMin;
    genInParams.minDominantMin.friends = configuration.minDominantMin;
    genInParams.minDominantMin.hashtags = configuration.minDominantMin;
    genInParams.minDominantMin.images = configuration.minDominantMin;
    genInParams.minDominantMin.locations = configuration.minDominantMin;
    genInParams.minDominantMin.media = configuration.minDominantMin;
    genInParams.minDominantMin.ngrams = configuration.minDominantMin;
    genInParams.minDominantMin.places = configuration.minDominantMin;
    genInParams.minDominantMin.sentiment = configuration.minDominantMin;
    genInParams.minDominantMin.urls = configuration.minDominantMin;
    genInParams.minDominantMin.userMentions = configuration.minDominantMin;
    genInParams.minDominantMin.words = configuration.minDominantMin;

    genInParams.histogramsObj = {};
    genInParams.histogramsObj.histograms = {};
    // genInParams.histogramsObj.maxTotalMin = {};
    genInParams.histogramsObj.histogramParseDominantMin = configuration.histogramParseDominantMin;
    genInParams.histogramsObj.histogramParseTotalMin = configuration.histogramParseTotalMin;

    const inputTypes = (configuration.userProfileOnlyFlag) ? USER_PROFILE_INPUT_TYPES : DEFAULT_INPUT_TYPES;

    inputTypes.forEach(function(type){
      statsObj.histograms[type] = {};
      histograms[type] = {};
    });

    async.eachSeries(inputTypes, function(type, cb){

      if (type === "sentiment"){

        // │  │  ├─ numInputs: 9
        // │  │  ├─ underMinNumInputs
        // │  │  ├─ overMaxNumInputs
        // │  │  ├─ currentMaxNumInputs: 0
        // │  │  ├─ totalInputs: 26
        // │  │  ├─ minDominantMin: 0.7125
        // │  │  ├─ minTotalMin: 10
        // │  │  ├─ lessThanMin: 0
        // │  │  ├─ moreThanMin: 26
        // │  │  └─ totalMin: 0

        globalInputsObj.inputs[type] = ["comp", "magnitude", "score"];
        // globalInputsObj.inputs[type] = inputsObj.inputs[type];
        globalInputsObj.meta.type[type] = {};
        // globalInputsObj.meta.type[type] = inputsObj.meta.type[type];
        globalInputsObj.meta.type[type].numInputs = 3;
        globalInputsObj.meta.type[type].underMinNumInputs = 0;
        globalInputsObj.meta.type[type].overMaxNumInputs = 0;
        globalInputsObj.meta.type[type].currentMaxNumInputs = 0;
        globalInputsObj.meta.type[type].totalInputs = 3;
        globalInputsObj.meta.type[type].minDominantMin = genInParams.minDominantMin[type];
        globalInputsObj.meta.type[type].minTotalMin = 1;
        globalInputsObj.meta.type[type].lessThanMin = 0;
        globalInputsObj.meta.type[type].moreThanMin = 3;
        globalInputsObj.meta.type[type].totalMin = 1;
        cb();
      }
      else{
        const folder = defaultHistogramsFolder + "/types/" + type;
        const file = "histograms_" + type + ".json";

        const minTotalMin = (genInParams.minTotalMin[type] && (genInParams.minTotalMin[type] !== undefined)) ? genInParams.minTotalMin[type] : configuration.minTotalMin[type];

        loadStream({folder: folder, file: file, minTotalMin: minTotalMin})
        .then(function(results){

          if (!results) {
            return cb();
          }

          console.log(chalkBlue("\nGIS | +++ LOADED HISTOGRAM | " + type.toUpperCase()
            + "\nGIS | TOTAL ITEMS:          " + results.totalInputs
            + "\nGIS | MAX TOT CAT:          " + results.maxTotalCategorized
            + "\nGIS | MIN TOTAL MIN:        " + minTotalMin
            + "\nGIS | MORE THAN TOTAL MIN:  " + results.moreThanMin + " (" + (100*results.moreThanMin/results.totalInputs).toFixed(2) + "%)"
            + "\nGIS | LESS THAN TOTAL MIN:  " + results.lessThanMin + " (" + (100*results.lessThanMin/results.totalInputs).toFixed(2) + "%)"
          ));

          genInParams.histogramsObj.histograms[type] = {};
          genInParams.histogramsObj.histograms[type] = results.obj;

          generateInputSets(genInParams)
          .then(function(inputsObj){
            globalInputsObj.inputs[type] = {};
            globalInputsObj.inputs[type] = inputsObj.inputs[type];
            globalInputsObj.meta.type[type] = {};
            globalInputsObj.meta.type[type] = inputsObj.meta.type[type];
            globalInputsObj.meta.type[type].totalInputs = results.totalInputs;
            globalInputsObj.meta.type[type].minDominantMin = genInParams.minDominantMin[type];
            globalInputsObj.meta.type[type].minTotalMin = minTotalMin;
            globalInputsObj.meta.type[type].lessThanMin = results.lessThanMin;
            globalInputsObj.meta.type[type].moreThanMin = results.moreThanMin;
            globalInputsObj.meta.type[type].totalMin = globalInputsObj.meta.type[type].totalMin || 0;
            cb();
          })
          .catch(function(err){
            return cb(err);
          })
        })
        .catch(function(err){
          console.log(chalkError("GIS | LOAD HISTOGRAMS / GENERATE INPUT SETS ERROR: " + err));
          cb(err);
        });
      }

    }, function(err){

      if (err) {
        return reject(err);
      }

      let inFolder = (hostname === PRIMARY_HOST) ? defaultInputsFolder : localInputsFolder;

      if (configuration.testMode) { 
        inFolder += "_test";
      }

      const tableArray = [];

      tableArray.push([
        "GIS |",
        "TYPE",
        "DOM MIN",
        "TOT IN",
        "TOT MIN",
        "IN",
        "GTOT IN"
      ]);

      globalInputsObj.meta.userProfileOnlyFlag = configuration.userProfileOnlyFlag || false;
      globalInputsObj.meta.numInputs = 0;

      async.eachSeries(inputTypes, function(type, cb){

        if (globalInputsObj.meta.type[type] === undefined) { return cb(); }

        globalInputsObj.meta.numInputs += globalInputsObj.meta.type[type].numInputs;

        globalInputsObj.meta.type[type].minDominantMin = globalInputsObj.meta.type[type].minDominantMin || 0;
        globalInputsObj.meta.type[type].minTotalMin = globalInputsObj.meta.type[type].minTotalMin || 0;

        tableArray.push([
          "GIS |",
          type.toUpperCase(),
          globalInputsObj.meta.type[type].minDominantMin.toFixed(8),
          globalInputsObj.meta.type[type].totalInputs,
          globalInputsObj.meta.type[type].minTotalMin,
          globalInputsObj.meta.type[type].numInputs,
          globalInputsObj.meta.numInputs
        ]);

        cb();

      }, function(err){

        if (err) {
          return reject(err);
        }

        const histogramsUsedString = (globalInputsObj.meta.userProfileOnlyFlag) ? "profile" : "all";

        globalInputsObj.inputsId = configuration.inputsFilePrefix 
          + "_" + moment().format(compactDateTimeFormat) 
          + "_" + globalInputsObj.meta.numInputs 
          + "_" + histogramsUsedString 
          + "_" + hostname 
          + "_" + process.pid;


        const networkInputsDoc = new global.wordAssoDb.NetworkInputs(globalInputsObj);

        networkInputsDoc.save(async function(err, savedNetworkInputsDoc){

          if (err) {
            console.log(chalkError("GIS | *** CREATE NETWORK INPUTS DB DOCUMENT: " + err));
            return reject(err);
          }

          printInputsObj("GIS | +++ SAVED NETWORK INPUTS DB DOCUMENT", savedNetworkInputsDoc);

          console.log(chalk.blue(
              "\nGIS | ========================================================================================="
            + "\nGIS | INPUTS | USER PROFILE ONLY: " + configuration.userProfileOnlyFlag 
            + "\nGIS | -----------------------------------------------------------------------------------------\n"
            + table(tableArray, { align: ["l", "l", "r", "r", "r", "r", "r"] })
            + "\nGIS | =========================================================================================\n"
          ));

          const inFile = globalInputsObj.inputsId + ".json";

          console.log(chalkInfo("GIS | ... SAVING INPUTS FILE: " + inFolder + "/" + inFile));

          await tcUtils.saveFile({folder: inFolder, file: inFile, obj: globalInputsObj});

          console.log(chalkInfo("GIS | ... UPDATING INPUTS CONFIG FILE: " + configDefaultFolder + "/" + defaultInputsConfigFile));

          const networkInputsConfigObj = await tcUtils.loadFile({folder: configDefaultFolder, file: defaultInputsConfigFile, noErrorNotFound: true });

          networkInputsConfigObj.INPUTS_IDS.push(globalInputsObj.inputsId);
          networkInputsConfigObj.INPUTS_IDS = _.uniq(networkInputsConfigObj.INPUTS_IDS);

          await tcUtils.saveFile({folder: configDefaultFolder, file: defaultInputsConfigFile, obj: networkInputsConfigObj});

          let slackText = "\n*GIS | INPUTS*";
          slackText = slackText + "\n" + globalInputsObj.inputsId;

          await slackSendWebMessage({channel: slackChannel, text: slackText});

          resolve();

        });

      });  
    });

  });
}

setTimeout(async function(){

  try {

    const cnf = await initConfig(configuration);
    configuration = deepcopy(cnf);

    statsObj.status = "START";

    await initSlackRtmClient();
    await initSlackWebClient();

    if (configuration.testMode) {
      console.log(chalkAlert(MODULE_ID_PREFIX + " | TEST MODE"));
    }

    console.log(chalkBlue(
        "\n--------------------------------------------------------"
      + "\n" + MODULE_ID_PREFIX + " | " + configuration.processName 
      // + "\nCONFIGURATION\n" + jsonPrint(configuration)
      + "\n--------------------------------------------------------"
    ));

    await connectDb();

    if (configuration.generateBothUserProfileOnlyAndAllHistogramsInputs) {

      console.log(chalkAlert(
          "\n--------------------------------------------------------"
        + "\n" + MODULE_ID_PREFIX + " | GENERATING BOTH USER PROFILE ONLY + ALL HISTOGRAMS INPUTS"
        + "\n--------------------------------------------------------"
      ));

      configuration.userProfileOnlyFlag = true;
      await runMain();
      configuration.userProfileOnlyFlag = false;
      await runMain();
    }
    else{
      await runMain();
    }
    quit();

  }
  catch(err){
    console.log(chalkError(MODULE_ID_PREFIX + " | **** INIT CONFIG ERROR *****\n", err));
    if (err.code !== 404) {
      quit({cause: new Error("INIT CONFIG ERROR")});
    }
  }
}, 1000);
