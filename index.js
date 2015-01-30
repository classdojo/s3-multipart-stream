/*
 * Write stream to AWS S3 using multipart upload.
*/



/*
  TODO:
    1. Create journal file name in Journal constructor
    2. Implement the stub method on Journal
    3. Test Journal
*/

var Writable       = require("stream").Writable;
var inherits       = require("util").inherits;
var chain          = require("slide").chain;
var _              = require("lodash");
var EventEmitter   = require("events").EventEmitter;
var fs             = require("fs");
var uuid           = require("node-uuid");
var once           = require("once");
var SparkMD5       = require("spark-md5");

var uploaderDebug  = require("debug")("uploader");
var uploadDebug    = require("debug")("upload");
var multiDebug     = require("debug")("multi");

var MINIMUM_CHUNK_UPLOAD_SIZE = 5242880;
var CONCURRENT_UPLOADS = 10;

/*
 *  @param s3Client
 *  @param [options]
 *          options.chunkUploadSize
 *          options.journalConfig
 *              options.journalConfig.sourceFile
 *              options.journalConfig.workingDirectory
 *  @param s3MultipartUploadConfig
 *
*/
inherits(MultipartWriteS3Upload, Writable);
function MultipartWriteS3Upload(s3Client, options, s3MultipartUploadConfig) {
  Writable.call(this);
  if(!s3MultipartUploadConfig) {
    s3MultipartUploadConfig = options;
    options = {};
  }
  this.__s3Client              = s3Client;
  this.s3MultipartUploadConfig = s3MultipartUploadConfig;
  this.__partNumber            = 1;
  this.__chunks                = [];
  this.__uploadedParts         = [];
  this.__queuedUploadSize      = 0;
  this.__uploadsInProgress     = 0;
  this.waitingUploads          = [];
  this.__concurrentUploads     = options.maxConcurrentUploads || CONCURRENT_UPLOADS;
  this.__chunkUploadSize       = _.isEmpty(options.chunkUploadSize) || _.isNaN(options.chunkUploadSize) || 
      options.chunkUploadSize < MINIMUM_CHUNK_UPLOAD_SIZE ?
              MINIMUM_CHUNK_UPLOAD_SIZE : options.chunkUploadSize;
  this.__journal               = new Journal(s3MultipartUploadConfig, options.journalConfig);
  this.__uploader              = new Uploader(this, this.__journal);
  this.__journal.on("journalError", function() {/* handle journal error */});
}


/*
 *
 * @param s3Client - fully configured aws-sdk instance
 * @param options
 *          options.chunkUploadSize
 *          options.maxConcurrentUploads
 *          options.multipartCreationParams -     {
 *               Bucket: 'STRING_VALUE',
 *               Key: 'STRING_VALUE',
 *               ACL: 'private | public-read | public-read-write | authenticated-read | bucket-owner-read | bucket-owner-full-control',
 *               CacheControl: 'STRING_VALUE',
 *               ContentDisposition: 'STRING_VALUE',
 *               ContentEncoding: 'STRING_VALUE',
 *               ContentLanguage: 'STRING_VALUE',
 *               ContentType: 'STRING_VALUE',
 *               Expires: new Date || 'Wed Dec 31 1969 16:00:00 GMT-0800 (PST)' || 123456789,
 *               GrantFullControl: 'STRING_VALUE',
 *               GrantRead: 'STRING_VALUE',
 *               GrantReadACP: 'STRING_VALUE',
 *               GrantWriteACP: 'STRING_VALUE',
 *               Metadata: {
 *                 someKey: 'STRING_VALUE',
 *                 //another key
 *               },
 *               SSECustomerAlgorithm: 'STRING_VALUE',
 *               SSECustomerKey: 'STRING_VALUE',
 *               SSECustomerKeyMD5: 'STRING_VALUE',
 *               ServerSideEncryption: 'AES256',
 *               StorageClass: 'STANDARD | REDUCED_REDUNDANCY',
 *               WebsiteRedirectLocation: 'STRING_VALUE'
 *             };
 *          options.journalConfig: {
 *             workingDirectory: "",
 *             sourceFile: ""
 *          }
 *
*/
MultipartWriteS3Upload.create = function(s3Client, options, cb) {
  var myS3Upload;
  var me = this;
  options.chunkUploadSize = options.chunkUploadSize || MINIMUM_CHUNK_UPLOAD_SIZE;
  /* */
  s3Client.createMultipartUpload(options.multipartCreationParams, function(err, s3MultipartUploadConfig) {
    if(err) {
      return cb(err);
    }
    myS3Upload = new me(s3Client, options, s3MultipartUploadConfig);
    MultipartWriteS3Upload._addFinishHandler(myS3Upload);
    myS3Upload.__uploader.start();
    cb(null, myS3Upload);
  });
};

MultipartWriteS3Upload._addFinishHandler = function(multipartWriteS3Upload) {
  multipartWriteS3Upload.on("finish", function() {
    multipartWriteS3Upload.finishUpload(function(err) {
      if(err) {
        return multipartWriteS3Upload.emit("error", err);
      }
      multipartWriteS3Upload.emit("done");
    });
  });
};


MultipartWriteS3Upload.prototype._write = function(chunk, enc, cb) {
  this.__chunks.push(chunk);
  this.__queuedUploadSize = this.__queuedUploadSize + _.size(chunk);
  if(this.__queuedUploadSize > this.__chunkUploadSize) {
    this._queueChunksForUpload();
  }
  cb();
};


MultipartWriteS3Upload.prototype._queueChunksForUpload = function(cb) {
  var partNumber = this.__partNumber++;
  /* 
     Unpack data from the partial function so the uploader can handle
     error scenario logging.
  */
  this.waitingUploads.push({
    chunks: this.__chunks,
    chunkSize: this.__queuedUploadSize,
    partNumber: partNumber,
    uploadFn: this._uploadChunks.bind(this, partNumber, this.__chunks)
  });

  //reset working sets
  this.__chunks = [];
  this.__queuedUploadSize = 0;
  if(cb) {
    cb();
  }
};

/* Set optional upload params that will override the defaults */
MultipartWriteS3Upload.prototype.setUploadParams = function(uploadParams) {
  this.__uploadParams = uploadParams;
};

MultipartWriteS3Upload.prototype.finishUpload = function(cb) {
  chain([
    !_.isEmpty(this.__chunks) && [this._queueChunksForUpload.bind(this)],
    [this._completeMultipartUpload.bind(this)]
  ], cb);
};

MultipartWriteS3Upload.prototype._completeMultipartUpload = function(cb) {
  var me = this;
  var completeConfig;
  var error;
  multiDebug("Waiting for uploader to finish");
  this.__uploader.once("empty", this._finalizeUpload.bind(this, cb));
};


MultipartWriteS3Upload.prototype._finalizeUpload = function(cb) {
  var sortedParts, completeConfig,
  me = this;
  sortedParts = _.sortBy(me.__uploadedParts, function(upload) {
    return upload.PartNumber;
  });
  completeConfig = {
    UploadId         : me.s3MultipartUploadConfig.UploadId,
    Bucket           : me.s3MultipartUploadConfig.Bucket,
    Key              : me.s3MultipartUploadConfig.Key,
    MultipartUpload  : {
      Parts: sortedParts
    }
  };

  multiDebug("Completing upload transfer");
  me.__uploader.stop();
  me.__s3Client.completeMultipartUpload(completeConfig, function(err, multipartResponse) {
    /*  Always log both error and multipart response into journal */
    me.__journal.logCompleteUploadResponse(err, multipartResponse, function(e) {
      error = err || e;
      if(error) {
        return cb(error);
      }
      cb();
    });
  });
};


/*
 * TODO: Add retries.
*/
MultipartWriteS3Upload.prototype._uploadChunks = function(partNumber, chunks, cb) {
  var body;
  var me = this;
  var uploadId = this.s3MultipartUploadConfig.UploadId;
  if(_.isString(chunks[0])) {
    body = chunks.join("");
  } else {
    //chunks are buffer objects
    body = Buffer.concat(chunks);
  }
  var uploadPayload = _.merge(
    this.__uploadParams || {},
    {
      UploadId    : uploadId,
      Body        : body,
      PartNumber  : partNumber,
      Bucket      : this.s3MultipartUploadConfig.Bucket, 
      Key         : this.s3MultipartUploadConfig.Key
    }
  );
  this.__s3Client.uploadPart(uploadPayload, function(err, uploadResponse) {
    if(err) {
      multiDebug("Error uploading chunks " + err);
      return cb(err);
    }
    me.__uploadedParts.push({
      ETag: uploadResponse.ETag,
      PartNumber: partNumber
    });
    multiDebug("Uploaded chunk " + partNumber);
    cb(null, uploadResponse);
  });
};




/* 
 * Always initialized. Does nothing if journal mode is not set
 *
 *
 *     @param []
 *       {
 *         workingDirectory :
 *         sourceFile  :
 *       }
*/


var journalStub = {
  logFinishedJobs: function(a, cb) { cb(); },
  logCompleteUploadResponse: function(a, b, cb) { cb(); },
  end: function(cb) { cb(); },
  hashSourceFile: function(cb) { cb(); }
};

inherits(Journal, EventEmitter);
function Journal(s3MultipartUploadConfig, journalConfig) {
  EventEmitter.call(this);
  var me                  = this;
  this.__journalMode      = !!journalConfig;
  if(!this.__journalMode) {
    return this._stubMethods();
  }
  this.__sourceFilePath     = journalConfig.sourceFile;
  this.__journalFile        = this._getJournalFileName(journalConfig.workingDirectory);
  this.__journal      = {
    uploadConfig: s3MultipartUploadConfig,
    segments: {
      success: [],
      failed: []
    }
  };
  this.__writeLk = 0;
}

/* 
 * Does not enforce 'once' semantics. Two successive
 * calls could result in inconsistencies between
 * this.__journal and the journal file contents.
 */
Journal.prototype.logFinishedJobs = function(jobs, cb) {
  for(var i in jobs) {
    this.__journal.segments[jobs[i].status].push(jobs[i]);
  }
  this._updateJournalFile(cb);
};

Journal.prototype._getJournalFileName = function(workingDirectory) {
  var ISONow = (new Date()).toISOString();
  return workingDirectory + "/" + ISONow;
};

/*
 * Stubs out all publicly used methods to transparently disable
 * journaling
*/
Journal.prototype._stubMethods = function() {
  _.extend(this, journalStub);
  return this;
};

Journal.prototype._updateJournalFile = function(cb) {
  var me = this;
  this.__writeLk++;
  fs.writeFile(this.__journalFile, JSON.stringify(this.__journal), function(err) {
    this.__writeLk--;
    if(err) {
      return cb(err);
    }
    cb();
  });
};

Journal.prototype.logCompleteUploadResponse = function(err, uploadResponse, cb) {
  this.__journal.uploadResponse = {
    body: uploadResponse
  };
  if(err) {
    this.__journal.uploadResponse.err = err.toString();
  }
  this._end(cb);
};

/*
 * 1. ensure outstanding fs commits are finished.
 * 2. hash sourcefile
 * 3. finish last fs commit recording the file hash and complete upload response
*/
Journal.prototype._end = function(cb) {
  chain([
    !this.__writeLk && [this.__waitForWritesToFinish],
    [this.hashSourceFile.bind(this)]
  ], cb);
};

Journal.prototype.__waitForWritesToFinish = function(cb) {
  var me = this;
  var t = setTimeout(function() {
    if(me.__writeLk === 0) {
      clearTimeout(t);
      cb();
    }
  }, 100);
};


Journal.prototype.hashSourceFile = function(cb) {
  var me                   = this;
  var sourceFileReadStream = fs.createReadStream(this.__sourceFilePath);
  var hasher               = new Writable();
  var spark                = new SparkMD5();
  var len = 0;
  hasher._write = function(buf, enc, cb) {
    len = len + buf.length;
    spark.append(buf);
    cb();
  };
  sourceFileReadStream.pipe(hasher);
  hasher.on("finish", function() {
    me.__journal.sourceFileChecksum = spark.end();
    me._updateJournalFile(cb);
  });
};


/*
  @param multipartWriteS3Upload - Configured instance of MultipartWriteS3Upload
  @param journal - Instance of Journal
*/
inherits(Uploader, EventEmitter);
function Uploader(multipartWriteS3Upload, journal) {
  this.__waitingUploads     = multipartWriteS3Upload.waitingUploads;
  this.__concurrentUploads  = multipartWriteS3Upload.__concurrentUploads;
  this.__id                 = uuid.v1();
  this.__outstandingUploads = [];
  this.__failedUploads      = [];
  this.__journal            = journal;
  this._serviceUploads      = once(this._serviceUploads_);
  this.__serviceInterval    = 1000;
}

Uploader.prototype.start = function() {
  var me = this;
  this.__i = setInterval(function() {
    me._serviceUploads();
  }, this.__serviceInterval);
};

Uploader.prototype.stop = function() {
  uploaderDebug("Stopping");
  clearInterval(this.__i);
};

/* TODO: Replace with an event based model that refills the outgoing queue only when needed. */
Uploader.prototype._serviceUploads_ = function(cb) {
  var upload,
  me = this;
  cb = cb || function() {};
  this._cleanupAndLogFinishedJobs(function(err) {
    if(err) {
      this.emit("uploaderError", err);
    } else {
      //refill pipeline
      while(me.__outstandingUploads.length < me.__concurrentUploads && !_.isEmpty(me.__waitingUploads)) {
        uploaderDebug("Initiating upload");
        upload = new UploadJob(me.__waitingUploads.shift());
        upload.start();
        me.__outstandingUploads.push(upload);
      }
      if(_.isEmpty(me.__waitingUploads) && _.isEmpty(me.__outstandingUploads)) {
        uploaderDebug("Empty");
        me.emit("empty");
      }
    }
    me._serviceUploads = once(me._serviceUploads_);
    cb(err);
  });
};

Uploader.prototype._cleanupAndLogFinishedJobs = function(cb) {
  var upload, i, compacted,
  finishedJobs = [];
  for(i in this.__outstandingUploads) {
    upload = this.__outstandingUploads[i];
    if(/failed|success/.test(upload.status)) {
      // this.__journal.logUploadJob(upload);
      finishedJobs.push(upload);
      this.__outstandingUploads[i] = null;
    }
  }
  compacted = _.compact(this.__outstandingUploads);
  this.__outstandingUploads = compacted;
  if(finishedJobs.length) {
    return this.__journal.logFinishedJobs(finishedJobs, cb);
  }
  cb();
};


function UploadJob(config) {
  this.config = config;
  this.status = "waiting";
}

UploadJob.prototype.start = function() {
  var me = this;
  this.status = "inProgress";
  uploadDebug("Start");
  this.config.uploadFn(function(err, uploadResponse) {
    if(err) {
      me.status = "failed";
      me.error = err;
      // me.emit("uploadError");
    } else {
      me.status = "success";
      me.uploadResponse = uploadResponse;
    }
    uploadDebug("Result " + me.status);
  });
};

UploadJob.prototype.toJSON = function() {
  var encodedData = [],
  serialized = {
    partNumber: this.config.partNumber,
    chunkSize:  this.config.chunkSize,
    status: this.status
  },
  chunk;
  if(this.status === "failed") {
    serialized.error = this.error.message;
  } else if(this.status === "success") {
    serialized.ETag = this.uploadResponse.ETag;
  }
  return serialized;
};

module.exports = MultipartWriteS3Upload;

//export for testing
module.exports.__Uploader   = Uploader;
module.exports.__UploadJob  = UploadJob;
module.exports.__Journal    = Journal;
