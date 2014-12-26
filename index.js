/*
 * Write stream to AWS S3 using multipart upload.
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

// function MultipartWriteS3(s3Client) {
//   this.__s3Client = s3Client;
//   this.__uploads = [];
// }


/*
 *  @param options
 *          options.chunkUploadSize
 *          options.workingDirectory
 *
*/
inherits(MultipartWriteS3Upload, Writable);
function MultipartWriteS3Upload(s3Client, s3MultipartUploadConfig, options) {
  Writable.call(this);
  this.__s3Client              = s3Client;
  this.s3MultipartUploadConfig = s3MultipartConfig;
  this.__partNumber            = 1;
  this.__chunks                = [];
  this.__uploadedParts         = [];
  this.__queuedUploadSize      = 0;
  this.__uploadsInProgress     = 0;
  this.waitingUploads          = [];
  this.__journal               = new Journal(s3MultipartUploadConfig, options.retryUplaod);
  this.__uploader              = new Uploader(this, this.__journal);
  this.__concurrentUploads     = options.maxConcurrentUploads || CONCURRENT_UPLOADS;
  this.__chunkUploadSize    = _.isEmpty(options.chunkUploadSize) || 
                               _.isNaN(options.chunkUploadSize) ||
                               options.chunkUploadSize < MINIMUM_CHUNK_UPLOAD_SIZE ?
                                    MINIMUM_CHUNK_UPLOAD_SIZE : options.chunkUploadSize;

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
 *          options.retryUpload: {
 *             workingDirectory: "",
 *             sourceFile: ""
 *          }
 *
*/
MultipartWriteS3Upload.create = function(s3Client, options, cb) {
  var myS3Upload;
  options.chunkUploadSize = options.chunkUploadSize || MINIMUM_CHUNK_UPLOAD_SIZE;
  /* */
  s3Client.createMultipartUpload(options.multipartCreationParams, function(err, s3MultipartUploadConfig) {
    if(err) {
      return cb(err);
    }
    myS3Upload = new this(s3Client, s3MultipartUploadConfig, options);
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


/* Initial naive implementation of writing each chunk serially to s3.
 * Only accepts Buffer|String right now.
*/

MultipartWriteS3Upload.prototype._write = function(chunk, enc, cb) {
  var me = this;
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
  var sortedParts;
  var completeConfig;
  multiDebug("Waiting for uploader to finish");
  this.__uploader.once("empty", function() {
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
      /* let's journal the response */
      me.__journal.logCompleteUploadResponse(err, multipartResponse);

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
 *      {
          workingFile :
          sourceFile  :
        }
*/
function Journal(s3MultipartUploadConfig, retryOptions) {
  var me              = this;
  this.__filePath     = filePath;
  this.__journal      = {
    uploadConfig: s3MultipartConfig,
    segments: {
      success: [],
      failed: []
    }
  };
  this.__filePath     = filePath;
  this.__journalMode  = !!retryOptions;
  this._writeFile     = once(this._writeFile_);
  this._cb            = function() {};
}

Journal.prototype.logUploadJob = function(job) {
  if(/failed|success/.match(job.status)) {
    this._writeFile();
  }
};

/* once version of function is stored in _writeFile */
Journal.prototype._writeFile_ = function(cb) {
  fs.writeFile(this.__journalFile, JSON.stringify(this.__journal), function(err) {
    if(err) {
      me.emit("journalError", err);
    }
    this._writeFile = once(this._writeFile_);
    (cb || this._cb)();
  });
};

Journal.prototype.logCompleteUploadResponse = function(err, uploadResponse) {

};

/* 
 * 1. ensure outstanding fs commits are finished.
 * 2. hash sourcefile
 * 3. finish last fs commit recording the file hash and return
*/
Journal.prototype.end = function(cb) {
  chain([
    this._writeFile.called && [this._waitForFileWriteFinish()],
    [this.hashSourceFile.bind(this)]
  ], cb);
};

Journal.prototype.hashSourceFile = function(cb) {
  var me                   = this;
  var sourceFileReadStream = fs.createReadStream(this.__filePath);
  var hasher               = new Writable();
  var spark                = new SparkMD5();
  hasher._write = function(buf, enc, cb) {
    spark.append(buf);
    cb();
  };
  sourceFileReadStream.pipe(hasher);
  hasher.on("end", function() {
    me.__journal.sourceFileChecksum = spark.end();
    me._writeFile_(cb);
  });
};

Journal.prototype._waitForFileWriteFinish = function() {
  return function(cb) {
    this._cb = cb;
  }
};

/* Only allow at most one outstanding commit to fs.
    
    TODO: replace manual bookkeeping with 'once' function  
*/
Journal.prototype.__commitJournal = function() {
  if(!this.__journalMode) {
    return;
  }
  var me = this;
  if(!this.__outstandingJournal) {
    this.__outstandingJournal = true;
    fs.writeFile(this.__journalFile, JSON.stringify(this.__journal), function(err) {
      if(err) {
        return me.emit("journalError", err);
      }
      me.__outstandingJournal = false;
    });
  }
};



inherits(Uploader, EventEmitter);
function Uploader(s3Multipart, journal) {
  this.__waitingUploads     = s3Multipart.waitingUploads;
  this.__id                 = uuid.v1();
  this.__outstandingUploads = [];
  this.__failedUploads      = [];
  this.__journal            = journal;
}

Uploader.prototype.start = function() {
  this.__i = setInterval(this.serviceUploads.bind(this), 1000);
};

Uploader.prototype.stop = function() {
  uploaderDebug("Stopping");
  clearInterval(this.__i);
};

Uploader.prototype.serviceUploads = function() {
  var upload;
  //cleanup outstandingUploads array
  this._cleanupAndLogFinishedJobs();
  while(this.__outstandingUploads.length < this.__concurrentUploads && !_.isEmpty(this.__waitingUploads)) {
    uploaderDebug("Initiating upload");
    upload = new UploadJob(this.__waitingUploads.shift());
    upload.start();
    this.__outstandingUploads.push(upload);
  }
  if(_.isEmpty(this.__waitingUploads) && _.isEmpty(this.__outstandingUploads)) {
    uploaderDebug("Empty");
    this.emit("empty");
  }
};

Uploader.prototype._cleanupAndLogFinishedJobs = function() {
  var upload, i, compacted;
  for(i in this.__outstandingUploads) {
    upload = this.__outstandingUploads[i];
    if(/failed|success/.test(upload.status)) {
      this.__journal.logUploadJob(upload)
      this.__outstandingUploads[i] = null;
    }
  }
  compacted = _.compact(this.__outstandingUploads);
  this.__outstandingUploads = compacted;
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

UploadJob.prototype.serialize = function() {
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
moduel.exports.__Journal    = Journal;
