## s3-multipart-stream
[![Build Status](https://travis-ci.org/classdojo/s3-multipart-stream.svg?branch=master)](https://travis-ci.org/classdojo/s3-multipart-stream)
[![codecov.io](https://codecov.io/github/classdojo/s3-multipart-stream/coverage.svg?branch=master)](https://codecov.io/github/classdojo/s3-multipart-stream?branch=master)
[![NPM version](https://badge.fury.io/js/s3-multipart-stream.png)](http://badge.fury.io/js/s3-multipart-stream)


s3-multipart-stream uploads files to Amazon S3 using the multipart upload API. It uploads streams by separating it into chunks and concurently uploading those chunks to S3.  The chunk size and maximum concurrent upload count is configurable via the options hash.

Each chunk upload is recorded in a working file whose location is specified by the workingDirectory option. Successful chunk uploads write their ETag, upload number, and chunk size to the file, while chunks that error out additionally write their data and error message into the file.

```javascript
var AWS               = require("aws-sdk");
var S3MultipartStream = require("s3-multipart-stream");
var fs                = require("fs");

/* Configure the AWS client with your credentials */
var s3 = new AWS.S3({
  accessKeyId: "myAccessKey",
  secretAccessKey: "mySecretAccessKey",
  region: "us-east-1"
});

var options = {
  /* Upload at most 10 concurrent chunks of 5MB each */
  chunkUploadSize         :  5242880
  maxConcurrentUploads    :  10
  multipartCreationParams: {
    Bucket: "myBucket",
    Key: "myKey"
    /* Any params accepted by s3 multipart creation API */
  },
  retryUpload: {
    workingDirectory  : "/tmp",
    sourceFile        : "",
    retries           : 10      
  }
};

S3MultipartStream.create(s3, options, function(err, s3Stream) {
  if(err) {
    console.error(err);
    process.exit(1);
  }

  var fileStream = fs.createReadStream("someFile.txt");
  fileStream
  .pipe(s3Stream);

});
```


## TODO

* Add `retryUpload(dataFile, journalFile)` method that retries a failed upload by retrying failed chunks.
