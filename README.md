## s3-multipart-stream
[![Build Status](https://travis-ci.org/classdojo/s3-multipart-stream.svg?branch=master)](https://travis-ci.org/classdojo/s3-multipart-stream)
[![codecov.io](https://codecov.io/github/classdojo/s3-multipart-stream/coverage.svg?branch=master)](https://codecov.io/github/classdojo/s3-multipart-stream?branch=master)
[![NPM version](https://badge.fury.io/js/s3-multipart-stream.png)](http://badge.fury.io/js/s3-multipart-stream)


```javascript
var AWS               = require("aws-sdk");
var S3MultipartStream = require("s3-multipart-stream");
var fs                = require("fs");

var s3 = new AWS.S3({
  accessKeyId: "myAccessKey",
  secretAccessKey: "mySecretAccessKey",
  region: "us-east-1"
});

var options = {
  parallelUploads : 10,
  chunkUploadSize : 5242880, // Upload at most 10 chunks of 5MB at a time.
  multipartCreationParams: {
    Bucket: "myBucket",
    Key: "myKey"
    /* Any params accepted by s3 multipart creation API */
  },
  workingDirectory : "/tmp"
};

var s3Stream = S3MultipartStream.create(s3, options, function(err, s3Stream) {
  if(err) {
    console.error(err);
    process.exit(1);
  }
  var fileStream = fs.createReadStream("someFile.txt");
  fileStream
  .pipe(s3Stream);  
});
```
