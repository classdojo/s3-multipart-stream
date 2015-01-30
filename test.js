var expect         = require("expect.js");
var sinon          = require("sinon");
var rewire         = require("rewire");
var s3Multipart    = rewire("./");
var _              = require("lodash");
var fs             = require("fs");
var timekeeper     = require("timekeeper");

var MINIMUM_CHUNK_UPLOAD_SIZE   = 25;
var CONCURRENT_UPLOADS          = 2;

s3Multipart.__set__("MINIMUM_CHUNK_UPLOAD_SIZE", MINIMUM_CHUNK_UPLOAD_SIZE);
s3Multipart.__set__("CONCURRENT_UPLOADS", CONCURRENT_UPLOADS);

describe("s3-multipart-new", function() {
  var mockS3Client;
  var largeString;
  var smallString = "small";
  var s3;
  var writeFileStub;
  var s3MultipartUploadConfig = {
    UploadId: 1,
    Bucket: "myBucket",
    Key: "myKey"
  };
  var workingFile = __dirname + "/journal-test.json";
  var sourceFile  = __dirname + "/test-source-file.txt";

  // creating a nice large string
  (function(){
    var str = "";
    for (var i = 0; i < (MINIMUM_CHUNK_UPLOAD_SIZE + 1); i++){
      str+="+";
    }
    largeString = str;
  })();

  before(function() {
    fs = require("fs");
    writeFileStub = sinon.stub(fs, "writeFile");
    writeFileStub.yieldsAsync(null);
  });



  beforeEach(function() {
    mockS3Client = sinon.stub({
      createMultipartUpload: function(){},
      completeMultipartUpload: function(){},
      uploadPart: function(){}
    });
    s3 = new s3Multipart(mockS3Client, {
      chunkUploadSize: 1,
      retryUpload: {
        workingDirectory : __dirname + "/tmp",
        sourceFile       : __dirname + "/test-source-file.txt"
      }
    }, s3MultipartUploadConfig);
  });

  describe("MultipartWriteS3Upload", function() {

    describe("MultipartWriteS3Upload.create", function() {

    });

    describe("constructor", function() {

    });

    describe("#_write", function() {
      beforeEach(function() {
        sinon.spy(s3, "_queueChunksForUpload");
      });

      it("adds the chunk size to the running upload size", function(done) {
        s3._write(smallString, "utf8", function() {
          expect(s3.__chunks).to.have.length(1);
          done();
        });
      });

      it("does not queue the chunk for upload if running chunk size total is less than desired chunk upload size", function(done) {
        s3._write(smallString, "utf8", function() {
          expect(s3.__chunks).to.have.length(1);
          expect(s3._queueChunksForUpload.callCount).to.be(0);
          done();
        });
      });

      it("queues the chunk for upload if running chunk size total is greater than desired chunk upload size", function(done) {
        s3._write(largeString, "utf8", function() {
          expect(s3._queueChunksForUpload.callCount).to.be(1);
          done();
        });
      });

    });
    
    describe("#_queueChunksForUpload", function(done) {
      beforeEach(function() {
        s3.__chunks = ["string", "string"];
        s3.__queuedUploadSize = 12;
        s3.__partNumber = 0;
      });

      it("pushes an expected upload job onto the waitingUploads queue", function() {
        var expectedUploadJob = {
          chunks: ["string", "string"],
          chunkSize: 12,
          partNumber: 0
        }, upload;
        s3._queueChunksForUpload();
        expect(s3.waitingUploads).to.have.length(1);
        expect(_.omit(s3.waitingUploads[0], "uploadFn")).to.eql(expectedUploadJob);
      });

      it("increments part number", function() {
        s3._queueChunksForUpload();
        expect(s3.__partNumber).to.be(1);
      });

      it("resets __chunks", function() {
        s3._queueChunksForUpload();
        expect(s3.__chunks).to.be.empty();
      });

      it("resets __queuedUploadSize", function() {
        s3._queueChunksForUpload();
        expect(s3.__queuedUploadSize).to.be(0);
      });

      it("calls cb if provided", function(done) {
        s3._queueChunksForUpload(done);
      });
    });

    describe("#finishUpload", function() {
      beforeEach(function() {
        var completeMultipartStub;
        sinon.spy(s3, "_queueChunksForUpload");
        completeMultipartStub = sinon.stub(s3, "_completeMultipartUpload");
        completeMultipartStub.yields(null);
      });

      describe("when there's chunks still to upload", function() {
        beforeEach(function() {
          s3.__chunks = [smallString];
        });

        it("calls _queueChunksForUpload", function(done) {
          s3.finishUpload(function(err) {
            expect(s3._queueChunksForUpload.callCount).to.be(1);
            done();
          });
        });
      });

      describe("when there's no chunk still to upload", function() {
        it("does not call _queueChunksForUpload", function(done) {
          s3.finishUpload(function(err) {
            expect(s3._queueChunksForUpload.callCount).to.be(0);
            done();
          });
        });
      });

      it("calls #_completeMultipartUpload", function(done) {
        s3.finishUpload(function(err) {
          expect(s3._completeMultipartUpload.callCount).to.be(1);
          done();
        });
      });

      // it("writes the file's MD5 checksum to the journal file", function() {

      // });
    });

    describe("#retryUpload", function() {

    });

    describe("#_completeMultipartUpload", function() {

      beforeEach(function() {
        sinon.stub(s3.__uploader, "once").yields();
        sinon.stub(s3, "_finalizeUpload").yields(null);
      });

      it("waits for __uploader to emit empty", function(done) {
        s3._completeMultipartUpload(function(err) {
          expect(s3.__uploader.once.callCount).to.be(1);
          done();
        });
      });

      it("calls _finalizeUpload", function(done) {
        s3._completeMultipartUpload(function(err) {
          expect(s3._finalizeUpload.callCount).to.be(1);
          done();
        });
      });
    });


    describe("#_finalizeUpload", function() {
      var uploadedPart1 = {
        ETag: "TAG",
        PartNumber: 1
      };
      var uploadedPart2 = {
        ETag: "TAG",
        PartNumber: 2
      };

      beforeEach(function() {
        s3.__uploadedParts = [uploadedPart2, uploadedPart1];
        s3.__s3Client.completeMultipartUpload.yields(null);
        sinon.stub(s3.__journal, "logCompleteUploadResponse").yields(null);
      });

      it("calls s3#completeMultipartUpload with the proper config", function(done) {
        var expectedConfig = {
          UploadId  : s3MultipartUploadConfig.UploadId,
          Bucket    : s3MultipartUploadConfig.Bucket,
          Key       : s3MultipartUploadConfig.Key,
          MultipartUpload : {
            Parts: [uploadedPart1, uploadedPart2]
          }
        };
        s3._finalizeUpload(function(err) {
          expect(s3.__s3Client.completeMultipartUpload.callCount).to.be(1);
          expect(s3.__s3Client.completeMultipartUpload.firstCall.args[0]).to.eql(expectedConfig);
          done();
        });

      });

      it("calls Journal#logCompleteUploadResponse with the multipart error and response", function(done) {
        var err = new Error("My Error");
        var response = {some: "response"};
        s3.__s3Client.completeMultipartUpload.yields(err, response);
        s3._finalizeUpload(function(err) {
          expect(s3.__journal.logCompleteUploadResponse.callCount).to.be(1);
          expect(s3.__journal.logCompleteUploadResponse.firstCall.args[0]).to.eql(err);
          expect(s3.__journal.logCompleteUploadResponse.firstCall.args[1]).to.eql(response);
          done();
        });
      });
    });

    describe("#_uploadChunks", function() {
      var partNumber = 1;
      beforeEach(function() {
        s3.__s3Client.uploadPart.yields(null, {
          ETag: "ETag"
        });
      });


      it("creates a proper body when the chunks are strings", function(done) {
        var chunks = ["string", "string"];
        s3._uploadChunks(partNumber, chunks, function(err) {
          expect(s3.__s3Client.uploadPart.firstCall.args[0].Body).to.be("stringstring");
          done();
        });
      });

      it("creates a proper body when the chunks are buffers", function(done) {
        var chunks = [new Buffer("string"), new Buffer("string")];
        var body;
        s3._uploadChunks(partNumber, chunks, function(err) {
          body = s3.__s3Client.uploadPart.firstCall.args[0].Body;
          expect(body).to.be.a(Buffer);
          expect(body.toString()).to.be("stringstring");
          done();
        });
      });

      it("calls s3Client#uploadPart with the proper config", function(done) {
        var config;
        var chunks = ["string", "string"];
        var expectedConfig = {
          UploadId: s3MultipartUploadConfig.UploadId,
          Bucket: s3MultipartUploadConfig.Bucket,
          Key: s3MultipartUploadConfig.Key,
          Body: "stringstring",
          PartNumber: partNumber
        };
        s3._uploadChunks(partNumber, chunks, function(err) {
          config = s3.__s3Client.uploadPart.firstCall.args[0];
          expect(config).to.eql(expectedConfig);
          done();
        });
      });

      it("errors out when the upload fails", function(done) {
        s3.__s3Client.uploadPart.yields(new Error("error"));
        s3._uploadChunks(partNumber, ["string"], function(err) {
          expect(err).to.be.an(Error);
          done();
        });
      });


      it("adds a successful job to the __uploadedParts array", function(done) {
        var expectedUploadParts = [
          {
            ETag: "ETag",
            PartNumber: partNumber
          }
        ];
        s3._uploadChunks(partNumber, ["string"], function(err) {
          expect(s3.__uploadedParts).to.eql(expectedUploadParts);
          done();
        });
      });
    });
  });

  describe("UPLOADERS", function() {
    var uploader,
    workingDirectory = __dirname + "/tmp",
    upload1, upload2, upload3;
    beforeEach(function() {
      upload1 = {
        chunks: ["string"],
        chunkSize: 6,
        partNumber: 1,
        uploadFn: function(cb) { cb(new Error("error"));}
      };
      upload2 = {
        chunks: ["string", "string"],
        chunkSize: 12,
        partNumber: 2,
        uploadFn: function(cb) {cb(null, {ETag: "ETag1"});}
      };
      upload3 = {
        chunks: ["string", "string", "string"],
        chunkSize: 18,
        partNumber: 3,
        uploadFn: function(cb) {cb(null, {ETag: "ETag2"});}
      };
    });

    describe("Uploader", function() {
      var journal;
      beforeEach(function() {
        var retryOptions = {
          workingDirectory: __dirname,
          sourceFile: __dirname + "/test-source-file.txt"
        };
        journal = new s3Multipart.__Journal(s3MultipartUploadConfig, retryOptions);
        journal._stubMethods();
        uploader = new s3Multipart.__Uploader(s3, journal);
        uploader.__waitingUploads = [upload1, upload2, upload3];
      });


      describe("#serviceUploads", function() {
        beforeEach(function() {
          uploader._cleanupAndLogFinishedJobs = sinon.stub().yields(null);
        });

        it("calls #_cleanupAndLogFinishedJobs", function() {
          uploader._serviceUploads_();
          expect(uploader._cleanupAndLogFinishedJobs.callCount).to.be(1);
        });

        it("refills the __outstandingUploads up to parallel uploads limit", function(done) {
          uploader.__concurrentUploads = CONCURRENT_UPLOADS;
          uploader._serviceUploads(function(err) {
            expect(uploader.__outstandingUploads).to.have.length(2);
            done();
          });
        });

        it("it does not add anything to __outstandingUploads if __waitingUploads is empty", function(done) {
          uploader.__waitingUploads = [];
          uploader._serviceUploads(function(err) {
            expect(uploader.__outstandingUploads).to.have.length(0);
            done();
          });
        });

        it("emits an 'empty' event if both __waitingUploads and __outstandingUploads are empty", function(done) {
          uploader.__waitingUploads = [];
          uploader.on("empty", done);
          uploader._serviceUploads();
        });

        it("does not emit 'empty' if __outstandingUploads is not empty", function() {
          // stubbing makes test synchronous
          uploader.on("empty", function() {
            throw new Error("should not emit");
          });
          uploader._serviceUploads();
        });
      });

      describe("#_cleanupAndLogFinishedJobs", function() {
        var jobs, copiedJobs;
        beforeEach(function() {
          jobs = [
            upload1,
            upload2,
            upload3
          ].map(function(e) {
            return new s3Multipart.__UploadJob(e);
          });
          jobs.forEach(function(job) {
            job.start();
          });
          copiedJobs = jobs.map(_.identity);
          uploader.__outstandingUploads = jobs;
          sinon.stub(journal, "logFinishedJobs").yields(null);
        });


        it("logs all finished jobs", function(done) {
          uploader._cleanupAndLogFinishedJobs(function(err) {
            expect(journal.logFinishedJobs.callCount).to.be(1);
            var loggedJobs = journal.logFinishedJobs.firstCall.args[0];
            expect(loggedJobs).to.have.length(3);
            done();
          });
        });

        it("logs nothing if there are no jobs finished", function(done) {
          uploader.__outstandingUploads = [new s3Multipart.__UploadJob(upload1)];
          uploader._cleanupAndLogFinishedJobs(function(err) {
            expect(journal.logFinishedJobs.callCount).to.be(0);
            done();
          });
        });
      });

      describe("#start", function() {
        xit("starts the service upload loop", function(done) {
          var stub = sinon.stub(uploader, "_serviceUploads");
          uploader.start();
          setTimeout(function() {
            expect(stub.callCount).to.be(1);
            done();
          }, 25);
          stub.restore();
        });
      });

      describe("#stop", function() {
        it("stops the uploader");
      });
    });

    describe("Journal", function() {
      var journal;
      var failedJob, successfulJob;
      var FAILED = "failed";
      var SUCCESS = "success";
      var retryOptions = {
        workingDirectory: __dirname,
        sourceFile: __dirname + "/test-source-file.txt"
      };
      var frozenDate = new Date("2015-01-22T22:18:41.145Z");

      beforeEach(function() {
        timekeeper.freeze(frozenDate);
        journal = new s3Multipart.__Journal(s3MultipartUploadConfig, retryOptions);
        failedJob = new s3Multipart.__UploadJob(upload1);
        successfulJob = new s3Multipart.__UploadJob(upload2);
        failedJob.start();
        successfulJob.start();
      });

      afterEach(function() {
        timekeeper.reset();
      });

      describe("#logFinishedJobs", function() {

        beforeEach(function() {
          journal._updateJournalFile = sinon.stub().yields(null);
        });

        it("pushes the jobs into the proper journal segment", function(done) {
          journal.logFinishedJobs([failedJob, successfulJob], function() {
            expect(journal.__journal.segments[FAILED]).to.have.length(1);
            expect(journal.__journal.segments[SUCCESS]).to.have.length(1);
            expect(JSON.stringify(journal.__journal.segments[FAILED].shift())).to.be(JSON.stringify(failedJob));
            expect(JSON.stringify(journal.__journal.segments[SUCCESS].shift())).to.be(JSON.stringify(successfulJob));
            done();
          });
        });

        it("updates the journal file the journal file", function(done) {
          journal.logFinishedJobs([failedJob, successfulJob], function() {
            expect(journal._updateJournalFile.callCount).to.be(1);
            done();
          });
        });
      });

      describe("#_updateJournalFile", function() {

        it("commits proper file contents to disk", function(done) {
          var expectedFileContents = {
            uploadConfig: s3MultipartUploadConfig,
            segments: {
              success: [successfulJob],
              failed:  [failedJob]
            }
          };
          //simplify setup by calling logfinishedJobs which calls _updatedJournalFile
          journal.logFinishedJobs([failedJob, successfulJob], function() {
            expect(writeFileStub.callCount).to.be(1);
            expect(writeFileStub.firstCall.args[1]).to.eql(JSON.stringify(expectedFileContents));
            done();
          });
        });

        it("writes to the filename returned by _getJournalFileName", function(done) {
          journal.logFinishedJobs([failedJob, successfulJob], function() {
            expect(writeFileStub.firstCall.args[0]).to.eql(journal._getJournalFileName(retryOptions.workingDirectory));
            done();
          });
        });
      });

      describe("#_getJournalFileName", function() {
        it("creates a filename that is an ISO string representing now", function() {
          expect(journal._getJournalFileName("/workingDirectory")).to.be("/workingDirectory/" + frozenDate.toISOString());
        });

      });

      describe("#logCompleteUploadResponse", function() {

      });
    });

    describe("UploadJob", function() {
      describe("#serialize", function() {
        var config;

        describe("failed job", function() {
          var failedJob;
          beforeEach(function() {
            failedJob = new s3Multipart.__UploadJob(upload1);
            failedJob.start();
          });

          it("stringifying returns a properly formatted serialized object", function() {
            var expectedConfig = {
              partNumber: 1,
              chunkSize: 6,
              status: "failed",
              error: "error"
            };
            config = JSON.stringify(failedJob);
            expect(config).to.eql(JSON.stringify(expectedConfig));
          });
        });

        describe("success job", function() {
          var successJob;
          beforeEach(function() {
            successJob = new s3Multipart.__UploadJob(upload2);
            successJob.start();
          });

          it("returns a properly formatted serialized object", function() {
            var expectedConfig = {
              partNumber: 2,
              chunkSize: 12,
              status: "success",
              ETag: "ETag1"
            };
            config = JSON.stringify(successJob);
            expect(config).to.eql(JSON.stringify(expectedConfig));
          });

        });

        describe("waiting|inProgress job", function() {
          var otherJob;
          beforeEach(function() {
            otherJob = new s3Multipart.__UploadJob(upload2);
          });

          it("returns a properly formatted serialized object", function() {
            var expectedConfig = {
              partNumber: 2,
              chunkSize: 12,
              status: "waiting"
            };
            config = JSON.stringify(otherJob);
            expect(config).to.eql(JSON.stringify(expectedConfig));
          });
        });
      });
    });
  });
});

