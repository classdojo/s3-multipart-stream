var expect = require("expect.js");
var sinon  = require("sinon");
var rewire = require("rewire");
var s3Multipart = rewire("./");
var _              = require("lodash");

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
      workingDirectory: __dirname + "/tmp"
    });
    s3.s3MultipartUploadConfig = s3MultipartUploadConfig;
  });

  describe("MultipartWriteS3Upload", function() {

    describe("MultipartWriteS3Upload.create", function() {

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
    });

    describe("#_completeMultipartUpload", function() {
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
      });
      it("constructs a proper config to pass onto s3", function() {
        var expectedConfig = {
          UploadId  : s3MultipartUploadConfig.UploadId,
          Bucket    : s3MultipartUploadConfig.Bucket,
          Key       : s3MultipartUploadConfig.Key,
          MultipartUpload : {
            Parts: [uploadedPart1, uploadedPart2]
          }
        };
        s3._completeMultipartUpload(function(err) {
          expect(s3.__s3Client.completeMultipartUpload.callCount).to.be(1);
          expect(s3.__s3Client.completeMultipartUpload.firstCall.args[0]).to.eql(expectedConfig);
        });
        s3.__uploader.emit("empty");
      });

      it("waites for __uploader to emit empty", function(done) {
        sinon.stub(s3.__uploader, "once").yields();
        s3._completeMultipartUpload(function(err) {
          expect(s3.__uploader.once.callCount).to.be(1);
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
      beforeEach(function() {
        uploader = new s3Multipart.__Uploader(s3, workingDirectory);
        uploader.__waitingUploads = [upload1, upload2, upload3];
      });


      describe("#serviceUploads", function() {
        beforeEach(function() {
          uploader._cleanupAndLogFinishedJobs = sinon.stub();
        });

        it("calls #_cleanupAndLogFinishedJobs", function() {
          uploader.serviceUploads();
          expect(uploader._cleanupAndLogFinishedJobs.callCount).to.be(1);
        });

        it("refills the __outstandingUploads up to parallel uploads limit", function() {
          uploader.__concurrentUploads = CONCURRENT_UPLOADS;
          uploader.serviceUploads();
          expect(uploader.__outstandingUploads).to.have.length(2);
        });

        it("it does not add anything to __outstandingUploads if __waitingUploads is empty", function() {
          uploader.__waitingUploads = [];
          uploader.serviceUploads();
          expect(uploader.__outstandingUploads).to.have.length(0);
        });

        it("emits an 'empty' event if both __waitingUploads and __outstandingUploads are empty", function(done) {
          uploader.__waitingUploads = [];
          uploader.on("empty", done);
          uploader.serviceUploads();
        });

        it("does not emit 'empty' if __outstandingUploads is not empty", function() {
          uploader.on("empty", function() {
            throw new Error("should not emit");
          });
          uploader.serviceUploads();
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
        });

        it("adds failed jobs to the failed journal array", function() {
          uploader._cleanupAndLogFinishedJobs();
          expect(uploader.__journal.segments.failed).to.have.length(1);
          expect(uploader.__journal.segments.failed[0]).to.not.be.empty();
        });

        it("adds successful jobs to the success journal array", function() {
          uploader._cleanupAndLogFinishedJobs();
          expect(uploader.__journal.segments.success).to.have.length(2);
          expect(uploader.__journal.segments.success[0]).to.not.be.empty();
          expect(uploader.__journal.segments.success[1]).to.not.be.empty();
        });

        it("compacts failed and successful jobs", function() {
          uploader._cleanupAndLogFinishedJobs();
          expect(uploader.__outstandingUploads).to.be.empty();
        });

        it("does not compact jobs that are not failed or successful", function() {
          jobs.forEach(function(job) {
            job.status = "waiting";
          });
          uploader._cleanupAndLogFinishedJobs();
          expect(uploader.__outstandingUploads).to.have.length(3);
        });

        describe("serialization", function() {
          beforeEach(function() {
            jobs.forEach(function(job) {
              job.serialize = sinon.stub();
            });
          });
          it("serializes failed jobs", function() {
            uploader._cleanupAndLogFinishedJobs();
            expect(_.pluck(copiedJobs, "status").filter(function(status) { return status === "failed";})).to.not.be.empty();
            copiedJobs.forEach(function(job) {
              if(job.status === "failed") {
                expect(job.serialize.callCount).to.be(1);
              }
            });
          });

          it("serializes successful jobs", function() {
            uploader._cleanupAndLogFinishedJobs();
            expect(_.pluck(copiedJobs, "status").filter(function(status) { return status === "success";})).to.not.be.empty();
            copiedJobs.forEach(function(job) {
              if(job.status === "success") {
                expect(job.serialize.callCount).to.be(1);
              }
            });
          });
        });

        describe("integration journal write", function() {

          beforeEach(function() {
            writeFileStub.reset();
          });

          it("attempts to write the proper journal file", function() {
            var expectedJournal = {
              uploadConfig: s3MultipartUploadConfig,
              segments: {
                success: [
                  {
                    partNumber: 2,
                    ETag: "ETag1"
                  },
                  {
                    partNumber: 3,
                    ETag: "ETag2"
                  }
                ],
                failed: [{
                  partNumber: 1,
                  error: "error",
                  data: [115, 116, 114, 105, 110, 103]
                }]
              }
            };
            uploader._cleanupAndLogFinishedJobs();
            expect(writeFileStub.callCount).to.be(1);
            expect(writeFileStub.firstCall.args[1]).to.be(JSON.stringify(expectedJournal));
          });
        });
      });

      describe("#commitJournal", function() {
        beforeEach(function() {
          writeFileStub.reset();
        });

        it("only allows one outstanding write at a time", function() {
          //simulate two quick successive calls
          uploader.commitJournal();
          uploader.commitJournal();
          expect(writeFileStub.callCount).to.be(1);
        });

        it("passes the proper arguments to fs.writeFile", function() {
          var exampleJournal = {
            uploadConfig: {},
            segments: {
              success: [],
              failed: []
            }
          };
          uploader.__journal = exampleJournal;
          uploader.commitJournal();
          expect(writeFileStub.firstCall.args[0]).to.be(uploader.__journalFile);
          expect(writeFileStub.firstCall.args[1]).to.eql(JSON.stringify(exampleJournal));
        });

        it("emits 'journalError' when there's a problem writing the file", function(done) {
          writeFileStub.yieldsAsync(new Error("error"));
          uploader.on("journalError", function(err) {
            expect(err).to.be.an(Error);
            done();
          });
          uploader.commitJournal();
        });
      });

      describe("#start", function() {

      });

      describe("#stop", function() {

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

          it("returns a properly formatted serialized object", function() {
            var expectedConfig = {
              partNumber: 1,
              error: "error",
              data: [115, 116, 114, 105, 110, 103]
            };
            config = failedJob.serialize();
            expect(config).to.eql(expectedConfig);
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
              ETag: "ETag1"
            };
            config = successJob.serialize();
            expect(config).to.eql(expectedConfig);
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
              status: "waiting"
            };
            config = otherJob.serialize();
            expect(config).to.eql(expectedConfig);
          });
        });
      });
    });

  });

});


