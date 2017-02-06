##Aws Utils

Helper utilities for use with AWS.

* S3OutputStream - An outputstream that can be used to stream a stream of unknown size to S3.
  
  Chunks the data into 5mb chunks and uploads these to s3 using s3's multipart upload support.
  Chunks can be uploaded in parallel using the stream's internal executor.