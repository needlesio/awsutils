package io.needles.awsutils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * An OutputStream that streams data to use using s3's multipart upload.
 * Data will be chunked into 5mb blocks and uploaded in a parallel manner.
 */
public class S3OutputStream extends OutputStream {
    private static final int BUFFER_SIZE = 5 * 1024 * 1024;
    private final AmazonS3 s3Client;
    private final String bucketName;
    private final String key;
    private final ThreadPoolExecutor executorService;

    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(BUFFER_SIZE);
    private String uploadId;
    private int partId = 1;
    private List<Future<PartETag>> uploadTasks = new ArrayList<>();
    private volatile boolean error;

    public S3OutputStream(AmazonS3 s3Client, String bucketName, String key, int parallelism){
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.key = key;
        // A bit of a hack to create a blocking ThreadPoolExecutor
        executorService = new ThreadPoolExecutor(
                parallelism, parallelism, 100, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>() {
                    @Override
                    public boolean offer(Runnable runnable) {
                        try {
                            put(runnable);
                        } catch (InterruptedException e){
                            return false;
                        }
                        return true;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public synchronized void write(int b) throws IOException {
        checkErrors();
        buffer.write(b);
        if (buffer.size() >= BUFFER_SIZE) {
            writeChunk();
        }
    }

    @Override
    public synchronized void write(byte b[], int off, int len) throws IOException {
        checkErrors();
        if (b == null) {
            throw new NullPointerException();
        }

        while(len > 0) {
            int remaining = BUFFER_SIZE - buffer.size();
            int bytesToWrite = Math.min(remaining, len);

            buffer.write(b, off, bytesToWrite);
            len = len - bytesToWrite;
            off = off + bytesToWrite;
            if (buffer.size() == BUFFER_SIZE) {
                writeChunk();
            }
        }

    }

    @Override
    public synchronized void close() throws IOException {
        if (buffer.size() > 0) {
            writeChunk();
        }
        executorService.shutdown();

        if (!uploadTasks.isEmpty()) {
            List<PartETag> tags = new ArrayList<>();
            for (Future<PartETag> future : uploadTasks) {
                tags.add(getFromFuture(future));
            }

            s3Client.completeMultipartUpload(new CompleteMultipartUploadRequest(
                    bucketName, key, uploadId, tags
            ));
        }
    }

    private void initUpload() {
        if (uploadId == null) {
            uploadId = s3Client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucketName, key)).getUploadId();
        }
    }

    private void writeChunk() {
        final byte[] buf = buffer.toByteArray();
        final int partNum = partId++;
        initUpload();

        uploadTasks.add(executorService.submit(new Callable<PartETag>() {
            @Override
            public PartETag call() throws Exception {
                try {
                    return s3Client.uploadPart(new UploadPartRequest()
                            .withBucketName(bucketName)
                            .withKey(key)
                            .withPartNumber(partNum)
                            .withUploadId(uploadId)
                            .withInputStream(new ByteArrayInputStream(buf))
                            .withPartSize(buf.length)).getPartETag();
                } catch (Exception e) {
                    error = true;
                    throw e;
                }
            }
        }));

        buffer.reset();
    }

    private void checkErrors() throws IOException {
        if (error) {
            try {
                for (Future<PartETag> future : uploadTasks) {
                    if (future.isDone()) {
                        getFromFuture(future);
                    }
                }
            } finally {
                for (Future<PartETag> future : uploadTasks) {
                    future.cancel(true);
                }
            }
        }
    }

    private PartETag getFromFuture(Future<PartETag> future)  throws IOException{
        try {
            return future.get();
        } catch (InterruptedException e) {
            // should never happen as we checked to see if task was done
            throw new RuntimeException("Should never happen", e);
        } catch (ExecutionException e){
            Throwable cause = e.getCause();
            if (cause instanceof IOException){
                throw (IOException) cause;
            } else if (cause instanceof RuntimeException){
                throw (RuntimeException) cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }
}
