package io.needles.awsutils;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import io.findify.s3mock.S3Mock;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class S3OutputStreamTest {
    private static String baseS3MockDir = "build/tmp/s3";
    private static int mockPort = 7861;
    private static String bucket = "testbucket";
    private static S3Mock mockS3;

    @BeforeClass
    public static void setupAll() throws Exception {
        FileUtils.deleteDirectory(new File(baseS3MockDir));
        mockS3 = S3Mock.create(mockPort, baseS3MockDir);
        mockS3.start();
        s3Client().createBucket(bucket);
    }

    @AfterClass
    public static void tearDownAll() {
        mockS3.stop();
    }

    @Test
    public void testWrite() throws Exception {
        S3OutputStream out = new S3OutputStream(s3Client(), bucket, "my/key", 5);
        byte[] buf = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890".getBytes();

        out.write(1);
        out.write(2);

        for (int i=0; i < 1000000; i++) {
            out.write(buf);
        }
        out.close();

        File file = new File(baseS3MockDir, bucket + "/my/key");
        byte[] fileContents = IOUtils.toByteArray(file.toURI());
        int fileLength = fileContents.length;

        assertEquals(2 + 1000000 * buf.length, fileLength);

        assertEquals(1, fileContents[0]);
        assertEquals(2, fileContents[1]);

        byte[] actual = new byte[buf.length];
        System.arraycopy(fileContents, fileLength - buf.length, actual, 0, buf.length);
        assertArrayEquals(buf, actual);
    }


    public static AmazonS3 s3Client() {
        AmazonS3 s3Client = new AmazonS3Client(new AnonymousAWSCredentials());
        s3Client.setEndpoint("http://127.0.0.1:" + mockPort);
        return s3Client;
    }
}
