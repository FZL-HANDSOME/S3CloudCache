package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.storage.factory.S3ClientFactory;
import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterWriter;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockSizeLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockUploadConcurrencyLevel;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class S3CloudCacheInstanceText {

    static void main() {
        cloudCacheInstanceText();
    }


    //测试WAL持久化是否正常工作
    private static void cloudCacheInstanceText() {
        //创建S3Client(String endpoint, String region, String accessKey, String secretKey)
        S3Client s3Client = S3ClientFactory.createS3Client("http://127.0.0.1:9001", "cn-local", "123456789", "123456789");
        //创建全局配置文件
        BucketConfig defaluetBucketConfig = new BucketConfig();
        defaluetBucketConfig
                .setBlockSize(BlockSizeLevel.SMALL.getBytes())
                .setCacheSize(16 * 1024 * 1024L)
                .setBlockUpLoadCount(BlockUploadConcurrencyLevel.LOW.getConcurrency())
                .setS3KeyPrefix("oreder/phone")
                .setLockMappedFilePageCache(false)
                .setWarmWalFile(true)
                .setWalFileSize(16 * 1024 * 1024L);
        S3CloudCacheConfig s3CloudCacheConfig = new S3CloudCacheConfig("textinstance", null, defaluetBucketConfig);
        //创建Instance
        S3CloudCacheInstance textInstance = new S3CloudCacheInstance(s3Client, s3CloudCacheConfig);
        textInstance.start();
        //获取特定的Bucket高效写入
        BucketWriterWriter textBucket = textInstance.getBucketWriterInstance("textbucket");

        for (int i = 0; i < 100; i++) {
            String value = "2026-07-11 16:42:18.726 INFO [WAL-PRESS-THREAD-0047] c.foreverfzl.cloudcache.wal.BlockUploadManager - traceId=73ac92f0d1e64489b21d56ce29f1765e,dataId=001256,blockKey=65892104572164,threadId=47,seq=1295 | upload minio object success, bucket=cn-local-wal,objectKey=wal/00000000000000001024/47,blockSize=536byte,useTime=18ms,enableHeadCheck=true | msg=wal block async upload finished, cache meta updated, pending flush count=32,current memory hold bytes=12582912,free os memory=2145896448";
            WriteResult write = textBucket.write(value.getBytes(StandardCharsets.UTF_8));
            if (write.isSuccess()) {
                System.out.println(write.getS3Key());
                System.out.println(write.getOffset());
                System.out.println(write.getSize());
            }
        }
        textInstance.close(10000, 10000);

//        //===========================
//        // 开始高频写入测试
//        //===========================
//        int threadCount = 100;
//        int writeCountPerThread = 600;
//        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
//        CountDownLatch latch = new CountDownLatch(threadCount);
//        long start = System.currentTimeMillis();
//        for (int i = 0; i < threadCount; i++) {
//            int threadId = i;
//            executor.submit(() -> {
//                try {
//                    for (int j = 0; j < writeCountPerThread; j++) {
////                        String value = "2026-07-01 15:30:22.156 INFO [thread-0005] ForeverCloudCache - data_id:000089 | msg:thread-0005_data-000089_ForeverCloudCache | traceId:9f2e78d103ac4211" + threadId + "_" + j;
//                        String value="2026-07-11 16:42:18.726 INFO [WAL-PRESS-THREAD-0047] c.foreverfzl.cloudcache.wal.BlockUploadManager - traceId=73ac92f0d1e64489b21d56ce29f1765e,dataId=001256,blockKey=65892104572164,threadId=47,seq=1295 | upload minio object success, bucket=cn-local-wal,objectKey=wal/00000000000000001024/47,blockSize=536byte,useTime=18ms,enableHeadCheck=true | msg=wal block async upload finished, cache meta updated, pending flush count=32,current memory hold bytes=12582912,free os memory=2145896448";
//                        WriteResult result = textBucket.write(value.getBytes(StandardCharsets.UTF_8));
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                } finally {
//                    latch.countDown();
//                }
//            });
//        }
//        try {
//            latch.await();
//        } catch (Exception e) {
//            Thread.currentThread().interrupt();
//        }
//        long cost = System.currentTimeMillis() - start;
//        long total = (long) threadCount * writeCountPerThread;
//        System.out.println("==============================");
//        System.out.println("total write=" + total);
//        System.out.println("cost=" + cost + " ms");
//        System.out.println("qps=" + (total * 1000 / cost));
//        textInstance.close(10000,10000);
//        executor.shutdownNow();

    }

}
