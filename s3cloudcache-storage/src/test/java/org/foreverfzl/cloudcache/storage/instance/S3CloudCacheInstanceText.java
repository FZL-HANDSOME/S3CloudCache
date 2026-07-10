package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.storage.factory.S3ClientFactory;
import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterWriter;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.*;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
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
                .setCacheSize(32 * 1024 * 1024L)
                .setBlockUpLoadCount(BlockUploadConcurrencyLevel.LOW.getConcurrency())
                .setPageFlushLevel(PageFlushLevel.NORMAL_20_MS.getFlushIntervalMs())
                .setS3KeyPrefix("oreder/phone")
                .setLockMappedFilePageCache(false)
                .setWarmWalFile(true)
                .setWalFileSize(32L * 1024 * 1024);
        S3CloudCacheConfig s3CloudCacheConfig = new S3CloudCacheConfig("textinstance", null, defaluetBucketConfig);
        //创建Instance
        S3CloudCacheInstance textInstance = new S3CloudCacheInstance(s3Client, s3CloudCacheConfig);
        //获取特定的Bucket高效写入
        BucketWriterWriter textBucket = textInstance.getBucketWriterInstance("textbucket");


        //===========================
        // 开始高频写入测试
        //===========================

        int threadCount = 80;
        int writeCountPerThread = 1967;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < writeCountPerThread; j++) {
                        String value = "2026-07-01 15:30:22.156 INFO [thread-0005] ForeverCloudCache - data_id:000089 | msg:thread-0005_data-000089_ForeverCloudCache | traceId:9f2e78d103ac4211" + threadId + "_" + j;
                        WriteResult result = textBucket.write(value.getBytes(StandardCharsets.UTF_8));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (Exception e) {
            Thread.currentThread().interrupt();
        }
        long cost = System.currentTimeMillis() - start;
        long total = (long) threadCount * writeCountPerThread;
        System.out.println("==============================");
        System.out.println("total write=" + total);
        System.out.println("cost=" + cost + " ms");
        System.out.println("qps=" + (total * 1000 / cost));
        try {
            Thread.sleep(600000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        executor.shutdown();
        textBucket.close();


    }

}
