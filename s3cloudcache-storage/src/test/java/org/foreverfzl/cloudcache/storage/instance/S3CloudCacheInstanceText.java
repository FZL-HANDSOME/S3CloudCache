package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.storage.factory.S3ClientFactory;
import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterInstance;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.*;
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
        S3Client s3Client = S3ClientFactory.createS3Client("http://127.0.0.1:9000", "cn-local", "fzl20050511", "fzl200505011");
        //创建全局配置文件
        BucketConfig defaluetBucketConfig = new BucketConfig();
        defaluetBucketConfig
                .setBlockSize(BlockSizeLevel.SMALL.getBytes())
                .setCacheSize(CacheSizeLevel.TINE.getBytes())
                .setBlockUpLoadCount(BlockUploadConcurrencyLevel.LOW.getConcurrency())
                .setPageFlushLevel(PageFlushLevel.NORMAL_20_MS.getFlushIntervalMs())
                .setS3KeyPrefix("text")
                .setLockMappedFilePageCache(false)
                .setWarmWalFile(false)
                .setWalFileSize(32L * 1024 * 1024);
        S3CloudCacheConfig s3CloudCacheConfig = new S3CloudCacheConfig("textInstance", null, defaluetBucketConfig);
        //创建Instance
        S3CloudCacheInstance textInstance = new S3CloudCacheInstance(s3Client, s3CloudCacheConfig);
        //获取特定的Bucket高效写入
        BucketWriterInstance textBucket = textInstance.getBucketWriterInstance("textBucket");


        //===========================
        // 开始高频写入测试
        //===========================

        int threadCount = 16;
        int writeCountPerThread = 100000 ;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < writeCountPerThread; j++) {
                        String value = "thread-" + threadId + "_data_" + j + "_ForeverCloudCache";
                        WriteResult result = textBucket.write(value.getBytes(StandardCharsets.UTF_8));
                        if (j % 10000 == 0) {
                            System.out.println("thread=" + threadId + " write=" + j);
                        }
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
        executor.shutdown();
        System.out.println("未关闭的线程——————————————————");
        Thread.getAllStackTraces()
                .keySet()
                .forEach(
                        t -> System.out.println(t.getName())
                );
    }

}
