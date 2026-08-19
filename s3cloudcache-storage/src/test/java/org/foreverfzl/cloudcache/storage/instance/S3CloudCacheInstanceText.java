package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.storage.factory.S3ClientFactory;
import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterWriter;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockSizeLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockUploadConcurrencyLevel;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.zip.CRC32;

public class S3CloudCacheInstanceText {

    static void main() {
        text();
//        exampleText();
//        verifyWalDataIntegrity();
    }


    //测试WAL持久化是否正常工作
    private static void text() {
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
        s3CloudCacheConfig.blockMaxIdleTime = 4000;
        //创建Instance
        S3CloudCacheInstance textInstance = new S3CloudCacheInstance(s3Client, s3CloudCacheConfig);
        textInstance.start();
        //获取特定的Bucket高效写入
        BucketWriterWriter bucketWriter = textInstance.getBucketWriterInstance("textbucket");

//        for (int i = 0; i < 2; i++) {
//            String value = "2026-07-11 16:42:18.726 INFO [WAL-PRESS-THREAD-0047] c.foreverfzl.cloudcache.wal.BlockUploadManager - traceId=73ac92f0d1e64489b21d56ce29f1765e,dataId=001256,blockKey=65892104572164,threadId=47,seq=1295 | upload minio object success, bucket=cn-local-wal,objectKey=wal/00000000000000001024/47,blockSize=536byte,useTime=18ms,enableHeadCheck=true | msg=wal block async upload finished, cache meta updated, pending flush count=32,current memory hold bytes=12582912,free os memory=2145896448";
//            WriteResult result = bucketWriter.write(value.getBytes(StandardCharsets.UTF_8));
//            if (result.isSuccess()) {
//                System.out.println(result.getS3Key());
//                System.out.println(result.getOffset());
//                System.out.println(result.getSize());
//            }
//        }
        Scanner scanner = new Scanner(System.in);
        if (scanner.nextInt() == 1) {

        }


//        for (int i = 0; i < 2; i++) {
//            System.out.println("输入数据");
//            String s = scanner.nextLine();
//            WriteResult result = bucketWriter.write(s.getBytes(StandardCharsets.UTF_8));
//            if (result.isSuccess()) {
//                System.out.println(result.getS3Key());
//                System.out.println(result.getOffset());
//                System.out.println(result.getSize());
//            }
//        }
//        System.out.println("请输入数字");
//        if (scanner.nextInt() == 1) {
//
//        }
        textInstance.close(15000, 15000);
    }


    /**
     * 高并发写入压测。
     *
     * <p>WAL 吞吐量统计的是所有成功 write 的 Value Bytes / 并发写入耗时；
     * Block 上传吞吐量统计的是关闭阶段将未封口 Block 上传至 S3 的 Value Bytes / 耗时。</p>
     * exampleText()方法
     * 1：16 个并发线程、每线程 400 次写入、每条 1KB；
     * 2：输出成功/失败写入数、成功写入的 Value 字节数；
     * 3：输出 WAL 写入吞吐量；
     * 4：关闭实例时封口并上传 Block，输出关闭阶段 Block 上传吞吐量。
     * 5：通过较长的 blockMaxIdleTime 避免压测过程中提前封口，保证上传吞吐量主要统计关闭阶段的上传。
     */
    public static void exampleText() {
        final int threadCount = 16;
        final int writesPerThread = 400;
        final int payloadSize = 1024;
        final long totalWrites = (long) threadCount * writesPerThread;

        BucketConfig bucketConfig = new BucketConfig()
                .setBlockSize(BlockSizeLevel.SMALL.getBytes())
                .setCacheSize(16 * 1024 * 1024L)
                .setBlockUpLoadCount(BlockUploadConcurrencyLevel.HIGH.getConcurrency())
                .setS3KeyPrefix("order/performance")
                .setLockMappedFilePageCache(false)
                .setWarmWalFile(true)
                .setWalFileSize(16 * 1024 * 1024L);
        S3CloudCacheConfig config = new S3CloudCacheConfig("textinstance", null, bucketConfig);
        // 避免压测期间因短暂空闲提前封口，使关闭阶段的上传吞吐量可测量。
        config.blockMaxIdleTime = 10 * 60 * 1000;
        S3Client s3Client = S3ClientFactory.createS3Client(
                "http://127.0.0.1:9001", "cn-local", "123456789", "123456789");
        S3CloudCacheInstance instance = new S3CloudCacheInstance(s3Client, config);
        instance.start();
        BucketWriterWriter bucketWriter = instance.getBucketWriterInstance("textbucket");
        ExecutorService writerExecutor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(threadCount);
        LongAdder successBytes = new LongAdder();
        LongAdder failedWrites = new LongAdder();

        for (int threadIndex = 0; threadIndex < threadCount; threadIndex++) {
            final int currentThread = threadIndex;
            writerExecutor.execute(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int writeIndex = 0; writeIndex < writesPerThread; writeIndex++) {
                        int recordId = currentThread * writesPerThread + writeIndex;
                        byte[] payload = createPayload(recordId, payloadSize);
                        WriteResult result = bucketWriter.write(payload);
                        if (result != null && result.isSuccess()) {
                            successBytes.add(payload.length);
                        } else {
                            failedWrites.increment();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            });
        }
        try {
            ready.await();
            long walStartNanos = System.nanoTime();
            start.countDown();
            finished.await();
            long walElapsedNanos = System.nanoTime() - walStartNanos;
            writerExecutor.shutdown();

            long uploadStartNanos = System.nanoTime();
            instance.close(60_000, 60_000);
            long uploadElapsedNanos = System.nanoTime() - uploadStartNanos;

            long writtenBytes = successBytes.sum();
            System.out.printf("realText: requested=%d, failed=%d, successBytes=%d%n",
                    totalWrites, failedWrites.sum(), writtenBytes);
            System.out.printf("WAL write throughput: %.2f MiB/s%n",
                    bytesPerSecond(writtenBytes, walElapsedNanos) / (1024.0 * 1024.0));
            System.out.printf("Block upload drain throughput: %.2f MiB/s%n",
                    bytesPerSecond(writtenBytes, uploadElapsedNanos) / (1024.0 * 1024.0));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("realText was interrupted", e);
        } finally {
            writerExecutor.shutdownNow();
        }
    }


    /**
     * 写入带唯一编号的数据后直接扫描 WAL，校验记录数、字节数、CRC32 和内容，确认没有丢失或串写。
     * verifyWalDataIntegrity()方法：
     * 1：8 个并发线程写入带唯一 ID 的固定长度记录；
     * 2：直接扫描活跃 WAL 文件；
     * 3：校验每条记录的 Magic、Value 长度、CRC32、完整 Value 内容；
     * 4：检查重复记录、缺失记录与 Value 总字节数；
     * 5：任一不一致会抛出 AssertionError，全部一致会打印通过记录数和字节数。
     */
    public static void verifyWalDataIntegrity() {
        final int threadCount = 8;
        final int writesPerThread = 500;
        final int payloadSize = 256;
        final int totalWrites = threadCount * writesPerThread;

        BucketConfig bucketConfig = new BucketConfig()
                .setBlockSize(BlockSizeLevel.SMALL.getBytes())
                .setCacheSize(16 * 1024 * 1024L)
                .setBlockUpLoadCount(BlockUploadConcurrencyLevel.LOW.getConcurrency())
                .setS3KeyPrefix("order/integrity")
                .setLockMappedFilePageCache(false)
                .setWarmWalFile(true)
                .setWalFileSize(16 * 1024 * 1024L);
        S3CloudCacheConfig config = new S3CloudCacheConfig("textinstance", null, bucketConfig);
        config.blockMaxIdleTime = 10 * 60 * 1000;
        S3Client s3Client = S3ClientFactory.createS3Client(
                "http://127.0.0.1:9001", "cn-local", "123456789", "123456789");
        S3CloudCacheInstance instance = new S3CloudCacheInstance(s3Client, config);
        instance.start();
        BucketWriterWriter bucketWriter = instance.getBucketWriterInstance("textbucket");
        ExecutorService writerExecutor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(threadCount);
        AtomicIntegerArray expectedRecords = new AtomicIntegerArray(totalWrites);

        try {
            for (int threadIndex = 0; threadIndex < threadCount; threadIndex++) {
                final int currentThread = threadIndex;
                writerExecutor.execute(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        for (int writeIndex = 0; writeIndex < writesPerThread; writeIndex++) {
                            int recordId = currentThread * writesPerThread + writeIndex;
                            WriteResult result = bucketWriter.write(createPayload(recordId, payloadSize));
                            if (result != null && result.isSuccess()) {
                                expectedRecords.set(recordId, 1);
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        finished.countDown();
                    }
                });
            }

            ready.await();
            start.countDown();
            finished.await();
            writerExecutor.shutdown();
            MappedFileManager mappedFileManager = bucketWriter.getMappedManager();
            DefaultMappedFile walFile = mappedFileManager.getActiveMappedFile().get();
            verifyWalRecords(walFile, expectedRecords, payloadSize);
            System.out.printf("WAL integrity check passed: successfulRecords=%d, valueBytes=%d%n",
                    countExpectedRecords(expectedRecords),
                    (long) countExpectedRecords(expectedRecords) * payloadSize);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("WAL integrity test was interrupted", e);
        } finally {
            writerExecutor.shutdownNow();
            instance.close(60_000, 60_000);
        }
    }

    private static void verifyWalRecords(DefaultMappedFile walFile, AtomicIntegerArray expectedRecords, int payloadSize) {
        if (walFile == null) {
            throw new AssertionError("Active WAL file is missing");
        }

        AtomicIntegerArray actualRecords = new AtomicIntegerArray(expectedRecords.length());
        long position = 0;
        long valueBytes = 0;
        CRC32 crc32 = new CRC32();
        while (position < walFile.wrotePosition) {
            int magic = walFile.getIntFromDataArea(position);
            if (magic != DataStruct.MAGIC_NUMBER) {
                throw new AssertionError("Invalid WAL magic at logical offset " + position);
            }
            int expectedChecksum = walFile.getIntFromDataArea(position + Integer.BYTES);
            int valueLength = walFile.getIntFromDataArea(position + Integer.BYTES * 2L);
            if (valueLength != payloadSize) {
                throw new AssertionError("Unexpected value length " + valueLength + " at logical offset " + position);
            }

            byte[] value = walFile.getOrgDataFromDataArea(position + DataStruct.HEADER_LENGTH, valueLength);
            crc32.reset();
            crc32.update(value);
            if ((int) crc32.getValue() != expectedChecksum) {
                throw new AssertionError("Checksum mismatch at logical offset " + position);
            }

            int recordId = ByteBuffer.wrap(value).getInt();
            if (recordId < 0 || recordId >= expectedRecords.length() || expectedRecords.get(recordId) == 0) {
                throw new AssertionError("Unexpected record id " + recordId + " in WAL");
            }
            if (!Arrays.equals(value, createPayload(recordId, payloadSize))) {
                throw new AssertionError("Value bytes were changed for record id " + recordId);
            }
            if (actualRecords.getAndIncrement(recordId) != 0) {
                throw new AssertionError("Duplicate WAL record id " + recordId);
            }

            valueBytes += valueLength;
            position += alignedRecordSize(valueLength);
        }

        if (position != walFile.wrotePosition) {
            throw new AssertionError("WAL scan ended at " + position + ", wrotePosition is " + walFile.wrotePosition);
        }
        for (int recordId = 0; recordId < expectedRecords.length(); recordId++) {
            if (expectedRecords.get(recordId) != actualRecords.get(recordId)) {
                throw new AssertionError("Missing WAL record id " + recordId);
            }
        }
        if (valueBytes != (long) countExpectedRecords(expectedRecords) * payloadSize) {
            throw new AssertionError("WAL value byte count is inconsistent: " + valueBytes);
        }
    }

    private static byte[] createPayload(int recordId, int payloadSize) {
        ByteBuffer buffer = ByteBuffer.allocate(payloadSize);
        buffer.putInt(recordId);
        while (buffer.hasRemaining()) {
            buffer.put((byte) (recordId * 31 + buffer.position()));
        }
        return buffer.array();
    }

    private static long alignedRecordSize(int valueLength) {
        long size = DataStruct.HEADER_LENGTH + valueLength;
        return (size + 3) & ~3L;
    }

    private static int countExpectedRecords(AtomicIntegerArray expectedRecords) {
        int count = 0;
        for (int index = 0; index < expectedRecords.length(); index++) {
            count += expectedRecords.get(index);
        }
        return count;
    }

    private static double bytesPerSecond(long bytes, long elapsedNanos) {
        return elapsedNanos == 0 ? 0.0 : bytes * 1_000_000_000.0 / elapsedNanos;
    }


}
