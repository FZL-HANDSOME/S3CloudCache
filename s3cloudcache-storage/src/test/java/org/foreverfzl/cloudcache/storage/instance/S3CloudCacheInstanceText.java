package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.storage.factory.S3ClientFactory;
import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterWriter;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.WriteResult;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockSizeLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockUploadConcurrencyLevel;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.zip.CRC32;

public class S3CloudCacheInstanceText {

    private static final Logger log = LoggerFactory.getLogger("Text");

    static void main() throws InterruptedException {
//        text();
//        highConcurrencyTest();
        dataIntegrityTest();
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
        s3CloudCacheConfig.blockMaxIdleTime = 5000;
        //创建Instance
        S3CloudCacheInstance textInstance = new S3CloudCacheInstance(s3Client, s3CloudCacheConfig);
        textInstance.start();
        //获取特定的Bucket高效写入
        BucketWriterWriter bucketWriter = textInstance.getBucketWriterInstance("textbucket");

        for (int i = 0; i < 4; i++) {
            String value = "2026-07-11 16:42:18.726 INFO [WAL-PRESS-THREAD-0047] c.foreverfzl.cloudcache.wal.BlockUploadManager - traceId=73ac92f0d1e64489b21d56ce29f1765e,dataId=001256,blockKey=65892104572164,threadId=47,seq=1295 | upload minio object success, bucket=cn-local-wal,objectKey=wal/00000000000000001024/47,blockSize=536byte,useTime=18ms,enableHeadCheck=true | msg=wal block async upload finished, cache meta updated, pending flush count=32,current memory hold bytes=12582912,free os memory=2145896448";
            CompletableFuture<WriteResult> completableFuture = bucketWriter.write(value.getBytes(StandardCharsets.UTF_8));
            completableFuture.whenComplete((writeResult, throwable) -> {
                if (writeResult.isSuccess()) {
                    log.info("S3key= {} \n;offset= {}\n;size= {}", writeResult.getS3Key(), writeResult.getOffset(), writeResult.getSize());
                }
            });
        }
        Scanner scanner = new Scanner(System.in);
        if (scanner.nextInt() == 1) {

        }

        textInstance.close(15000, 15000, 15000);
    }


    // ========================================================================
    // 高并发测试 + 数据完整性校验
    //
    // 阶段一 — 并发写入：
    //   16 条线程 × 每线程 10000 次写入 = 合计 160000 次写入。
    //   每条数据携带"线程号-序号"前缀，内容完全确定，方便事后定位。
    //   通过 CompletableFuture 回调统计成功/失败，并将
    //   (原始字节[], WriteResult) 对存入 CopyOnWriteArrayList。
    //
    // 阶段二 — S3 回读校验（在 instance.close() 确保全部上传后执行）：
    //   对每条成功写入的记录，用 WriteResult.s3Key + offset + size
    //   发起 HTTP Range 请求，将读回字节与原始字节逐字节比对。
    //   任何不一致都算作 FAIL 并打印首个差异字节的位置和十六进制值。
    //
    // 只有阶段一和阶段二全部通过，才能证明高并发写入数据真正正确。
    // ========================================================================
    public static void highConcurrencyTest() throws InterruptedException {
        S3Client s3Client = S3ClientFactory.createS3Client("http://127.0.0.1:9001", "cn-local", "123456789", "123456789");
        BucketConfig defaluetBucketConfig = new BucketConfig();
        defaluetBucketConfig
                .setBlockSize(BlockSizeLevel.TINY.getBytes())
                .setCacheSize(32 * 1024 * 1024L)
                .setBlockUpLoadCount(BlockUploadConcurrencyLevel.LOW.getConcurrency())
                .setS3KeyPrefix("oreder/phone")
                .setLockMappedFilePageCache(false)
                .setWarmWalFile(true)
                .setWalFileSize(32 * 1024 * 1024L);
        S3CloudCacheConfig s3CloudCacheConfig = new S3CloudCacheConfig("textinstance", null, defaluetBucketConfig);
        s3CloudCacheConfig.blockMaxIdleTime = 5000;
        S3CloudCacheInstance instance = new S3CloudCacheInstance(s3Client, s3CloudCacheConfig);
        instance.start();
        BucketWriterWriter writer = instance.getBucketWriterInstance("textbucket");

        int threadCount = 16;
        int writesPerThread = 10000;
        int total = threadCount * writesPerThread;

        CountDownLatch writeLatch = new CountDownLatch(total);
        LongAdder writeSuccessCount = new LongAdder();
        LongAdder writeFailCount = new LongAdder();

        // 用于收集所有成功写入的 (原始字节, WriteResult) 对，供回读校验
        // 每个元素: Object[]{ byte[] originalData, WriteResult writeResult }
        CopyOnWriteArrayList<Object[]> verifyPairs = new CopyOnWriteArrayList<>();
        // 写入阶段的错误描述（写入失败/Future 异常）
        CopyOnWriteArrayList<String> writeErrors = new CopyOnWriteArrayList<>();

        ExecutorService writeExecutor = Executors.newFixedThreadPool(threadCount);
        log.info("[高并发测试] ===== 阶段一：并发写入 =====");
        log.info("[高并发测试] 线程数={}, 每线程写入={}, 合计={}", threadCount, writesPerThread, total);

        for (int i = 0; i < threadCount; i++) {
            int threadId = i;
            writeExecutor.submit(() -> {
                for (int j = 0; j < writesPerThread; j++) {
                    // 构造内容确定、可定位的原始数据
                    byte[] original = ("thread-" + threadId + "-seq-" + j
                            + "-PAYLOAD-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789").getBytes(StandardCharsets.UTF_8);
                    try {
                        CompletableFuture<WriteResult> future = writer.write(original);
                        int seqJ = j;
                        future.whenComplete((res, thr) -> {
                            if (thr != null) {
                                writeErrors.add("Future异常 thread=" + threadId + " seq=" + seqJ
                                        + " err=" + thr.getMessage());
                                writeFailCount.increment();
                            } else if (res == null || !res.isSuccess()) {
                                writeErrors.add("写入失败 thread=" + threadId + " seq=" + seqJ
                                        + " result=" + res);
                                writeFailCount.increment();
                            } else {
                                // 写入成功：保存原始数据和 WriteResult 供后续校验
                                verifyPairs.add(new Object[]{original, res});
                                writeSuccessCount.increment();
                            }
                            writeLatch.countDown();
                        });
                    } catch (Exception e) {
                        writeErrors.add("write()异常 thread=" + threadId + " seq=" + j
                                + " err=" + e.getMessage());
                        writeFailCount.increment();
                        writeLatch.countDown();
                    }
                }
            });
        }

        // 等待全部 Future 回调完成（最多 5 分钟，因为数据量大）
        boolean writeCompleted = writeLatch.await(300, java.util.concurrent.TimeUnit.SECONDS);
        writeExecutor.shutdown();

        log.info("[高并发测试] 阶段一结束：成功={}, 失败={}, latch超时={}",
                writeSuccessCount.sum(), writeFailCount.sum(), !writeCompleted);
        if (!writeErrors.isEmpty()) {
            log.error("[高并发测试] 写入阶段共 {} 个错误，前10条：", writeErrors.size());
            writeErrors.stream().limit(10).forEach(e -> log.error("  WRITE-ERR: {}", e));
        }

        // 关闭 instance，确保所有 Block 全部上传到 S3 后再做回读校验
        log.info("[高并发测试] 正在关闭 instance，等待全部 Block 上传至 S3...");
        instance.close(30000, 30000, 30000);
        log.info("[高并发测试] instance 已关闭，共 {} 条记录待校验", verifyPairs.size());

        // =====================================================================
        // 阶段二：S3 回读逐字节校验
        // 使用多线程并发从 S3 读回数据，提升校验速度
        // =====================================================================
        log.info("[高并发测试] ===== 阶段二：S3 回读校验 =====");

        int verifyThreadCount = Math.min(32, Runtime.getRuntime().availableProcessors() * 2);
        ExecutorService verifyExecutor = Executors.newFixedThreadPool(verifyThreadCount);
        CountDownLatch verifyLatch = new CountDownLatch(verifyPairs.size());

        LongAdder verifyPass = new LongAdder();
        LongAdder verifyFail = new LongAdder();
        // 存储校验失败的详情（最多记录前 50 条，避免日志过多）
        CopyOnWriteArrayList<String> verifyErrors = new CopyOnWriteArrayList<>();

        for (Object[] pair : verifyPairs) {
            byte[] original = (byte[]) pair[0];
            WriteResult wr = (WriteResult) pair[1];

            verifyExecutor.submit(() -> {
                try {
                    String s3Key = wr.getS3Key();
                    long offset = wr.getOffset();
                    int size = wr.getSize();

                    // ① 首先校验 size 是否与原始数据长度一致
                    if (size != original.length) {
                        if (verifyErrors.size() < 50) {
                            verifyErrors.add("size不匹配 s3Key=" + s3Key + " offset=" + offset
                                    + " expectedLen=" + original.length + " size=" + size);
                        }
                        verifyFail.increment();
                        return;
                    }

                    // ② 通过 HTTP Range 从 S3 精确读取该数据片段
                    String range = "bytes=" + offset + "-" + (offset + size - 1);
                    GetObjectRequest getReq = GetObjectRequest.builder()
                            .bucket("textbucket")   // ← MinIO 存放 Block 的 bucket，按实际修改
                            .key(s3Key)
                            .range(range)
                            .build();

                    try (ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(getReq)) {
                        byte[] readBack = stream.readAllBytes();

                        // ③ 逐字节比对
                        if (Arrays.equals(original, readBack)) {
                            verifyPass.increment();
                        } else {
                            // 找到第一个不同的字节位置
                            int diffPos = -1;
                            int cmpLen = Math.min(original.length, readBack.length);
                            for (int k = 0; k < cmpLen; k++) {
                                if (original[k] != readBack[k]) {
                                    diffPos = k;
                                    break;
                                }
                            }
                            if (verifyErrors.size() < 50) {
                                verifyErrors.add(
                                        "FAIL s3Key=" + s3Key + " offset=" + offset + " size=" + size
                                                + " readBackLen=" + readBack.length
                                                + (diffPos >= 0
                                                ? " 首差异位=" + diffPos
                                                  + " 期望=0x" + Integer.toHexString(original[diffPos] & 0xFF)
                                                  + " 实际=0x" + Integer.toHexString(readBack[diffPos] & 0xFF)
                                                : " 长度不一致"));
                            }
                            verifyFail.increment();
                        }
                    }
                } catch (IOException e) {
                    if (verifyErrors.size() < 50) {
                        verifyErrors.add("S3读取异常 s3Key=" + wr.getS3Key()
                                + " offset=" + wr.getOffset() + " err=" + e.getMessage());
                    }
                    verifyFail.increment();
                } catch (Exception e) {
                    if (verifyErrors.size() < 50) {
                        verifyErrors.add("校验异常 s3Key=" + wr.getS3Key() + " err=" + e.getMessage());
                    }
                    verifyFail.increment();
                } finally {
                    verifyLatch.countDown();
                }
            });
        }

        // 等待全部校验任务完成（最多 10 分钟）
        boolean verifyCompleted = verifyLatch.await(600, java.util.concurrent.TimeUnit.SECONDS);
        verifyExecutor.shutdown();

        // =====================================================================
        // 最终汇总报告
        // =====================================================================
        log.info("[高并发测试] ===== 最终汇总 =====");
        log.info("[高并发测试] 阶段一写入：总计={}, 成功={}, 失败={}",
                total, writeSuccessCount.sum(), writeFailCount.sum());
        log.info("[高并发测试] 阶段二校验：待校验={}, PASS={}, FAIL={}, latch超时={}",
                verifyPairs.size(), verifyPass.sum(), verifyFail.sum(), !verifyCompleted);

        if (!verifyErrors.isEmpty()) {
            log.error("[高并发测试] 检测到 {} 条数据不一致（最多展示前50条）：", verifyErrors.size());
            verifyErrors.forEach(e -> log.error("  DATA-ERR: {}", e));
        }

        boolean allPassed = writeErrors.isEmpty()
                && verifyFail.sum() == 0
                && writeCompleted
                && verifyCompleted;
        if (allPassed) {
            log.info("[高并发测试] ✓ ALL PASSED：所有数据写入成功且与 S3 读回完全一致");
        } else {
            log.error("[高并发测试] ✗ FAILED：存在问题，请检查上方错误日志");
        }
    }


    // ========================================================================
    // 数据完整性测试
    // 目标：验证用户写入的原始字节与上传到 S3 后读回的字节逐字节一致，缺少 1 字节都不行
    // 策略：
    //   1. 写入若干条已知内容的数据（可区分的字节序列），等待 Future 完成拿到 WriteResult
    //   2. 利用 WriteResult.s3Key + offset + size 向 S3 GetObject，
    //      使用 range 请求（bytes=offset-(offset+size-1)）精确读取该数据片段
    //   3. 逐字节比对读回数据与原始数据；任何不一致都记录错误
    // ========================================================================
    public static void dataIntegrityTest() throws InterruptedException {
        //创建S3Client(String endpoint, String region, String accessKey, String secretKey)
        S3Client s3Client = S3ClientFactory.createS3Client("http://127.0.0.1:9001", "cn-local", "123456789", "123456789");
        //创建全局配置文件
        BucketConfig defaluetBucketConfig = new BucketConfig();
        defaluetBucketConfig
                .setBlockSize(BlockSizeLevel.TINY.getBytes())
                .setCacheSize(32 * 1024 * 1024L)
                .setBlockUpLoadCount(BlockUploadConcurrencyLevel.NORMAL.getConcurrency())
                .setS3KeyPrefix("oreder/phone")
                .setLockMappedFilePageCache(false)
                .setWarmWalFile(true)
                .setWalFileSize(32 * 1024 * 1024L);
        S3CloudCacheConfig s3CloudCacheConfig = new S3CloudCacheConfig("textinstance", null, defaluetBucketConfig);
        s3CloudCacheConfig.blockMaxIdleTime = 15000;
        //创建Instance
        S3CloudCacheInstance instance = new S3CloudCacheInstance(s3Client, s3CloudCacheConfig);
        instance.start();
        //获取特定的Bucket高效写入
        BucketWriterWriter writer = instance.getBucketWriterInstance("textbucket");

        // 准备写入的原始数据列表，每条数据独立可区分
        int writeCount = 1000;
        List<byte[]> originalDataList = new ArrayList<>(writeCount);
        for (int i = 0; i < writeCount; i++) {
            // 构造包含序号的可识别内容，长度各不相同以覆盖边界情况
            String content = String.format("[record-%03d] 数据完整性测试 ABCDEF0123456789 "
                    + "verifyBytes=%d 末尾填充:", i, i * 17);
            // 追加一段重复字节使得每条数据有不同长度（64 ~ 64+writeCount*7 字节）
            byte[] padding = new byte[i * 7];
            Arrays.fill(padding, (byte) (i % 127));
            byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
            byte[] data = new byte[contentBytes.length + padding.length];
            System.arraycopy(contentBytes, 0, data, 0, contentBytes.length);
            System.arraycopy(padding, 0, data, contentBytes.length, padding.length);
            originalDataList.add(data);
        }

        // 并发写入，等待所有 Future 完成
        CountDownLatch latch = new CountDownLatch(writeCount);
        // 用线程安全 List 保存 (originalData, WriteResult) 对
        CopyOnWriteArrayList<Object[]> resultPairs = new CopyOnWriteArrayList<>();
        AtomicInteger writeFailCount = new AtomicInteger(0);

        log.info("[数据完整性测试] 开始写入 {} 条数据", writeCount);
        for (int i = 0; i < writeCount; i++) {
            byte[] original = originalDataList.get(i);
            CompletableFuture<WriteResult> future = writer.write(original);
            future.whenComplete((res, thr) -> {
                if (thr != null || res == null || !res.isSuccess()) {
                    log.error("[数据完整性测试] 写入失败: res={}, thr={}", res, thr);
                    writeFailCount.incrementAndGet();
                } else {
                    // 保存原始数据 + WriteResult，供后续从 S3 读回校验
                    resultPairs.add(new Object[]{original, res});
                }
                latch.countDown();
            });
        }

        // 等待全部写入完成（含上传到 S3）
        boolean completed = latch.await(120, java.util.concurrent.TimeUnit.SECONDS);
        // 关闭 instance，确保所有 block 都已上传
        instance.close(15000, 15000, 15000);

        if (!completed) {
            log.error("[数据完整性测试] 等待超时，部分 Future 未完成");
        }
        log.info("[数据完整性测试] 写入阶段结束：成功={}, 失败={}",
                resultPairs.size(), writeFailCount.get());

        // ---- 读回阶段：从 S3 按 offset+size 精确读取并逐字节对比 ----
        int passCount = 0;
        int failCount = 0;
        for (Object[] pair : resultPairs) {
            byte[] original = (byte[]) pair[0];
            WriteResult wr = (WriteResult) pair[1];
            String s3Key = wr.getS3Key();
            long offset = wr.getOffset();
            int size = wr.getSize();

            // 校验 size 是否与写入长度一致
            if (size != original.length) {
                log.error("[数据完整性测试] size不匹配: s3Key={} offset={} "
                        + "expectedSize={} actualSize={}", s3Key, offset, original.length, size);
                failCount++;
                continue;
            }

            // 使用 HTTP Range 请求精确读取：bytes=offset-(offset+size-1)
            String range = "bytes=" + offset + "-" + (offset + size - 1);
            GetObjectRequest getReq = GetObjectRequest.builder()
                    .bucket("textbucket")   // MinIO bucket name（存放 block 文件的桶）
                    .key(s3Key)
                    .range(range)
                    .build();

            try (ResponseInputStream<GetObjectResponse> s3Stream = s3Client.getObject(getReq)) {
                byte[] readBack = s3Stream.readAllBytes();
                if (readBack.length != size) {
                    log.error("[数据完整性测试] 读回字节数不匹配: s3Key={} offset={} "
                            + "expectedSize={} readBackSize={}", s3Key, offset, size, readBack.length);
                    failCount++;
                    continue;
                }
                // 逐字节比对
                boolean match = Arrays.equals(original, readBack);
                if (match) {
                    passCount++;
                    log.debug("[数据完整性测试] PASS: s3Key={} offset={} size={}", s3Key, offset, size);
                } else {
                    failCount++;
                    // 找到第一个不同的字节位置，便于定位问题
                    int diffPos = -1;
                    for (int i = 0; i < original.length; i++) {
                        if (original[i] != readBack[i]) {
                            diffPos = i;
                            break;
                        }
                    }
                    log.error("[数据完整性测试] FAIL：数据不一致! s3Key={} offset={} size={} "
                                    + "首个差异字节位置={} 期望=0x{} 实际=0x{}",
                            s3Key, offset, size, diffPos,
                            Integer.toHexString(original[diffPos] & 0xFF),
                            Integer.toHexString(readBack[diffPos] & 0xFF));
                }
            } catch (IOException e) {
                log.error("[数据完整性测试] 从S3读取数据失败: s3Key={} offset={} size={} err={}",
                        s3Key, offset, size, e.getMessage());
                failCount++;
            }
        }

        log.info("[数据完整性测试] 最终结果：PASS={}, FAIL={}, 写入失败={}",
                passCount, failCount, writeFailCount.get());
        if (failCount == 0 && writeFailCount.get() == 0) {
            log.info("[数据完整性测试] ALL PASSED：写入数据与S3数据完全一致");
        } else {
            log.error("[数据完整性测试] 存在不一致，请检查上方错误日志");
        }
    }
}
