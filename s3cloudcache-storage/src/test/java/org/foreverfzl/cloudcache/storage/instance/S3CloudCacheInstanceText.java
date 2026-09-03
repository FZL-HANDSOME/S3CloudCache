package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.storage.factory.S3ClientFactory;
import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterWriter;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class S3CloudCacheInstanceText {

    private static final Logger log = LoggerFactory.getLogger("Text");

    // ===== 测试公共配置（与 MinIO 环境保持一致）=====
    private static final String ENDPOINT = "http://127.0.0.1:9001";
    private static final String REGION = "cn-local";
    private static final String ACCESS_KEY = "123456789";
    private static final String SECRET_KEY = "123456789";
    private static final String BUCKET_NAME = "textbucket";
    private static final String S3_KEY_PREFIX = "oreder/phone";

    /**
     * 测试失败统一出口：打印错误并抛出 AssertionError，让测试真正"失败"，而不是只打日志后假装通过。
     */
    private static void fail(String message) {
        log.error(message);
        throw new AssertionError(message);
    }

    public static void main(String[] args) throws Exception {
//        text();
//        highConcurrencyTest();
//        dataIntegrityTest();
        offHeapDataIntegrityTest();
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
            CompletableFuture<WriteResult> completableFuture = bucketWriter.writeHeapData(value.getBytes(StandardCharsets.UTF_8));
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
    //
    // 阶段二 — S3 回读校验（在 instance.close() 确保全部上传后执行）：
    //   对每条成功写入的记录，用 WriteResult.s3Key + offset + size
    //   发起 HTTP Range 请求，将读回字节与原始字节逐字节比对。
    //
    // 判定规则（全部满足才算通过，任一不满足即抛 AssertionError）：
    //   1. 写入阶段必须在超时内全部完成（不能靠 close() 兜底掩盖"Block 卡住不封口上传"这类 bug）；
    //   2. 写入失败数为 0，且成功数 == total；
    //   3. 回读校验阶段必须全部完成，FAIL 数为 0，且 PASS 数 == total。
    // ========================================================================
    public static void highConcurrencyTest() throws Exception {
        S3Client instanceClient = S3ClientFactory.createS3Client(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY);
        BucketConfig defaultBucketConfig = new BucketConfig();
        defaultBucketConfig
                .setBlockSize(BlockSizeLevel.TINY.getBytes())
                .setCacheSize(32 * 1024 * 1024L)
                .setBlockUpLoadCount(BlockUploadConcurrencyLevel.LOW.getConcurrency())
                .setS3KeyPrefix(S3_KEY_PREFIX)
                .setLockMappedFilePageCache(false)
                .setWarmWalFile(true)
                .setWalFileSize(32 * 1024 * 1024L);
        S3CloudCacheConfig config = new S3CloudCacheConfig("textinstance-concurrency", null, defaultBucketConfig);
        config.blockMaxIdleTime = 5000;
        S3CloudCacheInstance instance = new S3CloudCacheInstance(instanceClient, config);
        instance.start();
        BucketWriterWriter writer = instance.getBucketWriterInstance(BUCKET_NAME);

        int threadCount = 16;
        int writesPerThread = 10000;
        int total = threadCount * writesPerThread;

        CountDownLatch writeLatch = new CountDownLatch(total);
        LongAdder writeSuccessCount = new LongAdder();
        LongAdder writeFailCount = new LongAdder();
        CopyOnWriteArrayList<Object[]> verifyPairs = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<String> writeErrors = new CopyOnWriteArrayList<>();

        ExecutorService writeExecutor = Executors.newFixedThreadPool(threadCount);
        log.info("[高并发测试] ===== 阶段一：并发写入 =====");
        log.info("[高并发测试] 线程数={}, 每线程写入={}, 合计={}", threadCount, writesPerThread, total);

        boolean writeCompleted;
        try {
            for (int i = 0; i < threadCount; i++) {
                int threadId = i;
                writeExecutor.submit(() -> {
                    for (int j = 0; j < writesPerThread; j++) {
                        // 构造内容确定、可定位的原始数据
                        byte[] original = ("thread-" + threadId + "-seq-" + j
                                + "-PAYLOAD-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789").getBytes(StandardCharsets.UTF_8);
                        try {
                            CompletableFuture<WriteResult> future = writer.writeHeapData(original);
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
            writeCompleted = writeLatch.await(300, TimeUnit.SECONDS);

            log.info("[高并发测试] 阶段一结束：成功={}, 失败={}, latch超时={}",
                    writeSuccessCount.sum(), writeFailCount.sum(), !writeCompleted);
            if (!writeErrors.isEmpty()) {
                log.error("[高并发测试] 写入阶段共 {} 个错误，前10条：", writeErrors.size());
                writeErrors.stream().limit(10).forEach(e -> log.error("  WRITE-ERR: {}", e));
            }

            // 阶段一强制校验
            if (!writeCompleted) {
                fail("[高并发测试] 写入阶段超时：仍有 " + writeLatch.getCount() + " 条 Future 未完成");
            }
            if (!writeErrors.isEmpty() || writeFailCount.sum() != 0) {
                fail("[高并发测试] 写入阶段存在失败：失败数=" + writeFailCount.sum()
                        + ", 错误条数=" + writeErrors.size());
            }
            if (writeSuccessCount.sum() != total || verifyPairs.size() != total) {
                fail("[高并发测试] 写入成功数不匹配：期望=" + total
                        + ", 成功计数=" + writeSuccessCount.sum() + ", 收集对=" + verifyPairs.size());
            }
        } finally {
            writeExecutor.shutdownNow();
            // 无论写阶段是否失败，都关闭 instance：既清理资源，也强制把剩余 Block 上传完。
            // 用 try/catch 包住，避免 close() 抛异常时掩盖阶段一真正的 AssertionError。
            log.info("[高并发测试] 正在关闭 instance，等待全部 Block 上传至 S3...");
            try {
                instance.close(30000, 30000, 30000);
            } catch (Exception closeEx) {
                log.error("[高并发测试] instance.close() 失败", closeEx);
            }
        }
        log.info("[高并发测试] instance 已关闭，共 {} 条记录待校验", verifyPairs.size());

        // =====================================================================
        // 阶段二：S3 回读逐字节校验
        // =====================================================================
        log.info("[高并发测试] ===== 阶段二：S3 回读校验 =====");

        // instance.close() 会关闭 instanceClient，因此回读必须使用独立的 verifyClient
        S3Client verifyClient = S3ClientFactory.createS3Client(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY);
        int verifyThreadCount = Math.min(32, Runtime.getRuntime().availableProcessors() * 2);
        ExecutorService verifyExecutor = Executors.newFixedThreadPool(verifyThreadCount);
        CountDownLatch verifyLatch = new CountDownLatch(verifyPairs.size());
        LongAdder verifyPass = new LongAdder();
        LongAdder verifyFail = new LongAdder();
        CopyOnWriteArrayList<String> verifyErrors = new CopyOnWriteArrayList<>();

        boolean verifyCompleted;
        try {
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
                                .bucket(BUCKET_NAME)
                                .key(s3Key)
                                .range(range)
                                .build();

                        try (ResponseInputStream<GetObjectResponse> stream = verifyClient.getObject(getReq)) {
                            byte[] readBack = stream.readAllBytes();

                            // ③ 逐字节比对
                            if (Arrays.equals(original, readBack)) {
                                verifyPass.increment();
                            } else {
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

            verifyCompleted = verifyLatch.await(600, TimeUnit.SECONDS);

            log.info("[高并发测试] ===== 最终汇总 =====");
            log.info("[高并发测试] 阶段一写入：总计={}, 成功={}, 失败={}",
                    total, writeSuccessCount.sum(), writeFailCount.sum());
            log.info("[高并发测试] 阶段二校验：待校验={}, PASS={}, FAIL={}, latch超时={}",
                    verifyPairs.size(), verifyPass.sum(), verifyFail.sum(), !verifyCompleted);
            if (!verifyErrors.isEmpty()) {
                log.error("[高并发测试] 检测到 {} 条数据不一致（最多展示前50条）：", verifyErrors.size());
                verifyErrors.forEach(e -> log.error("  DATA-ERR: {}", e));
            }

            // 阶段二强制校验
            if (!verifyCompleted) {
                fail("[高并发测试] 回读校验超时：仍有 " + verifyLatch.getCount() + " 条未校验完成");
            }
            if (verifyFail.sum() != 0) {
                fail("[高并发测试] 回读校验失败：FAIL=" + verifyFail.sum());
            }
            if (verifyPass.sum() != total) {
                fail("[高并发测试] 回读校验 PASS 数不匹配：期望=" + total + ", 实际=" + verifyPass.sum());
            }

            log.info("[高并发测试] ✓ ALL PASSED：所有数据写入成功且与 S3 读回完全一致");
        } finally {
            verifyExecutor.shutdownNow();
            verifyClient.close();
        }
    }


    // ========================================================================
    // 数据完整性测试
    // 目标：验证用户写入的原始字节与上传到 S3 后读回的字节逐字节一致，缺少 1 字节都不行
    //
    // 判定规则（全部满足才算通过，任一不满足即抛 AssertionError）：
    //   1. 写入阶段必须在超时内全部完成（不能靠 close() 兜底掩盖"Block 卡住不封口上传"这类 bug）；
    //   2. 写入失败数为 0，且收集到的结果数 == writeCount；
    //   3. 回读阶段 PASS 数 == writeCount 且 FAIL 数为 0。
    // ========================================================================
    public static void dataIntegrityTest() throws Exception {
        S3Client instanceClient = S3ClientFactory.createS3Client(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY);
        BucketConfig defaultBucketConfig = new BucketConfig();
        defaultBucketConfig
                .setBlockSize(BlockSizeLevel.TINY.getBytes())
                .setCacheSize(32 * 1024 * 1024L)
                .setBlockUpLoadCount(BlockUploadConcurrencyLevel.NORMAL.getConcurrency())
                .setS3KeyPrefix(S3_KEY_PREFIX)
                .setLockMappedFilePageCache(false)
                .setWarmWalFile(true)
                .setWalFileSize(32 * 1024 * 1024L);
        S3CloudCacheConfig config = new S3CloudCacheConfig("textinstance-integrity", null, defaultBucketConfig);
        config.blockMaxIdleTime = 15000;
        S3CloudCacheInstance instance = new S3CloudCacheInstance(instanceClient, config);
        instance.start();
        BucketWriterWriter writer = instance.getBucketWriterInstance(BUCKET_NAME);

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

        CountDownLatch writeLatch = new CountDownLatch(writeCount);
        CopyOnWriteArrayList<Object[]> resultPairs = new CopyOnWriteArrayList<>();
        AtomicInteger writeFailCount = new AtomicInteger(0);

        log.info("[数据完整性测试] 开始写入 {} 条数据", writeCount);
        boolean writeCompleted;
        try {
            for (int i = 0; i < writeCount; i++) {
                byte[] original = originalDataList.get(i);
                CompletableFuture<WriteResult> future = writer.writeHeapData(original);
                future.whenComplete((res, thr) -> {
                    if (thr != null || res == null || !res.isSuccess()) {
                        log.error("[数据完整性测试] 写入失败: res={}, thr={}", res, thr);
                        writeFailCount.incrementAndGet();
                    } else {
                        // 保存原始数据 + WriteResult，供后续从 S3 读回校验
                        resultPairs.add(new Object[]{original, res});
                    }
                    writeLatch.countDown();
                });
            }

            // 等待全部写入完成（含上传到 S3）
            writeCompleted = writeLatch.await(120, TimeUnit.SECONDS);

            log.info("[数据完整性测试] 写入阶段结束：成功={}, 失败={}, latch超时={}",
                    resultPairs.size(), writeFailCount.get(), !writeCompleted);

            // 写入阶段强制校验（这是本测试最关键的一环：不能依赖 close() 兜底掩盖 bug）
            if (!writeCompleted) {
                fail("[数据完整性测试] 写入阶段超时：仍有 " + writeLatch.getCount() + " 条 Future 未完成");
            }
            if (writeFailCount.get() != 0) {
                fail("[数据完整性测试] 写入阶段存在失败：失败数=" + writeFailCount.get());
            }
            if (resultPairs.size() != writeCount) {
                fail("[数据完整性测试] 写入成功数不匹配：期望=" + writeCount + ", 实际=" + resultPairs.size());
            }
        } finally {
            // 无论写阶段是否失败，都关闭 instance（清理资源 + 强制上传剩余 Block），
            // 用 try/catch 包住，避免 close() 抛异常时掩盖阶段一真正的 AssertionError。
            try {
                instance.close(15000, 15000, 15000);
            } catch (Exception closeEx) {
                log.error("[数据完整性测试] instance.close() 失败", closeEx);
            }
        }

        // ---- 回读阶段：从 S3 按 offset+size 精确读取并逐字节对比 ----
        // instance.close() 会关闭 instanceClient，因此回读必须使用独立的 verifyClient
        S3Client verifyClient = S3ClientFactory.createS3Client(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY);
        int passCount = 0;
        int failCount = 0;
        try {
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
                        .bucket(BUCKET_NAME)
                        .key(s3Key)
                        .range(range)
                        .build();

                try (ResponseInputStream<GetObjectResponse> s3Stream = verifyClient.getObject(getReq)) {
                    byte[] readBack = s3Stream.readAllBytes();
                    if (readBack.length != size) {
                        log.error("[数据完整性测试] 读回字节数不匹配: s3Key={} offset={} "
                                + "expectedSize={} readBackSize={}", s3Key, offset, size, readBack.length);
                        failCount++;
                        continue;
                    }
                    // 逐字节比对
                    if (Arrays.equals(original, readBack)) {
                        passCount++;
                        log.debug("[数据完整性测试] PASS: s3Key={} offset={} size={}", s3Key, offset, size);
                    } else {
                        failCount++;
                        int diffPos = -1;
                        for (int k = 0; k < original.length; k++) {
                            if (original[k] != readBack[k]) {
                                diffPos = k;
                                break;
                            }
                        }
                        if (diffPos >= 0) {
                            log.error("[数据完整性测试] FAIL：数据不一致! s3Key={} offset={} size={} "
                                            + "首个差异字节位置={} 期望=0x{} 实际=0x{}",
                                    s3Key, offset, size, diffPos,
                                    Integer.toHexString(original[diffPos] & 0xFF),
                                    Integer.toHexString(readBack[diffPos] & 0xFF));
                        } else {
                            log.error("[数据完整性测试] FAIL：数据不一致! s3Key={} offset={} size={}",
                                    s3Key, offset, size);
                        }
                    }
                } catch (IOException e) {
                    log.error("[数据完整性测试] 从S3读取数据失败: s3Key={} offset={} size={} err={}",
                            s3Key, offset, size, e.getMessage());
                    failCount++;
                }
            }

            log.info("[数据完整性测试] 最终结果：PASS={}, FAIL={}, 写入失败={}",
                    passCount, failCount, writeFailCount.get());

            // 回读阶段强制校验
            if (failCount != 0 || passCount != writeCount) {
                fail("[数据完整性测试] 回读校验失败：PASS=" + passCount + ", FAIL=" + failCount
                        + ", 期望=" + writeCount);
            }
            log.info("[数据完整性测试] ALL PASSED：写入数据与S3数据完全一致");
        } finally {
            verifyClient.close();
        }
    }


    // ========================================================================
    // 堆外数据完整性测试（writeOffHeapData 两种重载）
    // 目标：验证通过 DirectByteBuffer 写入的原始字节与上传到 S3 后读回的字节逐字节一致。
    // 覆盖：
    //   1. writeOffHeapData(ByteBuffer)                 —— 写 buffer 的 [position, limit)
    //   2. writeOffHeapData(ByteBuffer, offset, length) —— 写 buffer 的 [position+offset, position+offset+length)
    // 判定规则：与 dataIntegrityTest 一致（超时/数量/字节比对任一不满足即抛 AssertionError）。
    // ========================================================================
    public static void offHeapDataIntegrityTest() throws Exception {
        S3Client instanceClient = S3ClientFactory.createS3Client(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY);
        BucketConfig defaultBucketConfig = new BucketConfig();
        defaultBucketConfig
                .setBlockSize(BlockSizeLevel.TINY.getBytes())
                .setCacheSize(32 * 1024 * 1024L)
                .setBlockUpLoadCount(BlockUploadConcurrencyLevel.NORMAL.getConcurrency())
                .setS3KeyPrefix(S3_KEY_PREFIX)
                .setLockMappedFilePageCache(false)
                .setWarmWalFile(true)
                .setWalFileSize(32 * 1024 * 1024L);
        S3CloudCacheConfig config = new S3CloudCacheConfig("textinstance-offheap", null, defaultBucketConfig);
        config.blockMaxIdleTime = 15000;
        S3CloudCacheInstance instance = new S3CloudCacheInstance(instanceClient, config);
        instance.start();
        BucketWriterWriter writer = instance.getBucketWriterInstance(BUCKET_NAME);

        int wholeBufferCount = 500;  // 场景一：整块 DirectByteBuffer
        int subRangeCount = 500;     // 场景二：打包 buffer 的 [offset, offset+length) 子区间
        int total = wholeBufferCount + subRangeCount;

        // 预生成确定性、长度各异的原始数据（总量约 2.4MB，会跨越一个 TINY(2MB) Block）
        List<byte[]> originalDataList = new ArrayList<>(total);
        for (int i = 0; i < total; i++) {
            String content = String.format("[offheap-%04d] 堆外数据完整性测试 DIRECT_BUFFER verifyBytes=%d tail:", i, i * 11);
            byte[] padding = new byte[i * 5];
            Arrays.fill(padding, (byte) ((i * 7) % 127));
            byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
            byte[] data = new byte[contentBytes.length + padding.length];
            System.arraycopy(contentBytes, 0, data, 0, contentBytes.length);
            System.arraycopy(padding, 0, data, contentBytes.length, padding.length);
            originalDataList.add(data);
        }

        CountDownLatch writeLatch = new CountDownLatch(total);
        CopyOnWriteArrayList<Object[]> resultPairs = new CopyOnWriteArrayList<>();
        AtomicInteger writeFailCount = new AtomicInteger(0);

        log.info("[堆外数据测试] 开始写入 {} 条堆外数据（wholeBuffer={}, subRange={}）",
                total, wholeBufferCount, subRangeCount);

        boolean writeCompleted;
        try {
            // 场景一：writeOffHeapData(ByteBuffer) —— 每条数据一个独立 DirectByteBuffer
            for (int i = 0; i < wholeBufferCount; i++) {
                byte[] original = originalDataList.get(i);
                ByteBuffer buffer = ByteBuffer.allocateDirect(original.length);
                buffer.put(original);
                buffer.flip();  // position=0, limit=length
                int recordIndex = i;
                writer.writeOffHeapData(buffer).whenComplete((res, thr) ->
                        onWriteDone("[堆外数据测试]", recordIndex, original, res, thr,
                                resultPairs, writeFailCount, writeLatch));
            }

            // 场景二：writeOffHeapData(ByteBuffer, offset, length) —— 多条记录打包进一个 DirectByteBuffer
            int recordsPerBuffer = 8;
            for (int groupStart = wholeBufferCount; groupStart < total; groupStart += recordsPerBuffer) {
                int groupEnd = Math.min(groupStart + recordsPerBuffer, total);
                int count = groupEnd - groupStart;
                int packedSize = 0;
                for (int i = groupStart; i < groupEnd; i++) {
                    packedSize += originalDataList.get(i).length;
                }
                ByteBuffer packed = ByteBuffer.allocateDirect(packedSize);
                int[] offsets = new int[count];
                int[] lengths = new int[count];
                int pos = 0;
                for (int i = groupStart; i < groupEnd; i++) {
                    byte[] rec = originalDataList.get(i);
                    offsets[i - groupStart] = pos;
                    lengths[i - groupStart] = rec.length;
                    packed.put(rec);
                    pos += rec.length;
                }
                packed.flip();  // position=0, limit=packedSize
                for (int i = groupStart; i < groupEnd; i++) {
                    int off = offsets[i - groupStart];
                    int len = lengths[i - groupStart];
                    byte[] original = originalDataList.get(i);
                    int recordIndex = i;
                    writer.writeOffHeapData(packed, off, len).whenComplete((res, thr) ->
                            onWriteDone("[堆外数据测试]", recordIndex, original, res, thr,
                                    resultPairs, writeFailCount, writeLatch));
                }
            }

            writeCompleted = writeLatch.await(120, TimeUnit.SECONDS);

            log.info("[堆外数据测试] 写入阶段结束：成功={}, 失败={}, latch超时={}",
                    resultPairs.size(), writeFailCount.get(), !writeCompleted);

            if (!writeCompleted) {
                fail("[堆外数据测试] 写入阶段超时：仍有 " + writeLatch.getCount() + " 条 Future 未完成");
            }
            if (writeFailCount.get() != 0) {
                fail("[堆外数据测试] 写入阶段存在失败：失败数=" + writeFailCount.get());
            }
            if (resultPairs.size() != total) {
                fail("[堆外数据测试] 写入成功数不匹配：期望=" + total + ", 实际=" + resultPairs.size());
            }
        } finally {
            try {
                instance.close(15000, 15000, 15000);
            } catch (Exception closeEx) {
                log.error("[堆外数据测试] instance.close() 失败", closeEx);
            }
        }

        // ---- 回读阶段：从 S3 按 offset+size 精确读取并逐字节对比 ----
        S3Client verifyClient = S3ClientFactory.createS3Client(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY);
        int passCount = 0;
        int failCount = 0;
        try {
            for (Object[] pair : resultPairs) {
                byte[] original = (byte[]) pair[0];
                WriteResult wr = (WriteResult) pair[1];
                String s3Key = wr.getS3Key();
                long offset = wr.getOffset();
                int size = wr.getSize();

                if (size != original.length) {
                    log.error("[堆外数据测试] size不匹配: s3Key={} offset={} expectedSize={} actualSize={}",
                            s3Key, offset, original.length, size);
                    failCount++;
                    continue;
                }

                String range = "bytes=" + offset + "-" + (offset + size - 1);
                GetObjectRequest getReq = GetObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key(s3Key)
                        .range(range)
                        .build();

                try (ResponseInputStream<GetObjectResponse> s3Stream = verifyClient.getObject(getReq)) {
                    byte[] readBack = s3Stream.readAllBytes();
                    if (readBack.length != size) {
                        log.error("[堆外数据测试] 读回字节数不匹配: s3Key={} offset={} expectedSize={} readBackSize={}",
                                s3Key, offset, size, readBack.length);
                        failCount++;
                        continue;
                    }
                    if (Arrays.equals(original, readBack)) {
                        passCount++;
                        log.debug("[堆外数据测试] PASS: s3Key={} offset={} size={}", s3Key, offset, size);
                    } else {
                        failCount++;
                        int diffPos = -1;
                        for (int k = 0; k < original.length; k++) {
                            if (original[k] != readBack[k]) {
                                diffPos = k;
                                break;
                            }
                        }
                        if (diffPos >= 0) {
                            log.error("[堆外数据测试] FAIL：数据不一致! s3Key={} offset={} size={} 首个差异字节位置={} 期望=0x{} 实际=0x{}",
                                    s3Key, offset, size, diffPos,
                                    Integer.toHexString(original[diffPos] & 0xFF),
                                    Integer.toHexString(readBack[diffPos] & 0xFF));
                        } else {
                            log.error("[堆外数据测试] FAIL：数据不一致! s3Key={} offset={} size={}", s3Key, offset, size);
                        }
                    }
                } catch (IOException e) {
                    log.error("[堆外数据测试] 从S3读取数据失败: s3Key={} offset={} size={} err={}",
                            s3Key, offset, size, e.getMessage());
                    failCount++;
                }
            }

            log.info("[堆外数据测试] 最终结果：PASS={}, FAIL={}, 写入失败={}",
                    passCount, failCount, writeFailCount.get());

            if (failCount != 0 || passCount != total) {
                fail("[堆外数据测试] 回读校验失败：PASS=" + passCount + ", FAIL=" + failCount + ", 期望=" + total);
            }
            log.info("[堆外数据测试] ALL PASSED：堆外写入数据与S3数据完全一致");
        } finally {
            verifyClient.close();
        }
    }

    /**
     * 写入回调统一处理：成功收集 (original, WriteResult)，失败计数，并递减 latch。
     */
    private static void onWriteDone(String test, int index, byte[] original, WriteResult res, Throwable thr,
                                    CopyOnWriteArrayList<Object[]> pairs, AtomicInteger failCount, CountDownLatch latch) {
        if (thr != null || res == null || !res.isSuccess()) {
            log.error("{} 写入失败: record={}, res={}, thr={}", test, index, res, thr);
            failCount.incrementAndGet();
        } else {
            pairs.add(new Object[]{original, res});
        }
        latch.countDown();
    }
}
