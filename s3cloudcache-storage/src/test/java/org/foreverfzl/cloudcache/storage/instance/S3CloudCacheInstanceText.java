package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.storage.factory.S3ClientFactory;
import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterWriter;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudcache.core.datastruct.HeapBlockDataStruct;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.metadata.manager.DeadDataQueue;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.AppendMessageResult;
import org.foreverfzl.cloudcache.wal.datastruct.WalDataStruct;
import org.foreverfzl.cloudcache.wal.Util.BucketMetaInfoUtil;
import org.foreverfzl.cloudchache.common.FutureContext;
import org.foreverfzl.cloudchache.common.exception.CloudCacheException;
import org.foreverfzl.cloudchache.common.WriteResult;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockSizeLevel;
import org.foreverfzl.cloudchache.common.cloudcahceEnum.BlockUploadConcurrencyLevel;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.lang.reflect.Proxy;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * 中文：包含真实 MinIO 手动用例和 offline* 离线故障回归；main 会发真实请求并产生 WAL/对象，JUnit 只调用离线入口。
 * English: Contains real-MinIO manual cases and offline* fault regressions; main performs real I/O and creates WAL/objects, while JUnit invokes offline entry points.
 * 中文：完整性检查记录身份、字节和返回位置，不要求未提交并发记录按调用顺序排列；压测结果不是生产吞吐 SLA。
 * English: Integrity checks record identities, bytes and returned locations, not invocation-order layouts for uncommitted records; load results are not production SLAs.
 */
public class S3CloudCacheInstanceText {

    /** 中文：阶段/失败诊断，日志不能替代断言；English: stage/failure diagnostics, not a replacement for assertions. */
    private static final Logger log = LoggerFactory.getLogger("Text");

    // ===== 测试公共配置（与 MinIO 环境保持一致）=====
    // English: Real-service manual settings; offline scenarios never use this endpoint or credentials.
    /** 中文：需提前启动的本地服务地址；English: local service endpoint to provision before running. */
    private static final String ENDPOINT = "http://127.0.0.1:9001";
    /** 中文：客户端签名区域；English: client signing region. */
    private static final String REGION = "cn-local";
    /** 中文：本地示例访问标识，勿在源码中替换为生产凭证；English: local demo identifier; never replace with production credentials in source. */
    private static final String ACCESS_KEY = "123456789";
    /** 中文：本地示例密钥，部署凭证应外置管理；English: local demo secret; manage deployment credentials externally. */
    private static final String SECRET_KEY = "123456789";
    /** 中文：必须预先存在的远端 Bucket；English: remote bucket that must already exist. */
    private static final String BUCKET_NAME = "textbucket";
    /** 中文：手动对象前缀，重复测试需注意存量对象命名空间；English: manual object prefix; account for existing namespaces on reruns. */
    private static final String S3_KEY_PREFIX = "oreder/phone";

    /**
     * 测试失败统一出口：打印错误并抛出 AssertionError，让测试真正"失败"，而不是只打日志后假装通过。
     * English: Logs and throws AssertionError so a diagnostic is not mistaken for a passing test.
     * @param message 中文：场景失败描述；English: scenario failure description.
     * @throws AssertionError 中文：总是失败退出；English: always thrown to fail the test.
     */
    private static void fail(String message) {
        log.error(message);
        throw new AssertionError(message);
    }

    /**
     * 中文：手动执行下面选中的真实服务用例，不会自动执行 offline 回归；运行前确认目录与远端对象可以用于测试。
     * English: Manually runs the selected real-service case, not offline regressions; verify local/remote test targets before running.
     * @param args 中文：未使用；English: unused.
     * @throws Exception 中文：网络、文件或等待失败；English: network, file or wait failure.
     */
    public static void main(String[] args) throws Exception {
//        text();
//        //高并发测试
//        highConcurrencyTest();
//        //串行数据校验测试
        dataIntegrityTest();
//        //堆外内存测试
//        offHeapDataIntegrityTest();
//        中等并发大数据量测试
//        Medium_concurrency_large_data_volume_test();
    }


    //测试WAL持久化是否正常工作
    /**
     * 中文：历史交互示例，等待控制台输入并打印真实上传结果；不含完整性断言，回调错误处理也不宜直接用于生产。
     * English: Legacy interactive demo waiting for console input and logging real uploads; lacks integrity assertions and production-grade callback error handling.
     */
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
    // 中等并发 + 大数据量测试（触发文件切换）
    // 配置：walFileSize=16MB, blockSize=2MB(TINY)
    // 写入：8 线程 × 1000 条 × 4KB ≈ 32MB，跨越 2 个 WAL 文件（必然触发文件切换）
    // 校验：写阶段零失败 + 数量精确；回读阶段逐字节比对（Range GET），一字不差才算通过
    // ========================================================================
    /**
     * 中文：真实 8×1000 条 4KiB 写入触发文件轮转，检查成功数量与 Range GET 字节。
     * English: Real 8x1000 writes of 4 KiB force rotation; verifies success counts and Range GET bytes.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void Medium_concurrency_large_data_volume_test() throws Exception {
        S3Client instanceClient = S3ClientFactory.createS3Client(ENDPOINT, REGION, ACCESS_KEY, SECRET_KEY);
        BucketConfig defaultBucketConfig = new BucketConfig();
        defaultBucketConfig
                .setBlockSize(BlockSizeLevel.TINY.getBytes())          // 2MB
                .setCacheSize(64 * 1024 * 1024L)                       // 64MB，块池 32 个，足够容纳 16 个数据块
                .setBlockUpLoadCount(BlockUploadConcurrencyLevel.NORMAL.getConcurrency())
                .setS3KeyPrefix(S3_KEY_PREFIX)
                .setLockMappedFilePageCache(false)
                .setWarmWalFile(false)
                .setWalFileSize(16 * 1024 * 1024L);                    // 16MB，触发文件切换
        S3CloudCacheConfig config = new S3CloudCacheConfig("textinstance-medium", null, defaultBucketConfig);
        config.blockMaxIdleTime = 10000;
        S3CloudCacheInstance instance = new S3CloudCacheInstance(instanceClient, config);
        instance.start();
        BucketWriterWriter writer = instance.getBucketWriterInstance(BUCKET_NAME);

        int threadCount = 8;          // 中等并发
        int writesPerThread = 1000;
        int total = threadCount * writesPerThread;   // 8000
        int recordSize = 4096;        // 每条 4KB
        // 总数据量 ≈ 8000 × 4KB = 32MB > 16MB，必然触发至少一次文件切换

        CountDownLatch writeLatch = new CountDownLatch(total);
        LongAdder writeSuccessCount = new LongAdder();
        LongAdder writeFailCount = new LongAdder();
        CopyOnWriteArrayList<Object[]> verifyPairs = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<String> writeErrors = new CopyOnWriteArrayList<>();

        ExecutorService writeExecutor = Executors.newFixedThreadPool(threadCount);
        log.info("[中等并发大数据量测试] ===== 阶段一：并发写入 =====");
        log.info("[中等并发大数据量测试] 线程数={}, 每线程={}, 合计={}, 单条={}B, 总数据≈{}MB",
                threadCount, writesPerThread, total, recordSize, (long) total * recordSize / 1024 / 1024);

        boolean writeCompleted;
        try {
            for (int t = 0; t < threadCount; t++) {
                int threadId = t;
                writeExecutor.submit(() -> {
                    for (int s = 0; s < writesPerThread; s++) {
                        byte[] original = buildMediumRecord(threadId, s, recordSize);
                        try {
                            CompletableFuture<WriteResult> future = writer.writeHeapData(original);
                            int seq = s;
                            future.whenComplete((res, thr) -> {
                                if (thr != null) {
                                    writeErrors.add("Future异常 thread=" + threadId + " seq=" + seq
                                            + " err=" + thr.getMessage());
                                    writeFailCount.increment();
                                } else if (res == null || !res.isSuccess()) {
                                    writeErrors.add("写入失败 thread=" + threadId + " seq=" + seq + " result=" + res);
                                    writeFailCount.increment();
                                } else {
                                    verifyPairs.add(new Object[]{original, res});
                                    writeSuccessCount.increment();
                                }
                                writeLatch.countDown();
                            });
                        } catch (Exception e) {
                            writeErrors.add("write()异常 thread=" + threadId + " seq=" + s + " err=" + e.getMessage());
                            writeFailCount.increment();
                            writeLatch.countDown();
                        }
                    }
                });
            }

            writeCompleted = writeLatch.await(600, TimeUnit.SECONDS);

            log.info("[中等并发大数据量测试] 阶段一结束：成功={}, 失败={}, latch超时={}",
                    writeSuccessCount.sum(), writeFailCount.sum(), !writeCompleted);
            if (!writeErrors.isEmpty()) {
                log.error("[中等并发大数据量测试] 写入阶段共 {} 个错误，前10条：", writeErrors.size());
                writeErrors.stream().limit(10).forEach(e -> log.error("  WRITE-ERR: {}", e));
            }

            if (!writeCompleted) {
                fail("[中等并发大数据量测试] 写入阶段超时：仍有 " + writeLatch.getCount() + " 条 Future 未完成");
            }
            if (!writeErrors.isEmpty() || writeFailCount.sum() != 0) {
                fail("[中等并发大数据量测试] 写入阶段存在失败：失败数=" + writeFailCount.sum()
                        + ", 错误条数=" + writeErrors.size());
            }
            if (writeSuccessCount.sum() != total || verifyPairs.size() != total) {
                fail("[中等并发大数据量测试] 写入成功数不匹配：期望=" + total
                        + ", 成功计数=" + writeSuccessCount.sum() + ", 收集对=" + verifyPairs.size());
            }
        } finally {
            writeExecutor.shutdownNow();
            log.info("[中等并发大数据量测试] 正在关闭 instance，等待全部 Block 上传至 S3...");
            try {
                instance.close(60000, 60000, 60000);
            } catch (Exception closeEx) {
                log.error("[中等并发大数据量测试] instance.close() 失败", closeEx);
            }
        }
        log.info("[中等并发大数据量测试] instance 已关闭，共 {} 条记录待校验", verifyPairs.size());

        // ===== 阶段二：S3 回读逐字节校验 =====
        log.info("[中等并发大数据量测试] ===== 阶段二：S3 回读校验 =====");
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

                        if (size != original.length) {
                            if (verifyErrors.size() < 50) {
                                verifyErrors.add("size不匹配 s3Key=" + s3Key + " offset=" + offset
                                        + " expectedLen=" + original.length + " size=" + size);
                            }
                            verifyFail.increment();
                            return;
                        }

                        String range = "bytes=" + offset + "-" + (offset + size - 1);
                        GetObjectRequest getReq = GetObjectRequest.builder()
                                .bucket(BUCKET_NAME)
                                .key(s3Key)
                                .range(range)
                                .build();

                        try (ResponseInputStream<GetObjectResponse> stream = verifyClient.getObject(getReq)) {
                            byte[] readBack = stream.readAllBytes();
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
                                    verifyErrors.add("FAIL s3Key=" + s3Key + " offset=" + offset + " size=" + size
                                            + " readBackLen=" + readBack.length
                                            + (diffPos >= 0 ? " 首差异位=" + diffPos : " 长度不一致"));
                                }
                                verifyFail.increment();
                            }
                        }
                    } catch (IOException e) {
                        if (verifyErrors.size() < 50) {
                            verifyErrors.add("S3读取异常 s3Key=" + wr.getS3Key() + " offset=" + wr.getOffset()
                                    + " err=" + e.getMessage());
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

            log.info("[中等并发大数据量测试] ===== 最终汇总 =====");
            log.info("[中等并发大数据量测试] 阶段一写入：总计={}, 成功={}, 失败={}",
                    total, writeSuccessCount.sum(), writeFailCount.sum());
            log.info("[中等并发大数据量测试] 阶段二校验：待校验={}, PASS={}, FAIL={}, latch超时={}",
                    verifyPairs.size(), verifyPass.sum(), verifyFail.sum(), !verifyCompleted);
            if (!verifyErrors.isEmpty()) {
                log.error("[中等并发大数据量测试] 检测到 {} 条数据不一致（最多展示前50条）：", verifyErrors.size());
                verifyErrors.forEach(e -> log.error("  DATA-ERR: {}", e));
            }

            if (!verifyCompleted) {
                fail("[中等并发大数据量测试] 回读校验超时：仍有 " + verifyLatch.getCount() + " 条未校验完成");
            }
            if (verifyFail.sum() != 0) {
                fail("[中等并发大数据量测试] 回读校验失败：FAIL=" + verifyFail.sum());
            }
            if (verifyPass.sum() != total) {
                fail("[中等并发大数据量测试] 回读校验 PASS 数不匹配：期望=" + total + ", 实际=" + verifyPass.sum());
            }

            log.info("[中等并发大数据量测试] ✓ ALL PASSED：所有数据写入成功且与 S3 读回完全一致");
        } finally {
            verifyExecutor.shutdownNow();
            verifyClient.close();
        }
    }

    /**
     * 构造一条确定性、可定位的固定长度记录，确保每条数据唯一且可回读逐字节比对。
     * English: Deterministic fixed-length bytes identify their producer/sequence for exact readback comparison.
     * @param threadId 中文：生产者编号；English: producer identifier.
     * @param seq 中文：生产者内序号；English: sequence within that producer.
     * @param recordSize 中文：数组字节数，用例传入 4096；English: array byte length, 4096 in this case.
     * @return 中文：新建独立数组；English: new independent array.
     */
    private static byte[] buildMediumRecord(int threadId, int seq, int recordSize) {
        byte[] record = new byte[recordSize];
        byte[] header = ("MED-" + threadId + "-" + seq + ":").getBytes(StandardCharsets.UTF_8);
        System.arraycopy(header, 0, record, 0, Math.min(header.length, recordSize));
        for (int i = header.length; i < recordSize; i++) {
            record[i] = (byte) ((threadId * 131 + seq * 17 + i) & 0xFF);
        }
        return record;
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
    /**
     * 中文：真实 160000 条并发写入，在 close 前检查 Future 完成，避免关闭兜底掩盖自动封口故障。
     * English: Real 160000 concurrent writes check future completion before close, avoiding shutdown masking automatic-sealing failures.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
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
    /**
     * 中文：真实串行提交 1000 条变长记录，异步上传后按各条成功位置逐字节回读。
     * English: Serially submits 1000 variable-length records to real storage and verifies every successful range byte by byte.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
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
    /**
     * 中文：真实服务各写 500 条完整/切片 direct buffer，验证位置、长度与字节一致。
     * English: Writes 500 whole-buffer and 500 direct-buffer range records to real storage, verifying locations, sizes and bytes.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
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

    // 以下回归只使用临时 WAL 和内存 S3Client，不修改上面真实 MinIO 测试的 main 入口。
    // 中文：离线方法由 JUnit 包装类调用，与 main 当前选中的真实用例无关。
    // English: JUnit wrappers invoke offline methods independently of whichever real-service case main selects.
    /** 中文：2 MiB 逻辑块，WAL 还需容纳每条协议头；English: 2 MiB logical block; WAL capacity also includes each record header. */
    private static final int REGRESSION_BLOCK_SIZE = 2 * 1024 * 1024;
    /** 中文：内存模拟端专用 Bucket 名；English: bucket name reserved for the in-memory endpoint. */
    private static final String REGRESSION_BUCKET = "offline-regression";

    /**
     * 中文：阻塞 PUT 返回证明 Future 不提前成功；finally 释放闸门以免失败后泄漏线程。
     * English: Gates PUT return to prove futures do not succeed early; finally releases the gate to avoid leaking workers.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void offlineFutureConfirmationTest() throws Exception {
        MemoryS3 remote = new MemoryS3();
        remote.blockUploads.set(true);
        try (RegressionFixture fixture = new RegressionFixture("confirmation", remote)) {
            byte[] value = regressionRecord(1, 16385);
            CompletableFuture<WriteResult> future = fixture.writer.writeHeapData(value);
            sealRegressionTail(fixture.writer);
            checkRegression(remote.uploadEntered.await(10, TimeUnit.SECONDS), "upload did not start");
            checkRegression(!future.isDone(), "Future completed before S3 returned a successful response");
            remote.allowUpload.countDown();
            verifyRegressionResult(remote, value, future.get(15, TimeUnit.SECONDS));
        } finally {
            remote.allowUpload.countDown();
        }
    }

    /**
     * 中文：注入 503，验证重试及失败尾块 WAL 保留，再重开同目录完整恢复。
     * English: Injects 503 to verify retries and failed-tail WAL retention, then restarts the same directory for complete recovery.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void offlineFailedUploadRecoveryTest() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-regression-failure-");
        MemoryS3 failedRemote = new MemoryS3();
        failedRemote.failUploads.set(true);
        Map<Integer, byte[]> expected = new HashMap<>();
        List<CompletableFuture<WriteResult>> futures = new ArrayList<>();
        try (RegressionFixture fixture = new RegressionFixture(directory, "failed-restart", failedRemote)) {
            for (int id = 0; id < 24; id++) {
                byte[] record = regressionRecord(id, 32768 + id % 4);
                expected.put(id, record);
                futures.add(fixture.writer.writeHeapData(record));
            }
            // A partial tail must remain recoverable even when shutdown itself starts the failing upload.
            // 中文：部分尾块即使由关闭触发失败上传，也必须保留重启所需的 WAL。
            fixture.close();
            for (CompletableFuture<WriteResult> future : futures) {
                try {
                    WriteResult result = future.get(20, TimeUnit.SECONDS);
                    checkRegression(!result.isSuccess(), "failed S3 upload returned a successful Future");
                } catch (ExecutionException expectedFailure) {
                    // Exceptional completion is also an explicit failure, never a false success.
                    // 中文：异常完成也是明确失败，不能把它算成上传成功。
                }
            }
            checkRegression(failedRemote.attempts.get() >= 3, "transient upload exception was not retried");
        }
        checkRegression(failedRemote.objects.isEmpty(), "failure stub unexpectedly accepted an object");
        try (var paths = Files.walk(directory)) {
            checkRegression(paths.anyMatch(path -> Files.isRegularFile(path)
                            && path.getFileName().toString().matches("\\d+")),
                    "failed upload deleted its recovery WAL: " + directory);
        }
        MemoryS3 recoveredRemote = new MemoryS3();
        try (RegressionFixture fixture = new RegressionFixture(directory, "failed-restart", recoveredRemote)) {
            awaitRegressionBytes(recoveredRemote, totalRegressionBytes(expected), 20000);
            verifyRegressionRecords(recoveredRemote, expected);
        }
    }

    /**
     * 中文：8×96 条混合长度轮流覆盖四种 API，两块物理池强制回收并跨多个 WAL。
     * English: 8x96 mixed-length records exercise four APIs; a two-block pool forces reuse across multiple WAL files.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void offlineConcurrentIntegrityTest() throws Exception {
        MemoryS3 remote = new MemoryS3();
        try (RegressionFixture fixture = new RegressionFixture("concurrent", remote)) {
            ExecutorService executor = Executors.newFixedThreadPool(8);
            ConcurrentLinkedQueue<PendingRegressionRecord> pending = new ConcurrentLinkedQueue<>();
            // 中文：只在生产者中并发收集；等待全部生产者完成后才做顺序校验，避免对未完成集合断言。
            // English: Producers collect concurrently; assertions run only after all producer tasks finish.
            List<Future<?>> producers = new ArrayList<>();
            CountDownLatch start = new CountDownLatch(1);
            try {
                for (int thread = 0; thread < 8; thread++) {
                    int threadId = thread;
                    producers.add(executor.submit(() -> {
                        start.await();
                        for (int sequence = 0; sequence < 96; sequence++) {
                            int id = threadId * 96 + sequence;
                            byte[] record = regressionRecord(id, 16384 + id % 97);
                            CompletableFuture<WriteResult> future;
                            switch (id % 4) {
                                // 中文：非零数组偏移及非零 position/相对 offset 专门检测切片坐标系错误。
                                // English: Nonzero array offsets and buffer position/relative offsets detect mixed coordinate systems.
                                case 0 -> future = fixture.writer.writeHeapData(record);
                                case 1 -> {
                                    byte[] container = new byte[record.length + 11];
                                    System.arraycopy(record, 0, container, 7, record.length);
                                    future = fixture.writer.writeHeapData(container, 7, record.length);
                                }
                                case 2 -> {
                                    ByteBuffer buffer = ByteBuffer.allocateDirect(record.length + 5);
                                    buffer.position(5);
                                    buffer.put(record).flip().position(5);
                                    future = fixture.writer.writeOffHeapData(buffer);
                                    checkRegression(buffer.position() == 5, "direct buffer position changed");
                                }
                                default -> {
                                    ByteBuffer buffer = ByteBuffer.allocateDirect(record.length + 13);
                                    buffer.position(9);
                                    buffer.put(record).flip().position(4);
                                    future = fixture.writer.writeOffHeapData(buffer, 5, record.length);
                                    checkRegression(buffer.position() == 4, "direct slice position changed");
                                }
                            }
                            pending.add(new PendingRegressionRecord(record, future));
                        }
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> producer : producers) producer.get(40, TimeUnit.SECONDS);
                sealRegressionTail(fixture.writer);
                Map<Integer, byte[]> expected = new HashMap<>();
                for (PendingRegressionRecord entry : pending) {
                    verifyRegressionResult(remote, entry.payload, entry.future.get(20, TimeUnit.SECONDS));
                    expected.put(ByteBuffer.wrap(entry.payload).getInt(), entry.payload);
                }
                checkRegression(expected.size() == 768, "producer lost records");
                checkRegression(remote.objects.size() >= 6, "test did not cross enough blocks");
                checkRegression(fixture.writer.getMappedManager().getActiveMappedFile().get().fileFromOffset
                        >= 2L * REGRESSION_BLOCK_SIZE, "test did not rotate WAL files");
                verifyRegressionRecords(remote, expected);
            } finally {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * 中文：16 线程首次查找同 Bucket，比较对象身份而非只看名字。
     * English: Sixteen threads first-access one bucket and compare object identity rather than names.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void offlineWriterSingletonTest() throws Exception {
        try (RegressionFixture fixture = new RegressionFixture("writers", new MemoryS3())) {
            ExecutorService executor = Executors.newFixedThreadPool(16);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<BucketWriterWriter>> results = new ArrayList<>();
            ConcurrentLinkedQueue<BucketWriterWriter> observed = new ConcurrentLinkedQueue<>();
            try {
                for (int index = 0; index < 16; index++) {
                    results.add(executor.submit(() -> {
                        start.await();
                        BucketWriterWriter writer = fixture.instance.getBucketWriterInstance("concurrently-created-bucket");
                        observed.add(writer);
                        return writer;
                    }));
                }
                start.countDown();
                BucketWriterWriter first = results.getFirst().get(10, TimeUnit.SECONDS);
                for (Future<BucketWriterWriter> result : results) {
                    checkRegression(result.get(10, TimeUnit.SECONDS) == first, "same Bucket has multiple Writers");
                }
            } finally {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
                // Also stop orphan recovery threads if the singleton assertion exposes a regression.
                // 中文：若单例断言失败，也关闭意外重复的恢复线程，避免测试失败后进程无法退出。
                observed.forEach(BucketWriterWriter::close);
            }
        }
    }

    /**
     * 中文：关闭实例一后继续使用实例二并创建实例三，检测静态执行器误共享。
     * English: Uses a second instance and creates a third after closing the first to detect incorrect shared-static executors.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void offlineInstanceIsolationTest() throws Exception {
        MemoryS3 secondRemote = new MemoryS3();
        try (RegressionFixture first = new RegressionFixture("first-instance", new MemoryS3());
             RegressionFixture second = new RegressionFixture("second-instance", secondRemote)) {
            first.close();
            byte[] record = regressionRecord(42, 255);
            CompletableFuture<WriteResult> future = second.writer.writeHeapData(record);
            sealRegressionTail(second.writer);
            verifyRegressionResult(secondRemote, record, future.get(15, TimeUnit.SECONDS));
            // Construction after another instance closes must also work in the same JVM.
            // 中文：同 JVM 内关闭后再创建，能捕获静态线程池永久关闭后被再次使用的问题。
            try (RegressionFixture third = new RegressionFixture("third-instance", new MemoryS3())) {
                checkRegression(third.writer != null, "new instance failed after another instance closed");
            }
        }
    }

    /**
     * 中文：同目录和远端状态重开，验证旧位置仍有效且新对象不复用旧 Key。
     * English: Restarts the same directory/remote state, requiring old locations to remain valid and new keys to differ.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void offlineRestartKeyUniquenessTest() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-regression-restart-");
        MemoryS3 remote = new MemoryS3();
        WriteResult oldResult;
        byte[] oldRecord = regressionRecord(100, 1111);
        try (RegressionFixture first = new RegressionFixture(directory, "same-instance", remote)) {
            CompletableFuture<WriteResult> oldFuture = first.writer.writeHeapData(oldRecord);
            sealRegressionTail(first.writer);
            oldResult = oldFuture.get(15, TimeUnit.SECONDS);
            verifyRegressionResult(remote, oldRecord, oldResult);
        }
        try (RegressionFixture second = new RegressionFixture(directory, "same-instance", remote)) {
            byte[] newRecord = regressionRecord(101, 2222);
            CompletableFuture<WriteResult> newFuture = second.writer.writeHeapData(newRecord);
            sealRegressionTail(second.writer);
            WriteResult newResult = newFuture.get(15, TimeUnit.SECONDS);
            checkRegression(!oldResult.getS3Key().equals(newResult.getS3Key()), "restart reused an acknowledged S3 key");
            verifyRegressionResult(remote, oldRecord, oldResult);
            verifyRegressionResult(remote, newRecord, newResult);
            verifyRegressionRecords(remote, Map.of(100, oldRecord, 101, newRecord));
        }
    }

    /**
     * 中文：前块失败后块成功，重启检查成功块上传次数不增加且完整恢复缺口。
     * English: With an earlier failed block and later confirmed block, restart must fill the gap without reuploading the confirmed object.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void offlineNonContiguousConfirmationTest() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-regression-upload-gap-");
        MemoryS3 remote = new MemoryS3();
        byte[] failedRecord = regressionRecord(200, REGRESSION_BLOCK_SIZE * 3 / 4);
        byte[] confirmedRecord = regressionRecord(201, REGRESSION_BLOCK_SIZE * 3 / 4 + 1);
        remote.failedRecordIds.add(200);
        WriteResult confirmed;
        try (RegressionFixture fixture = new RegressionFixture(directory, "upload-gap", remote)) {
            CompletableFuture<WriteResult> failed = fixture.writer.writeHeapData(failedRecord);
            CompletableFuture<WriteResult> successful = fixture.writer.writeHeapData(confirmedRecord);
            sealRegressionTail(fixture.writer);
            confirmed = successful.get(20, TimeUnit.SECONDS);
            verifyRegressionResult(remote, confirmedRecord, confirmed);
            try {
                checkRegression(!failed.get(20, TimeUnit.SECONDS).isSuccess(), "gap block reported false success");
            } catch (ExecutionException expectedFailure) {
                // The first block failed, but the following block is already acknowledged to its caller.
                // 中文：前块失败不撤销后块已经交付给调用方的确认位置。
            }
        }
        String confirmedObject = REGRESSION_BUCKET + "/" + confirmed.getS3Key();
        int previousUploads = remote.acceptedUploads.get(confirmedObject).get();
        remote.failedRecordIds.clear();
        try (RegressionFixture fixture = new RegressionFixture(directory, "upload-gap", remote)) {
            awaitRegressionBytes(remote, (long) failedRecord.length + confirmedRecord.length, 20000);
            // Wait for recovery/upload termination before asserting absence of a late duplicate upload.
            // 中文：等待恢复/上传退出后再断言次数，避免漏掉迟到的重复 PUT。
            fixture.close();
            checkRegression(remote.acceptedUploads.get(confirmedObject).get() == previousUploads,
                    "restart reuploaded an acknowledged block beyond a failed upload gap");
            verifyRegressionResult(remote, confirmedRecord, confirmed);
            verifyRegressionRecords(remote, Map.of(200, failedRecord, 201, confirmedRecord));
        }
    }

    /**
     * 中文：只让第二次 Core 复制失败，第三条停在 WAL→Core 空窗，验证恢复等原请求结算后无重复重放。
     * English: Fails only the second Core copy and delays the third at the WAL-to-Core gap, checking recovery waits for original requests without duplicates.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void offlineRuntimeBrokenBlockRecoveryTest() throws Exception {
        MemoryS3 remote = new MemoryS3();
        try (RegressionFixture fixture = new RegressionFixture("broken-block", remote)) {
            // Fault injection stays in the test: WAL receives real data, only the second core copy fails.
            // 中文：反射和覆盖仅用于测试注入，不在生产代码增加故障开关；WAL 仍写真实完整协议。
            var coreField = BucketWriterWriter.class.getDeclaredField("cacheBlockManager");
            coreField.setAccessible(true);
            CacheBlockManager core = (CacheBlockManager) coreField.get(fixture.writer);
            byte[] first = regressionRecord(300, 65537);
            byte[] second = regressionRecord(301, 65538);
            byte[] late = regressionRecord(302, 65539);
            CompletableFuture<WriteResult> firstFuture = fixture.writer.writeHeapData(first);
            MappedFileManager wal = fixture.writer.getMappedManager();
            AppendMessageResult secondWal = wal.appendData(new WalDataStruct(second));
            AppendMessageResult lateWal = wal.appendData(new WalDataStruct(late));
            checkRegression(secondWal.isOk() && lateWal.isOk(), "fault injection WAL append failed");
            checkRegression(secondWal.getLogicalIndex() == lateWal.getLogicalIndex(), "test records must share one block");
            CompletableFuture<WriteResult> secondFuture = new CompletableFuture<>();
            FutureContext secondContext = new FutureContext(secondFuture);
            secondContext.setWalRecordId(secondWal.getBlockOffset());
            CompletableFuture<WriteResult> lateFuture = new CompletableFuture<>();
            FutureContext lateContext = new FutureContext(lateFuture);
            lateContext.setWalRecordId(lateWal.getBlockOffset());
            AtomicInteger failedCopies = new AtomicInteger();
            CloudCacheBlock originalBlock = core.getExistingBlock(secondWal.getFileFromOffset(), secondWal.getLogicalIndex());
            var failedAppend = core.appendData(new HeapBlockDataStruct(secondWal.getDefaultMappedFile(),
                    secondWal.getLogicalIndex(), second, 0, second.length) {
                /**
                 * 中文：测试专用复制失败策略，保留 WAL 原数据，只耗尽 Core 的复制重试。
                 * English: Test-only copy-failure strategy retaining original WAL while exhausting Core copy retries.
                 * @param target 中文：故意不写入的目标段；English: destination segment intentionally left untouched.
                 * @return 中文：始终 false，模拟物理复制失败；English: always false to simulate a physical copy failure.
                 */
                @Override
                public boolean writeTo(MemorySegment target) {
                    // 中文：不写 target，重复返回 false 以耗尽 Core 原有的两次复制尝试。
                    // English: Leave target untouched and return false on both of Core's copy attempts.
                    failedCopies.incrementAndGet();
                    return false;
                }
            }, secondContext, true);
            checkRegression(!failedAppend.result() && failedCopies.get() == 2, "physical append failure was not injected");
            sealRegressionTail(fixture.writer);
            CloudCacheBlock prematureLease = core.beginRecovery(secondWal.getFileFromOffset(), secondWal.getLogicalIndex());
            if (prematureLease != null) core.finishRecovery(prematureLease, false);
            checkRegression(prematureLease == null, "recovery started before the late original append settled");
            checkRegression(core.getExistingBlock(secondWal.getFileFromOffset(), secondWal.getLogicalIndex()) == originalBlock,
                    "broken logical block was recycled before recovery");
            // This original append must register its Future and settle, but must not add a duplicate core copy.
            // 中文：迟到原请求必须登记 Future 并结算，但恢复之外不应再次复制重复数据。
            core.appendData(new HeapBlockDataStruct(lateWal.getDefaultMappedFile(), lateWal.getLogicalIndex(),
                    late, 0, late.length), lateContext, true);
            verifyRegressionResult(remote, first, firstFuture.get(20, TimeUnit.SECONDS));
            verifyRegressionResult(remote, second, secondFuture.get(20, TimeUnit.SECONDS));
            verifyRegressionResult(remote, late, lateFuture.get(20, TimeUnit.SECONDS));
            verifyRegressionRecords(remote, Map.of(300, first, 301, second, 302, late));
        }
    }

    /**
     * 中文：占满物理池后中断申请，要求明确 FAILED、保留中断与 WAL、投死信并可重启恢复。
     * English: Interrupts allocation under a full pool, requiring FAILED state, preserved interrupt/WAL, dead-letter notification and restart recovery.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void offlineInterruptedPoolWaitTest() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-regression-interrupted-pool-");
        MemoryS3 remote = new MemoryS3();
        remote.blockUploads.set(true);
        byte[] first = regressionRecord(400, REGRESSION_BLOCK_SIZE * 3 / 4);
        byte[] second = regressionRecord(401, REGRESSION_BLOCK_SIZE * 3 / 4 + 1);
        byte[] waiting = regressionRecord(402, REGRESSION_BLOCK_SIZE * 3 / 4 + 2);
        long interruptedFileOffset;
        try (RegressionFixture fixture = new RegressionFixture(directory, "interrupted-pool", remote)) {
            CompletableFuture<WriteResult> firstFuture = fixture.writer.writeHeapData(first);
            CompletableFuture<WriteResult> secondFuture = fixture.writer.writeHeapData(second);
            checkRegression(remote.uploadEntered.await(10, TimeUnit.SECONDS), "pool owners did not start uploading");
            // Both physical blocks remain owned by gated uploads; the next logical block has only WAL data.
            // 中文：闸门让物理池无空闲，第三个逻辑块只有 WAL 来源，没有可借用的绑定。
            MappedFileManager wal = fixture.writer.getMappedManager();
            AppendMessageResult record = wal.appendData(new WalDataStruct(waiting));
            checkRegression(record.isOk(), "waiting record was not accepted into WAL");
            interruptedFileOffset = record.getFileFromOffset();
            var coreField = BucketWriterWriter.class.getDeclaredField("cacheBlockManager");
            coreField.setAccessible(true);
            CacheBlockManager core = (CacheBlockManager) coreField.get(fixture.writer);
            checkRegression(core.getExistingBlock(record.getFileFromOffset(), record.getLogicalIndex()) == null,
                    "interrupted test unexpectedly has a physical block binding");
            CompletableFuture<WriteResult> future = new CompletableFuture<>();
            FutureContext context = new FutureContext(future);
            context.setWalRecordId(record.getBlockOffset());
            try {
                Thread.currentThread().interrupt();
                var result = core.appendData(new HeapBlockDataStruct(record.getDefaultMappedFile(),
                        record.getLogicalIndex(), waiting, 0, waiting.length), context, true);
                checkRegression(!result.result(), "interrupted resource wait returned success");
                checkRegression(Thread.currentThread().isInterrupted(), "append discarded interrupt status");
            } finally {
                Thread.interrupted(); // Clear only the interruption deliberately injected by this test.
            }
            checkRegression(!future.get(5, TimeUnit.SECONDS).isSuccess(), "interrupted request did not fail promptly");
            checkRegression(wal.blockMetaDataManager.getBlockMetaData(record.getFileFromOffset(),
                    record.getLogicalIndex()).getState() == BlockMetaData.FAILED, "unbound interrupted block is not FAILED");
            checkRegression(core.beginRecovery(record.getFileFromOffset(), record.getLogicalIndex()) == null,
                    "unbound terminal task unexpectedly obtained a runtime recovery lease");
            var deadQueueField = wal.blockMetaDataManager.getClass().getDeclaredField("deadDataQueue");
            deadQueueField.setAccessible(true);
            DeadDataQueue deadQueue = (DeadDataQueue) deadQueueField.get(wal.blockMetaDataManager);
            var dead = deadQueue.poll();
            checkRegression(dead != null && dead.getFileFromOffset() == record.getFileFromOffset()
                    && dead.getLogicalIndex() == record.getLogicalIndex(), "interrupted task did not reach the dead-letter queue");
            remote.allowUpload.countDown();
            verifyRegressionResult(remote, first, firstFuture.get(15, TimeUnit.SECONDS));
            verifyRegressionResult(remote, second, secondFuture.get(15, TimeUnit.SECONDS));
        } finally {
            remote.allowUpload.countDown();
        }
        try (var files = Files.walk(directory)) {
            checkRegression(files.anyMatch(path -> Files.isRegularFile(path)
                    && path.getFileName().toString().equals(Long.toString(interruptedFileOffset))),
                    "interrupted request's WAL was deleted");
        }
        try (RegressionFixture fixture = new RegressionFixture(directory, "interrupted-pool", remote)) {
            awaitRegressionBytes(remote, (long) first.length + second.length + waiting.length, 20000);
            fixture.close();
            verifyRegressionRecords(remote, Map.of(400, first, 401, second, 402, waiting));
        }
    }

    /**
     * 中文：损坏第二条 Value 的 CRC，禁止把首条合法前缀当完整 Block 上传。
     * English: Corrupts the second Value CRC and forbids uploading the valid first prefix as a complete block.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void offlineCorruptWalRecoveryTest() throws Exception {
        offlineCorruptWalRecovery(false);
    }

    /**
     * 中文：清零第二条、保留第三条，禁止把中间零洞当正常结束。
     * English: Zeroes the second record while retaining a third, forbidding a middle zero gap from being treated as EOF.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void offlineZeroHoleWalRecoveryTest() throws Exception {
        offlineCorruptWalRecovery(true);
    }

    /**
     * 中文：先制造上传失败留下临时 WAL，再仅损坏第二条并重启；断言恢复异常、PUT 次数为零且原 WAL 保留。
     * English: Retains temporary WAL through upload failure, damages only the second record, then requires exceptional recovery, zero PUTs and retained WAL.
     * @param zeroHole 中文：true 清零整条且保留后续记录；false 翻转 Value 使 CRC 失配；English: true zeroes a record before a later record; false flips Value to break CRC.
     * @throws Exception 中文：临时文件或有界恢复等待失败；English: temporary-file or bounded recovery-wait failure.
     */
    private static void offlineCorruptWalRecovery(boolean zeroHole) throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-regression-corrupt-wal-");
        MemoryS3 failedRemote = new MemoryS3();
        failedRemote.failUploads.set(true);
        byte[] first = regressionRecord(500, 4097);
        byte[] second = regressionRecord(501, 4099);
        try (RegressionFixture fixture = new RegressionFixture(directory, "corrupt-wal", failedRemote)) {
            CompletableFuture<WriteResult> firstFuture = fixture.writer.writeHeapData(first);
            CompletableFuture<WriteResult> secondFuture = fixture.writer.writeHeapData(second);
            CompletableFuture<WriteResult> thirdFuture = zeroHole
                    ? fixture.writer.writeHeapData(regressionRecord(502, 4101)) : null;
            fixture.close();
            checkRegression(!firstFuture.get(5, TimeUnit.SECONDS).isSuccess(), "failed setup upload was acknowledged");
            checkRegression(!secondFuture.get(5, TimeUnit.SECONDS).isSuccess(), "failed setup upload was acknowledged");
            if (thirdFuture != null) {
                checkRegression(!thirdFuture.get(5, TimeUnit.SECONDS).isSuccess(), "failed setup upload was acknowledged");
            }
        }
        Path walFile;
        try (var files = Files.walk(directory)) {
            walFile = files.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().equals("0"))
                    .findFirst().orElseThrow(() -> new AssertionError("setup WAL disappeared"));
        }
        // Keep the first record valid; simulate either CRC damage or an unfilled reservation before a valid third record.
        // 中文：4096 是文件元数据头，12 是记录头；第二条偏移必须加首条对齐后的长度。
        // English: 4096 is the file metadata header and 12 the record header; locate the second record after the aligned first record.
        long secondHeaderOffset = 4096L + 12 + ((first.length + 3) & ~3);
        try (var channel = Files.newByteChannel(walFile, StandardOpenOption.WRITE)) {
            channel.position(zeroHole ? secondHeaderOffset : secondHeaderOffset + 12);
            ByteBuffer damage = zeroHole ? ByteBuffer.allocate(12 + ((second.length + 3) & ~3))
                    : ByteBuffer.wrap(new byte[]{(byte) (second[0] ^ 0x7f)});
            while (damage.hasRemaining()) channel.write(damage);
        }
        MemoryS3 remote = new MemoryS3();
        S3CloudCacheInstance instance = new S3CloudCacheInstance(remote.client(),
                regressionConfig(directory, "corrupt-wal"));
        try {
            instance.start();
            var recoveryField = S3CloudCacheInstance.class.getDeclaredField("recoveryFuture");
            recoveryField.setAccessible(true);
            CompletableFuture<?> recovery = (CompletableFuture<?>) recoveryField.get(instance);
            boolean rejected = false;
            try {
                recovery.get(15, TimeUnit.SECONDS);
            } catch (ExecutionException expectedFailure) {
                rejected = true;
            }
            checkRegression(rejected, "corrupt WAL recovery incorrectly reported success");
            checkRegression(remote.attempts.get() == 0 && remote.objects.isEmpty(),
                    "corrupt WAL uploaded a valid prefix as though the block were complete");
        } finally {
            instance.close(3000, 3000, 5000);
        }
        checkRegression(Files.exists(walFile), "corrupt original WAL was removed instead of retained for repair");
        checkRegression(remote.objects.isEmpty(), "close uploaded the prefix of a corrupt block");
        checkRegression(BucketMetaInfoUtil.readBucketMetaFile(walFile.getParent().getParent()).getIsDirty() != 0,
                "failed WAL recovery incorrectly marked its bucket clean");
    }

    /**
     * 中文：损坏 bucketMeta CRC 后拒绝恢复和新 Writer，逐字节检查旧 WAL/元数据未被覆盖。
     * English: Corrupts bucketMeta CRC, requires recovery/new-writer rejection and verifies original WAL/metadata remain byte-identical.
     * @throws Exception 中文：资源创建或有界等待失败，令测试失败；English: fixture/resource or bounded-wait failure fails the test.
     */
    public static void offlineCorruptBucketMetadataTest() throws Exception {
        Path directory = Files.createTempDirectory("cloudcache-regression-corrupt-bucket-meta-");
        MemoryS3 failedRemote = new MemoryS3();
        failedRemote.failUploads.set(true);
        try (RegressionFixture fixture = new RegressionFixture(directory, "corrupt-bucket-meta", failedRemote)) {
            CompletableFuture<WriteResult> future = fixture.writer.writeHeapData(regressionRecord(600, 8193));
            fixture.close();
            checkRegression(!future.get(5, TimeUnit.SECONDS).isSuccess(), "failed setup upload was acknowledged");
        }
        Path walFile;
        try (var files = Files.walk(directory)) {
            walFile = files.filter(path -> Files.isRegularFile(path) && path.getFileName().toString().equals("0"))
                    .findFirst().orElseThrow(() -> new AssertionError("setup WAL disappeared"));
        }
        Path bucketDirectory = walFile.getParent().getParent();
        Path metadataFile = bucketDirectory.resolve("bucketMeta");
        byte[] metadataBytes = Files.readAllBytes(metadataFile);
        try (var channel = Files.newByteChannel(metadataFile, StandardOpenOption.WRITE)) {
            // 中文：只翻转已存 CRC 的一个字节，不改布局字段；用保留的原字节验证失败路径不覆盖文件。
            // English: Flip only a stored CRC byte, not layout fields; preserve original bytes to detect overwrite on failure.
            channel.position(16); // Stored CRC, after dirty(4), block size(4), and WAL size(8).
            channel.write(ByteBuffer.wrap(new byte[]{(byte) (metadataBytes[16] ^ 0x40)}));
        }
        org.junit.Assert.assertThrows(org.foreverfzl.cloudchache.common.exception.WalException.class,
                () -> BucketMetaInfoUtil.readBucketMetaFile(bucketDirectory));
        byte[] originalWal = Files.readAllBytes(walFile);
        byte[] corruptedMetadata = Files.readAllBytes(metadataFile);
        MemoryS3 remote = new MemoryS3();
        S3CloudCacheInstance instance = new S3CloudCacheInstance(remote.client(),
                regressionConfig(directory, "corrupt-bucket-meta"));
        try {
            instance.start();
            var recoveryField = S3CloudCacheInstance.class.getDeclaredField("recoveryFuture");
            recoveryField.setAccessible(true);
            CompletableFuture<?> recovery = (CompletableFuture<?>) recoveryField.get(instance);
            boolean recoveryRejected = false;
            try {
                recovery.get(15, TimeUnit.SECONDS);
            } catch (ExecutionException expectedFailure) {
                recoveryRejected = true;
            }
            checkRegression(recoveryRejected, "invalid bucket metadata was treated as an empty recovery");
            boolean writerRejected = false;
            try {
                instance.getBucketWriterInstance(REGRESSION_BUCKET);
            } catch (CloudCacheException expectedFailure) {
                writerRejected = true;
            }
            checkRegression(writerRejected, "new Writer overwrote metadata after failed recovery preparation");
            checkRegression(remote.attempts.get() == 0, "uninterpretable WAL was uploaded");
        } finally {
            instance.close(3000, 3000, 5000);
        }
        checkRegression(Arrays.equals(originalWal, Files.readAllBytes(walFile)),
                "failed metadata recovery altered or truncated the original WAL");
        checkRegression(Arrays.equals(corruptedMetadata, Files.readAllBytes(metadataFile)),
                "failed metadata recovery overwrote the original bucket metadata");
    }

    /**
     * 中文：主动封口并刷新 WAL/检查点，使故障回归不依赖空闲调度周期；它不等待 S3 上传完成。
     * English: Explicitly seals/flushes WAL and checkpoints so regressions do not depend on idle timers; does not wait for S3 upload.
     * @param writer 中文：测试拥有的运行中 Writer；English: live writer owned by the test.
     */
    private static void sealRegressionTail(BucketWriterWriter writer) {
        MappedFileManager manager = writer.getMappedManager();
        manager.sealAllBlocks();
        manager.endFlushFileReadPosition();
        manager.endMetaFlush();
    }

    /**
     * 中文：测试 Value 自带大端 id/length 共 8 字节，与外层 12 字节 WAL 头无关，用于检测重复、缺失与乱序后的内容。
     * English: Test Value begins with big-endian id/length (8 bytes), independent of the 12-byte WAL header, enabling duplicate/missing/content checks after reordering.
     * @param id 中文：本用例唯一记录 ID；English: record ID unique within the case.
     * @param length 中文：含测试头的 Value 字节数，至少 8；English: Value byte count including the test header, at least 8.
     * @return 中文：新建确定性字节数组；English: newly allocated deterministic bytes.
     */
    private static byte[] regressionRecord(int id, int length) {
        byte[] bytes = new byte[length];
        ByteBuffer.wrap(bytes).putInt(id).putInt(length);
        for (int index = 8; index < length; index++) bytes[index] = (byte) (id * 31 + index * 17);
        return bytes;
    }

    /**
     * 中文：验证成功结果确实指向已接收对象的原始 Value，不能只凭 ETag 或长度判断正确。
     * English: Verifies a successful result points to original Value bytes in an accepted object, not merely a matching ETag/length.
     * @param remote 中文：保留已接受对象的模拟服务；English: fake retaining accepted objects.
     * @param expected 中文：不可变使用的原始 Value；English: original Value treated as immutable.
     * @param result 中文：必须非 null 且成功的提交结果；English: result required to be non-null and successful.
     * @throws AssertionError 中文：身份、范围或字节不匹配；English: object, range or byte mismatch.
     */
    private static void verifyRegressionResult(MemoryS3 remote, byte[] expected, WriteResult result) {
        checkRegression(result != null && result.isSuccess(), "write did not finish successfully");
        byte[] object = remote.objects.get(REGRESSION_BUCKET + "/" + result.getS3Key());
        checkRegression(object != null, "successful Future refers to an object not accepted by S3");
        int offset = Math.toIntExact(result.getOffset());
        checkRegression(result.getSize() == expected.length && offset >= 0
                && offset + expected.length <= object.length, "returned range is invalid");
        checkRegression(Arrays.equals(expected, Arrays.copyOfRange(object, offset, offset + expected.length)),
                "returned range differs from original record id=" + ByteBuffer.wrap(expected).getInt());
    }

    /**
     * @param records 中文：按 ID 唯一保存的期望 Value；English: expected Values uniquely keyed by ID.
     * @return 中文：Value 字节总数，不计 WAL 头/填充；English: total Value bytes excluding WAL headers/padding.
     */
    private static long totalRegressionBytes(Map<Integer, byte[]> records) {
        return records.values().stream().mapToLong(bytes -> bytes.length).sum();
    }

    /**
     * 中文：以单调时钟等待最低字节量到达，仅用于就绪等待；之后必须另外逐记录验证，不能用总长度证明完整性。
     * English: Monotonic-clock readiness wait for a minimum byte count; follow with per-record verification since total length alone proves no integrity.
     * @param remote 中文：可并发写入的模拟对象表；English: fake object store concurrently populated by uploads.
     * @param expectedBytes 中文：期望至少接收的 Value 字节数；English: minimum expected Value bytes.
     * @param timeoutMillis 中文：等待上限，毫秒；English: wait limit in milliseconds.
     * @throws Exception 中文：等待中断；超时另抛 AssertionError；English: interrupted wait; timeout separately throws AssertionError.
     */
    private static void awaitRegressionBytes(MemoryS3 remote, long expectedBytes, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (remote.objects.values().stream().mapToLong(bytes -> bytes.length).sum() < expectedBytes) {
            checkRegression(System.nanoTime() < deadline, "recovery did not upload all expected bytes");
            Thread.sleep(10);
        }
    }

    /**
     * 中文：按测试 Value 自带头遍历所有对象，不依赖对象/记录遍历顺序；必须 ID 集合一致、无重复、字节完全一致。
     * English: Parses every object using test Value headers independent of traversal order; requires identical ID sets, no duplicates and exact bytes.
     * @param remote 中文：待检查的模拟服务内容；English: fake service contents to verify.
     * @param expected 中文：原始 ID→Value 映射；English: original ID-to-Value mapping.
     * @throws AssertionError 中文：缺失、重复、未知 ID、截断或字节变化；English: missing, duplicate, unknown, truncated or changed records.
     */
    private static void verifyRegressionRecords(MemoryS3 remote, Map<Integer, byte[]> expected) {
        Set<Integer> found = new HashSet<>();
        long total = 0;
        for (byte[] object : remote.objects.values()) {
            int offset = 0;
            while (offset < object.length) {
                checkRegression(object.length - offset >= 8, "truncated record header in S3 object");
                ByteBuffer header = ByteBuffer.wrap(object, offset, 8);
                int id = header.getInt();
                int length = header.getInt();
                checkRegression(length >= 8 && length <= object.length - offset, "corrupt record length in S3");
                checkRegression(found.add(id), "duplicate record in S3: " + id);
                checkRegression(expected.containsKey(id), "unexpected record in S3: " + id);
                checkRegression(Arrays.equals(expected.get(id), Arrays.copyOfRange(object, offset, offset + length)),
                        "S3 payload differs for record " + id);
                offset += length;
                total += length;
            }
        }
        checkRegression(found.equals(expected.keySet()), "S3 record set has missing records");
        checkRegression(total == totalRegressionBytes(expected), "S3 payload byte count mismatch");
    }

    /**
     * 中文：离线用例公共硬断言，不以日志掩盖失败；English: hard offline assertion that never hides failure in logs.
     * @param condition 中文：必须成立的不变量；English: invariant required to hold.
     * @param message 中文：失败上下文；English: failure context.
     * @throws AssertionError 中文：condition 为 false；English: condition is false.
     */
    private static void checkRegression(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    /**
     * 中文：跨生产者收集的浅不可变对应关系，数组不防御复制，测试发布后不再修改。
     * English: Shallowly immutable pairing collected across producers; arrays are not defensively copied and must stay unchanged after publication.
     * @param payload 中文：原始 Value；English: original Value.
     * @param future 中文：该条异步提交结果；English: asynchronous commit result for the record.
     */
    private record PendingRegressionRecord(byte[] payload, CompletableFuture<WriteResult> future) { }

    /**
     * 中文：两块物理池/两块 WAL，关闭预热/锁页并开启模拟 HEAD；空闲时间 600000 ms，测试显式封口避免依赖定时器。
     * English: Two-block pool/WAL, no warmup/page locking and fake HEAD enabled; 600000 ms idle threshold makes tests seal explicitly instead of relying on timers.
     * @param directory 中文：本用例隔离目录，可在重启阶段复用；English: isolated case directory reusable during restart phases.
     * @param name 中文：本用例稳定实例名；English: stable instance name for the case.
     * @return 中文：新建可变配置，生产构造器还会快照；English: new mutable config, later snapshotted by the production constructor.
     */
    private static S3CloudCacheConfig regressionConfig(Path directory, String name) {
        BucketConfig bucket = new BucketConfig()
                .setBlockSize(REGRESSION_BLOCK_SIZE)
                .setCacheSize(2L * REGRESSION_BLOCK_SIZE)
                .setWalFileSize(2L * REGRESSION_BLOCK_SIZE)
                .setBlockUpLoadCount(2)
                .setS3KeyPrefix("offline-regression")
                .setWarmWalFile(false)
                .setLockMappedFilePageCache(false)
                .setEnableHeadCheck(true);
        S3CloudCacheConfig config = new S3CloudCacheConfig(name, directory.toString(), bucket);
        config.blockMaxIdleTime = 600000;
        return config;
    }

    /**
     * 中文：离线场景的实例/闸门资源拥有者；临时目录保留便于重启和诊断，不代表会自动删除全部文件。
     * English: Owns instance/gate cleanup for offline scenarios; temporary directories remain for restart/diagnostics rather than being recursively deleted.
     */
    private static final class RegressionFixture implements AutoCloseable {
        /** 中文：真实生产实例，由夹具关闭；English: real production instance closed by this fixture. */
        private final S3CloudCacheInstance instance;
        /** 中文：共享测试 Bucket Writer，生命周期归实例；English: shared test writer owned by the instance. */
        private final BucketWriterWriter writer;
        /** 中文：可跨重启阶段保留对象的模拟服务；English: fake service retaining objects across restart phases. */
        private final MemoryS3 remote;
        /** 中文：避免 try-with-resources 与显式 close 重复收尾；English: prevents duplicate cleanup from explicit close plus try-with-resources. */
        private final AtomicBoolean closed = new AtomicBoolean();

        /**
         * 中文：创建全新临时目录用于隔离用例；English: creates a fresh temporary directory to isolate the case.
         * @param name 中文：实例/目录诊断名；English: instance/directory diagnostic name.
         * @param remote 中文：非 null 的模拟服务；English: non-null fake service.
         * @throws Exception 中文：临时目录或初始化失败；English: temporary-directory or initialization failure.
         */
        private RegressionFixture(String name, MemoryS3 remote) throws Exception {
            this(Files.createTempDirectory("cloudcache-regression-" + name + "-"), name, remote);
        }

        /**
         * 中文：允许复用目录模拟重启；启动恢复后取得 Writer，恢复可仍在异步执行。
         * English: Allows directory reuse to simulate restart; obtains a writer after scheduling recovery, which may still be running.
         * @param directory 中文：隔离 WAL 根目录；English: isolated WAL root.
         * @param name 中文：稳定实例名；English: stable instance name.
         * @param remote 中文：接收对象的模拟端；English: fake receiving uploaded objects.
         * @throws Exception 中文：生产实例初始化或恢复准备失败；English: production initialization/recovery preparation failure.
         */
        private RegressionFixture(Path directory, String name, MemoryS3 remote) throws Exception {
            this.remote = remote;
            log.info("Offline regression WAL directory: {}", directory);
            instance = new S3CloudCacheInstance(remote.client(), regressionConfig(directory, name));
            instance.start();
            writer = instance.getBucketWriterInstance(REGRESSION_BUCKET);
        }

        /** 中文：先开闸再幂等关闭，避免失败收尾自己阻塞上传；English: opens the gate before idempotent close so test cleanup cannot itself block uploads. */
        @Override
        public void close() {
            remote.allowUpload.countDown();
            if (closed.compareAndSet(false, true)) instance.close(3000, 3000, 5000);
        }
    }

    /** Synchronous in-memory S3 boundary: bytes are accepted before a successful response. */
    /**
     * 中文：线程安全内存对象表和可控失败/闸门，只模拟本项目使用的 PUT/HEAD；不是完整 S3 或掉电持久化模拟。
     * English: Concurrent in-memory objects with controlled failures/gates, modeling only required PUT/HEAD operations, not complete S3 or power-loss durability.
     */
    private static final class MemoryS3 {
        /** 中文：bucket/key→完整对象字节，成功 PUT 响应前写入；English: bucket/key to full object bytes, stored before successful PUT response. */
        private final ConcurrentHashMap<String, byte[]> objects = new ConcurrentHashMap<>();
        /** 中文：每个对象接受 PUT 的次数，用于检测重启重复覆盖；English: accepted PUT count per object, detecting repeated overwrite on restart. */
        private final ConcurrentHashMap<String, AtomicInteger> acceptedUploads = new ConcurrentHashMap<>();
        /** 中文：按对象首条测试 ID 定向抛 503；English: test record IDs at object start selected for injected 503 failures. */
        private final Set<Integer> failedRecordIds = ConcurrentHashMap.newKeySet();
        /** 中文：开启时 PUT 等待 allowUpload，模拟慢服务器；English: makes PUT await allowUpload to simulate a slow server. */
        private final AtomicBoolean blockUploads = new AtomicBoolean();
        /** 中文：全部 PUT 注入 503，验证异常重试；English: injects 503 into every PUT to test exception retries. */
        private final AtomicBoolean failUploads = new AtomicBoolean();
        /** 中文：含失败在内的 PUT 尝试数；English: PUT attempt count including failures. */
        private final AtomicInteger attempts = new AtomicInteger();
        /** 中文：第一项上传已进入 SDK 边界的同步信号；English: signals entry of the first upload into the SDK boundary. */
        private final CountDownLatch uploadEntered = new CountDownLatch(1);
        /** 中文：一次性放行全部等待 PUT，finally 必须放行；English: one-shot release of waiting PUTs, always released during cleanup. */
        private final CountDownLatch allowUpload = new CountDownLatch(1);

        /**
         * 中文：创建无网络代理；close 特意不清对象，模拟真实服务跨客户端重建仍保存数据。
         * English: Creates a network-free proxy; close deliberately keeps objects to model a real service surviving client recreation.
         * @return 中文：共享此模拟端状态的新客户端代理；English: new proxy sharing this fake's state.
         */
        private S3Client client() {
            return (S3Client) Proxy.newProxyInstance(S3Client.class.getClassLoader(), new Class<?>[]{S3Client.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "putObject": {
                                // 中文：先阻塞/失败注入，再读取实际 RequestBody；不要直接读取测试原数组，否则会漏掉上传范围错误。
                                // English: Gate/inject failure before reading the actual RequestBody, not the test's original array, to expose upload-range bugs.
                                attempts.incrementAndGet();
                                uploadEntered.countDown();
                                if (blockUploads.get() && !allowUpload.await(20, TimeUnit.SECONDS)) {
                                    throw new AssertionError("test upload gate timed out");
                                }
                                if (failUploads.get()) throw S3Exception.builder().statusCode(503).message("injected offline failure").build();
                                PutObjectRequest request = (PutObjectRequest) args[0];
                                RequestBody body = (RequestBody) args[1];
                                byte[] bytes;
                                try (var input = body.contentStreamProvider().newStream()) {
                                    bytes = input.readAllBytes();
                                }
                                checkRegression(bytes.length == body.contentLength(), "S3 request body length mismatch");
                                if (bytes.length >= 8 && failedRecordIds.contains(ByteBuffer.wrap(bytes).getInt())) {
                                    throw S3Exception.builder().statusCode(503).message("injected block-specific failure").build();
                                }
                                String objectKey = request.bucket() + "/" + request.key();
                                objects.put(objectKey, bytes);
                                acceptedUploads.computeIfAbsent(objectKey, key -> new AtomicInteger()).incrementAndGet();
                                return PutObjectResponse.builder().eTag("offline-etag").build();
                            }
                            case "headObject": {
                                // 中文：长度来自已保存对象而不是请求声明，未 PUT 的对象返回 404。
                                // English: Derive length from stored bytes, not request claims; an object never accepted by PUT yields 404.
                                HeadObjectRequest request = (HeadObjectRequest) args[0];
                                byte[] bytes = objects.get(request.bucket() + "/" + request.key());
                                if (bytes == null) throw S3Exception.builder().statusCode(404).message("missing offline object").build();
                                return HeadObjectResponse.builder().contentLength((long) bytes.length).eTag("offline-etag").build();
                            }
                            case "close": return null;
                            case "serviceName": return "s3";
                            case "toString": return "OfflineMemoryS3";
                            case "hashCode": return System.identityHashCode(proxy);
                            case "equals": return proxy == args[0];
                            default: throw new UnsupportedOperationException("Unexpected S3 operation: " + method);
                        }
                    });
        }
    }

    /**
     * 写入回调统一处理：成功收集 (original, WriteResult)，失败计数，并递减 latch。
     * English: Collects (original, result) on success, counts failures and signals callback completion; containers support concurrent notifications.
     * @param test 中文：日志场景名；English: scenario label.
     * @param index 中文：原记录序号；English: original record index.
     * @param original 中文：未修改的原 Value；English: unchanged original Value.
     * @param res 中文：可能为 null 的业务结果；English: possibly null business result.
     * @param thr 中文：异常完成时的错误，可为 null；English: completion error, possibly null.
     * @param pairs 中文：成功对应关系的并发集合；English: concurrent success pair collection.
     * @param failCount 中文：原子失败计数；English: atomic failure count.
     * @param latch 中文：每条回调都须递减的完成信号；English: completion signal decremented by every callback.
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
