package org.foreverfzl.cloudcache.storage.instance;

import org.junit.Test;

/** Offline regression tests. No real S3 service or user WAL directory is accessed. */
/**
 * 中文：离线回归的 JUnit 入口，复用手动测试类中的故障场景；只用临时目录与内存客户端，不调用真实 MinIO main。
 * English: JUnit entry points reusing fault scenarios with temporary directories/in-memory clients; never invoke the real MinIO main.
 * 中文：每个 timeout 单位为毫秒，用于发现死锁/悬挂，不能当成生产性能 SLA。
 * English: Each timeout is in milliseconds to detect hangs/deadlocks, not a production performance SLA.
 */
public class IntegrityRegressionTest {
    /**
     * 中文：上传闸门未打开时 Future 不能提前成功。
     * English: An upload gate proves futures cannot succeed before remote confirmation.
     * @throws Exception 中文：隔离资源创建或有界等待失败，令测试失败；English: fixture creation or bounded-wait failure fails the test.
     */
    @Test(timeout = 60000)
    public void futureWaitsForRemoteConfirmation() throws Exception {
        S3CloudCacheInstanceText.offlineFutureConfirmationTest();
    }

    /**
     * 中文：重试耗尽后保留 WAL，重开后恢复全部记录。
     * English: Retains WAL after exhausted retries and recovers all records on restart.
     * @throws Exception 中文：隔离资源创建或有界等待失败，令测试失败；English: fixture creation or bounded-wait failure fails the test.
     */
    @Test(timeout = 90000)
    public void failedUploadPreservesWalAndRestarts() throws Exception {
        S3CloudCacheInstanceText.offlineFailedUploadRecoveryTest();
    }

    /**
     * 中文：四种写入入口在小池复用/跨 WAL 时仍逐条字节一致。
     * English: All four write APIs preserve bytes through small-pool reuse and WAL rotation.
     * @throws Exception 中文：隔离资源创建或有界等待失败，令测试失败；English: fixture creation or bounded-wait failure fails the test.
     */
    @Test(timeout = 90000)
    public void concurrentMixedWritesSurviveBlockReuseAndFileRotation() throws Exception {
        S3CloudCacheInstanceText.offlineConcurrentIntegrityTest();
    }

    /**
     * 中文：并发首次查找只能启动一个共享 Writer。
     * English: Concurrent first access must create one shared writer.
     * @throws Exception 中文：隔离资源创建或有界等待失败，令测试失败；English: fixture creation or bounded-wait failure fails the test.
     */
    @Test(timeout = 60000)
    public void concurrentWriterLookupReturnsOneWriter() throws Exception {
        S3CloudCacheInstanceText.offlineWriterSingletonTest();
    }

    /**
     * 中文：实例关闭不能关闭其他实例的调度资源。
     * English: Closing one instance must not disable another instance's scheduler.
     * @throws Exception 中文：隔离资源创建或有界等待失败，令测试失败；English: fixture creation or bounded-wait failure fails the test.
     */
    @Test(timeout = 60000)
    public void closingOneInstanceDoesNotDisableAnother() throws Exception {
        S3CloudCacheInstanceText.offlineInstanceIsolationTest();
    }

    /**
     * 中文：清理 WAL 后重启不能复用已确认对象 Key。
     * English: Restart after WAL cleanup must not reuse acknowledged object keys.
     * @throws Exception 中文：隔离资源创建或有界等待失败，令测试失败；English: fixture creation or bounded-wait failure fails the test.
     */
    @Test(timeout = 90000)
    public void cleanRestartDoesNotOverwriteConfirmedObjects() throws Exception {
        S3CloudCacheInstanceText.offlineRestartKeyUniquenessTest();
    }

    /**
     * 中文：前块失败不应使后面已确认块被重启覆盖。
     * English: A failed earlier block must not cause later acknowledged blocks to be overwritten on restart.
     * @throws Exception 中文：隔离资源创建或有界等待失败，令测试失败；English: fixture creation or bounded-wait failure fails the test.
     */
    @Test(timeout = 90000)
    public void restartSkipsConfirmedBlockBeyondFailedGap() throws Exception {
        S3CloudCacheInstanceText.offlineNonContiguousConfirmationTest();
    }

    /**
     * 中文：恢复要等待 WAL→Core 的迟到原请求结算，不能重复或漏写。
     * English: Recovery must await late original WAL-to-Core requests without losing or duplicating bytes.
     * @throws Exception 中文：隔离资源创建或有界等待失败，令测试失败；English: fixture creation or bounded-wait failure fails the test.
     */
    @Test(timeout = 90000)
    public void brokenBlockRecoveryWaitsForLateOriginalAppend() throws Exception {
        S3CloudCacheInstanceText.offlineRuntimeBrokenBlockRecoveryTest();
    }

    /**
     * 中文：池耗尽中断应明确失败并保留重启恢复来源。
     * English: Interrupted pool backpressure must terminate failure explicitly and retain restart recovery data.
     * @throws Exception 中文：隔离资源创建或有界等待失败，令测试失败；English: fixture creation or bounded-wait failure fails the test.
     */
    @Test(timeout = 90000)
    public void interruptedPoolWaitRetainsWalAndCompletesFailure() throws Exception {
        S3CloudCacheInstanceText.offlineInterruptedPoolWaitTest();
    }

    /**
     * 中文：CRC 损坏后不能提交合法前缀，也不能删除原 WAL。
     * English: CRC damage must neither commit a valid prefix nor delete the source WAL.
     * @throws Exception 中文：隔离资源创建或有界等待失败，令测试失败；English: fixture creation or bounded-wait failure fails the test.
     */
    @Test(timeout = 90000)
    public void corruptWalDoesNotUploadValidPrefixOrDeleteOriginal() throws Exception {
        S3CloudCacheInstanceText.offlineCorruptWalRecoveryTest();
    }

    /**
     * 中文：预留零洞后存在有效记录时应拒绝整块。
     * English: Reject a whole block when a reservation zero gap precedes valid records.
     * @throws Exception 中文：隔离资源创建或有界等待失败，令测试失败；English: fixture creation or bounded-wait failure fails the test.
     */
    @Test(timeout = 90000)
    public void zeroWalReservationHoleDoesNotHideLaterRecord() throws Exception {
        S3CloudCacheInstanceText.offlineZeroHoleWalRecoveryTest();
    }

    /**
     * 中文：配置元数据损坏必须阻止新 Writer 覆盖旧恢复信息。
     * English: Corrupt configuration metadata must prevent a new writer from overwriting recovery information.
     * @throws Exception 中文：隔离资源创建或有界等待失败，令测试失败；English: fixture creation or bounded-wait failure fails the test.
     */
    @Test(timeout = 90000)
    public void corruptBucketMetadataRejectsWritesWithoutOverwritingWal() throws Exception {
        S3CloudCacheInstanceText.offlineCorruptBucketMetadataTest();
    }
}
