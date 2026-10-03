package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.storage.instance.cloudcache.S3CloudCacheInstance;
import org.foreverfzl.cloudchache.common.FutureContext;
import org.foreverfzl.cloudchache.common.WriteResult;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/** 无真实 S3 请求；专门验证用户同步回调不能阻塞存储线程或同块的其他结果通知。 */
/**
 * 中文：通过互等同步回调和回调内 close 检查结果通知的执行隔离，不测试真实网络吞吐或对象持久性。
 * English: Checks result-notification isolation with mutually waiting synchronous callbacks and callback-driven close, not real network throughput or object durability.
 */
public class FutureNotificationRegressionTest {
    /**
     * 中文：仅供 close 集成场景创建隔离 WAL 的 JUnit 临时目录。
     * English: JUnit temporary directory used for isolated WAL in the close integration scenario.
     */
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    /**
     * 中文：成功通知时，任意一条同步回调等待同块另一条结果都不能阻塞后者完成。
     * English: During success notification, a synchronous callback waiting for a sibling result must not prevent that sibling from completing.
     * @throws Exception 中文：回调未在期限内完成或异常结束；English: callbacks time out or complete exceptionally
     */
    @Test(timeout = 10000)
    public void successCallbackCanWaitForAnotherFutureInTheSameBlock() throws Exception {
        assertSiblingCallbacksDoNotBlock(true);
    }

    /**
     * 中文：失败通知遵守同样的隔离约束，不能因一条用户回调等待另一条失败结果而串行死锁。
     * English: Failure notifications obey the same isolation rule and must not deadlock serially when one callback awaits another failure result.
     * @throws Exception 中文：回调未在期限内完成或异常结束；English: callbacks time out or complete exceptionally
     */
    @Test(timeout = 10000)
    public void failureCallbackCanWaitForAnotherFutureInTheSameBlock() throws Exception {
        assertSiblingCallbacksDoNotBlock(false);
    }

    /**
     * 中文：直接驱动 BlockMetaData 的成功或失败终态，隔离验证通知机制而不启动 WAL 或上传器。
     * English: Directly drives BlockMetaData to a success or failure terminal state, isolating notification behavior without WAL or an uploader.
     * @param success 中文：true 检查成功通知，false 检查失败通知；English: true tests success notification, false tests failure notification
     * @throws Exception 中文：等待任一回调或结果失败；English: waiting for a callback or result fails
     */
    private void assertSiblingCallbacksDoNotBlock(boolean success) throws Exception {
        BlockMetaData meta = new BlockMetaData();
        CompletableFuture<WriteResult> first = new CompletableFuture<>();
        CompletableFuture<WriteResult> second = new CompletableFuture<>();
        addFuture(meta, first, 0);
        addFuture(meta, second, 1);

        // 两边都等待对方，因此无需依赖 ConcurrentHashMap 的遍历顺序。
        // 旧实现逐条 complete，会在第一条的同步回调里等待尚未通知的第二条。
        // 中文：两条 thenAccept 都是同步回调，双向依赖使测试不依赖通知遍历顺序。
        // English: Both thenAccept callbacks are synchronous; mutual dependencies make the test independent of notification traversal order.
        CompletableFuture<Void> firstCallback = first.thenAccept(result -> awaitSibling(second, success));
        CompletableFuture<Void> secondCallback = second.thenAccept(result -> awaitSibling(first, success));
        if (success) {
            // 中文：先使预期、WAL 完成和物理完成字节数相等，按合法状态顺序进入上传成功终态。
            // English: Equalize expected, WAL-completed, and physical-completed bytes before following valid transitions to upload success.
            meta.addExpectedBytes(2);
            meta.addPageCacheBytes(2);
            meta.addFinishedBytes(2);
            assertTrue(meta.trySeal());
            assertTrue(meta.tryStartUpload());
            assertTrue(meta.markUploadSuccess());
            meta.completeAllFuture();
        } else {
            assertTrue(meta.markUploadFailed());
            meta.failAllFuture();
        }
        firstCallback.get(5, TimeUnit.SECONDS);
        secondCallback.get(5, TimeUnit.SECONDS);
        assertEquals(success, first.get().isSuccess());
        assertEquals(success, second.get().isSuccess());
    }

    /**
     * 中文：登记带有完整结果坐标的合成一字节记录；此帮助方法不真正写 WAL 或物理块。
     * English: Registers a synthetic one-byte record with complete result coordinates; it writes neither WAL nor a physical block.
     * @param meta 中文：两条记录共享的块元数据；English: Block metadata shared by the two records
     * @param future 中文：待通知的测试 Future；English: test Future to notify
     * @param offset 中文：合成记录标识和物理偏移；English: synthetic record identity and physical offset
     */
    private static void addFuture(BlockMetaData meta, CompletableFuture<WriteResult> future, long offset) {
        FutureContext context = new FutureContext(future);
        context.setWalRecordId(offset);
        context.setPhysicalOffset(offset);
        context.setS3Key("notification-test");
        context.setSize(1);
        meta.addFuture(context);
    }

    /**
     * 中文：在用户同步回调中限时等待兄弟记录；把超时或异常转成断言，便于定位通知相互阻塞。
     * English: Waits for a sibling inside a synchronous user callback, converting timeout or exceptions to assertions exposing notification blockage.
     * @param sibling 中文：同块另一条记录的 Future；English: another record's Future in the same Block
     * @param expectedSuccess 中文：两条结果应具有的成功标志；English: success flag expected on both results
     * @throws AssertionError 中文：等待超时、异常或结果标志不符；English: timeout, exceptional completion, or unexpected result flag
     */
    private static void awaitSibling(CompletableFuture<WriteResult> sibling, boolean expectedSuccess) {
        try {
            assertEquals(expectedSuccess, sibling.get(2, TimeUnit.SECONDS).isSuccess());
        } catch (Exception error) {
            throw new AssertionError("同一 Block 的另一条 Future 被当前同步回调阻塞", error);
        }
    }

    /**
     * 中文：在真实实例写入的同步成功回调内关闭实例，验证回调不运行在必须被 close 等待的上传任务上。
     * English: Closes a real instance from a synchronous write-success callback, verifying the callback does not occupy an upload task that close must await.
     * @throws Exception 中文：临时目录、写入或限时等待失败；English: temporary-directory, write, or timed-wait operations fail
     */
    @Test(timeout = 20000)
    public void synchronousSuccessCallbackCanCloseTheInstance() throws Exception {
        AtomicBoolean clientClosed = new AtomicBoolean();
        // 中文：内存替身只报告 PUT 成功和 close 标记，本场景关注生命周期而非远端数据比对。
        // English: The in-memory double only reports PUT success and records close; this scenario checks lifecycle, not remote payload contents.
        S3Client client = (S3Client) Proxy.newProxyInstance(S3Client.class.getClassLoader(),
                new Class<?>[]{S3Client.class}, (proxy, method, arguments) -> switch (method.getName()) {
                    case "putObject" -> PutObjectResponse.builder().eTag("notification-test-etag").build();
                    case "close" -> { clientClosed.set(true); yield null; }
                    case "serviceName" -> "s3";
                    case "toString" -> "FutureNotificationTestS3";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        int blockSize = 2 * 1024 * 1024;
        BucketConfig bucket = new BucketConfig().setBlockSize(blockSize).setCacheSize(2L * blockSize)
                .setWalFileSize(2L * blockSize).setBlockUpLoadCount(1).setS3KeyPrefix("notification-test")
                .setWarmWalFile(false).setLockMappedFilePageCache(false).setEnableHeadCheck(false);
        S3CloudCacheInstance instance = new S3CloudCacheInstance(client,
                new S3CloudCacheConfig("callback-close", temporary.newFolder().toString(), bucket));
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        CompletableFuture<Void> callback = null;
        try {
            var writer = instance.getBucketWriterInstance("notification-test");
            CompletableFuture<WriteResult> write = writer.writeHeapData(new byte[128]);
            callback = write.thenAccept(result -> {
                assertTrue(result.isSuccess());
                callbackThread.set(Thread.currentThread());
                instance.close(1000, 1000, 1000);
            });
            // 中文：主动封口尾块使上传立即具备条件，不依赖空闲扫描时间来触发回调。
            // English: Seal the partial tail explicitly so upload can proceed without waiting for an idle scan to trigger the callback.
            writer.getMappedManager().sealAllBlocks();
            callback.get(5, TimeUnit.SECONDS);
            assertTrue("同步回调应能完成关闭，而不是等待当前上传线程自身", clientClosed.get());
        } finally {
            // 若重新引入旧行为，先打断自等待，确保失败测试不会泄漏后台线程和文件映射。
            // 中文：仅在回归导致回调仍卡住时中断已记录的测试线程，然后重试关闭存储资源。
            // English: Interrupt the recorded test callback only if the regression leaves it blocked, then retry storage cleanup.
            if (callback != null && !callback.isDone() && callbackThread.get() != null) {
                callbackThread.get().interrupt();
                try { callback.get(5, TimeUnit.SECONDS); } catch (Exception ignored) { }
            }
            instance.close(1000, 1000, 1000);
        }
    }
}
