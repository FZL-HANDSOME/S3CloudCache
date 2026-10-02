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
public class FutureNotificationRegressionTest {
    @Rule
    public final TemporaryFolder temporary = new TemporaryFolder();

    @Test(timeout = 10000)
    public void successCallbackCanWaitForAnotherFutureInTheSameBlock() throws Exception {
        assertSiblingCallbacksDoNotBlock(true);
    }

    @Test(timeout = 10000)
    public void failureCallbackCanWaitForAnotherFutureInTheSameBlock() throws Exception {
        assertSiblingCallbacksDoNotBlock(false);
    }

    private void assertSiblingCallbacksDoNotBlock(boolean success) throws Exception {
        BlockMetaData meta = new BlockMetaData();
        CompletableFuture<WriteResult> first = new CompletableFuture<>();
        CompletableFuture<WriteResult> second = new CompletableFuture<>();
        addFuture(meta, first, 0);
        addFuture(meta, second, 1);

        // 两边都等待对方，因此无需依赖 ConcurrentHashMap 的遍历顺序。
        // 旧实现逐条 complete，会在第一条的同步回调里等待尚未通知的第二条。
        CompletableFuture<Void> firstCallback = first.thenAccept(result -> awaitSibling(second, success));
        CompletableFuture<Void> secondCallback = second.thenAccept(result -> awaitSibling(first, success));
        if (success) {
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

    private static void addFuture(BlockMetaData meta, CompletableFuture<WriteResult> future, long offset) {
        FutureContext context = new FutureContext(future);
        context.setWalRecordId(offset);
        context.setPhysicalOffset(offset);
        context.setS3Key("notification-test");
        context.setSize(1);
        meta.addFuture(context);
    }

    private static void awaitSibling(CompletableFuture<WriteResult> sibling, boolean expectedSuccess) {
        try {
            assertEquals(expectedSuccess, sibling.get(2, TimeUnit.SECONDS).isSuccess());
        } catch (Exception error) {
            throw new AssertionError("同一 Block 的另一条 Future 被当前同步回调阻塞", error);
        }
    }

    @Test(timeout = 20000)
    public void synchronousSuccessCallbackCanCloseTheInstance() throws Exception {
        AtomicBoolean clientClosed = new AtomicBoolean();
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
            writer.getMappedManager().sealAllBlocks();
            callback.get(5, TimeUnit.SECONDS);
            assertTrue("同步回调应能完成关闭，而不是等待当前上传线程自身", clientClosed.get());
        } finally {
            // 若重新引入旧行为，先打断自等待，确保失败测试不会泄漏后台线程和文件映射。
            if (callback != null && !callback.isDone() && callbackThread.get() != null) {
                callbackThread.get().interrupt();
                try { callback.get(5, TimeUnit.SECONDS); } catch (Exception ignored) { }
            }
            instance.close(1000, 1000, 1000);
        }
    }
}
