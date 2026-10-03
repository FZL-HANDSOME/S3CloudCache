package org.foreverfzl.cloudcache.core.manager;

import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.metadata.entity.DeadDataInfo;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.exception.CoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** Uploads immutable, sealed blocks; releases all leases before publishing their result. */
/**
 * 中文：对封口且写齐的逻辑块异步上传，任务先固定物理块和 WAL 引用；成功结果要求上传确认落盘，所有任务结果都在释放任务资源后派发。
 * English: Asynchronously uploads sealed, fully copied logical blocks after pinning their physical block and WAL; success requires persisted acknowledgement, and all task outcomes are dispatched after task-resource release.
 */
public class CacheBlockUpdater {
    /**
     * 中文：上传失败及资源生命周期诊断日志。
     * English: Diagnostics for upload failures and resource lifecycle.
     */
    private static final Logger log = LoggerFactory.getLogger(LogName.CACHE_BLOCK_UPDATER);
    /**
     * 中文：所属块池，用于稳定映射校验、任务计数和终态登记。
     * English: Owning block pool used for binding validation, task accounting, and terminal state registration.
     */
    private final CacheBlockManager manager;
    /**
     * 中文：借用的同步 S3 客户端，由实例统一管理关闭。
     * English: Borrowed synchronous S3 client whose closure is managed by the instance.
     */
    private final S3Client s3Client;
    /**
     * 中文：本类拥有的每任务虚拟线程执行器；不承载用户 Future 通知线程。
     * English: Owned virtual-thread-per-task executor; user Future notification threads do not run on it.
     */
    private final ExecutorService s3VirtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    /**
     * 中文：限制同时执行上传与其重试的任务数，重试间隔仍占用许可。
     * English: Bounds concurrent uploading/retrying tasks; a permit remains held during retry delays.
     */
    private final Semaphore upLoadLimiter;
    /**
     * 中文：是否在 PUT 后用 HEAD 校验对象长度；该检查不是内容校验和验证。
     * English: Whether to verify object length with HEAD after PUT; this is not checksum validation.
     */
    private final boolean enableHeadCheck;

    /**
     * 中文：在 metadata 锁下捕获的任务身份与长度快照；记录本身不复制 payload，数据稳定性依靠物理块租约。
     * English: Task identity and length snapshot captured under metadata; the record does not copy payload, whose stability depends on the physical block lease.
     * @param block 持有上传引用的物理块；English: physical block pinned for upload
     * @param metadata 捕获的原逻辑块 metadata；English: captured logical-block metadata
     * @param file 单独持有引用的 WAL 文件；English: WAL file with a separate held reference
     * @param fileOffset WAL 逻辑起始字节偏移；English: logical WAL start in bytes
     * @param index WAL 内逻辑块序号；English: logical block index within WAL
     * @param key 本次固定的 S3 对象键；English: fixed S3 object key for this task
     * @param size 本次上传的 value 字节总数；English: total value bytes to upload
     */
    private record Upload(CloudCacheBlock block, BlockMetaData metadata, DefaultMappedFile file,
                          long fileOffset, int index, String key, long size) { }

    /**
     * 中文：创建上传并发限制并保存所借用的资源；客户端不在本类关闭。
     * English: Creates upload concurrency control and stores borrowed resources; this class does not close the client.
     * @param manager 所属物理块管理器；English: owning physical block manager
     * @param maximumUploads 同时占用上传许可的最大任务数；English: maximum tasks concurrently holding upload permits
     * @param s3Client 借用的 S3 客户端；English: borrowed S3 client
     * @param enableHeadCheck 是否校验远端长度；English: whether to verify remote length
     * @throws IllegalArgumentException maximumUploads 不大于零；English: maximumUploads is not positive
     */
    public CacheBlockUpdater(CacheBlockManager manager, int maximumUploads, S3Client s3Client, boolean enableHeadCheck) {
        if (maximumUploads <= 0) throw new IllegalArgumentException("maximumUploads must be positive");
        this.manager = manager;
        this.s3Client = s3Client;
        this.upLoadLimiter = new Semaphore(maximumUploads);
        this.enableHeadCheck = enableHeadCheck;
    }

    /**
     * 中文：在同一 metadata 锁下复核绑定、零引用和上传资格，再固定物理块及 WAL 后提交任务；不合格候选静默跳过，执行器拒绝则结算失败并释放租约。
     * English: Revalidates binding, zero references, and eligibility under one metadata monitor, then pins the block and WAL before submission; ineligible candidates are ignored, while executor rejection settles failure and releases leases.
     * @param block 待上传候选，null 或未绑定时无操作；有效任务必须已绑定 WAL；English: candidate, ignored when null or unbound; valid tasks must have a bound WAL
     */
    public void upLoadBlock(CloudCacheBlock block) {
        if (block == null) return;
        BlockMetaData metadata = block.getBlockMetaData();
        if (metadata == null) return;
        Upload upload;
        synchronized (metadata) {
            if (block.getBlockMetaData() != metadata || block.isDelayClean()
                    || manager.getExistingBlock(block.getFileFromOffset(), block.getLogicalIndex()) != block
                    || block.getReferenceCount() != 0 || !metadata.tryStartUpload()) return;
            // 中文：资格判断和状态转移与写入取得租约共用一个监视锁，避免把正在写入或刚复用的物理块交给异步任务。
            // English: Admission and state transition share the writer-lease monitor, excluding active writers and recently rebound physical blocks.
            upload = new Upload(block, metadata, block.getDefaultMappedFile(), block.getFileFromOffset(),
                    block.getLogicalIndex(), block.getS3Key(), block.getWritePosition());
            block.setUnActive();
            block.getReference();
            // 中文：任务提交前固定 WAL 生命周期；捕获字段与独立引用避免异步线程再读取已回收块的身份。
            // English: Pin WAL lifetime before submission; captured fields and a separate reference avoid rereading identity from a recycled block.
            upload.file().hold();
            manager.upCount.incrementAndGet();
        }
        try {
            s3VirtualExecutor.execute(() -> executeUploadTask(upload));
        } catch (RuntimeException rejected) {
            failUpload(upload, rejected);
            releaseUpload(upload);
            metadata.failAllFuture();
        }
    }

    /**
     * 中文：等待上传许可，最多执行三次 PUT/校验，成功后持久化 WAL 上传确认并标记成功；所有分支先归还许可和租约，再通知 Future。
     * English: Waits for a permit, performs up to three PUT/verification attempts, then persists the WAL acknowledgement and marks success; all paths release permits and leases before Future notification.
     * @param upload 已固定资源和身份的任务；English: task with pinned resources and captured identity
     */
    private void executeUploadTask(Upload upload) {
        boolean permitAcquired = false;
        boolean success = false;
        try {
            upLoadLimiter.acquire();
            permitAcquired = true;
            Exception lastFailure = null;
            for (int attempt = 0; attempt < 3; attempt++) {
                try {
                    if (executeUpload(upload)) {
                        success = true;
                        break;
                    }
                    lastFailure = new CoreException("S3 response verification failed");
                } catch (Exception error) {
                    if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Upload interrupted");
                    lastFailure = error;
                }
                if (attempt < 2) Thread.sleep(1000);
            }
            if (!success) throw new CoreException("Failed to upload block: " + lastFailure);
            // Complete a Future only after the upload acknowledgement is durable.
            // 中文：远端 PUT 成功还不足以通知用户；确认位持久化失败也必须进入失败路径并保留 WAL。
            // English: Remote PUT success alone cannot notify users; acknowledgement persistence failure must follow the failure path and retain WAL.
            success = false;
            upload.file().ackUpLoadPosition(upload.index());
            upload.metadata().markUploadSuccess();
            success = true;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            failUpload(upload, interrupted);
        } catch (Exception error) {
            failUpload(upload, error);
        } finally {
            if (permitAcquired) upLoadLimiter.release();
            releaseUpload(upload);
        }
        // A synchronous user continuation may submit another write: return pool capacity first.
        // 中文：先释放池容量和 WAL 引用，再由 metadata 独立派发通知；用户同步回调可继续写入或关闭实例。
        // English: Return pool capacity and WAL references before metadata dispatches independent notifications, allowing synchronous user callbacks to write again or close the instance.
        if (success) upload.metadata().completeAllFuture();
        else upload.metadata().failAllFuture();
    }

    /**
     * 中文：同步上传捕获长度内的原始 value 字节；要求非空 ETag，启用时另检查 HEAD 长度，不将 ETag 视作通用内容摘要。
     * English: Synchronously uploads raw value bytes within the captured length; requires a nonblank ETag and optionally checks HEAD length, without treating ETag as a general content digest.
     * @param upload 持有稳定 payload 的上传任务；English: upload task holding stable payload
     * @return 响应与可选长度校验是否通过；非中断 SDK 异常由调用层按上限重试；English: whether response and optional length verification passed; the caller retries non-interrupted SDK failures within its limit
     */
    private boolean executeUpload(Upload upload) {
        PutObjectRequest request = PutObjectRequest.builder().bucket(manager.bucketName).key(upload.key()).build();
        PutObjectResponse response = s3Client.putObject(request,
                RequestBody.fromByteBuffer(upload.block().getWriteMemorySegment(0, upload.size()).asByteBuffer()));
        if (response.eTag() == null || response.eTag().isBlank()) return false;
        return !enableHeadCheck || s3Client.headObject(HeadObjectRequest.builder()
                .bucket(manager.bucketName).key(upload.key()).build()).contentLength() == upload.size();
    }

    /**
     * 中文：使用捕获的逻辑坐标登记终态与死信，保留未确认 WAL；此方法不自行释放资源或通知 Future。
     * English: Registers failure and a dead-letter locator using captured coordinates, retaining unacknowledged WAL; does not itself release resources or notify Futures.
     * @param upload 捕获身份的任务；English: task with captured identity
     * @param failure 导致终结的异常；English: terminating failure
     */
    private void failUpload(Upload upload, Exception failure) {
        log.error("S3 upload failed, instance={}, bucket={}, key={}", manager.instanceName, manager.bucketName, upload.key(), failure);
        manager.blockMetaDataManager.markUploadFailed(upload.fileOffset(), upload.index(),
                new DeadDataInfo(manager.instanceName, manager.bucketName, upload.fileOffset(), upload.index(), upload.key()));
    }

    /**
     * 中文：每个已登记任务仅调用一次：标记延迟清理并归还块租约，再释放 WAL 引用与任务计数；引用归零的块可能立即被复用。
     * English: Called once per registered task: requests deferred cleanup and releases its block lease, then releases the WAL reference and task count; a zero-reference block may be reused immediately.
     * @param upload 需要结算的已登记任务；English: registered task to settle
     */
    private void releaseUpload(Upload upload) {
        try {
            synchronized (upload.metadata()) {
                upload.block().setDelayClean();
                upload.block().releaseReference();
            }
        } finally {
            // 中文：块释放后禁止从 block 读取旧身份；WAL 与计数使用任务快照完成结算。
            // English: After block release, do not read former identity from the block; settle WAL and accounting through the task snapshot.
            upload.file().release();
            manager.upCount.decrementAndGet();
        }
    }

    /**
     * 中文：先正常关闭并等待三十秒，必要时中断任务再等待三十秒；仍有任务则抛错让上层保留原生内存，不等待独立 Future 通知线程或关闭借用的 S3 客户端。
     * English: Shuts down gracefully for up to thirty seconds, then interrupts tasks and waits up to thirty more seconds if necessary; remaining tasks cause failure so upstream retains native memory. Independent Future notification threads and the borrowed S3 client are not closed here.
     * @throws IllegalStateException 取消后上传仍未退出，或第二次等待被中断；English: uploads remain after cancellation or the second wait is interrupted
     */
    public void close() {
        s3VirtualExecutor.shutdown();
        boolean interrupted = false;
        try {
            try {
                if (s3VirtualExecutor.awaitTermination(30, TimeUnit.SECONDS)) return;
            } catch (InterruptedException error) {
                interrupted = true;
            }
            s3VirtualExecutor.shutdownNow();
            try {
                if (!s3VirtualExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Uploads still running; refusing to close their native memory");
                }
            } catch (InterruptedException error) {
                interrupted = true;
                throw new IllegalStateException("Interrupted while waiting for uploads; native memory retained", error);
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }
}
