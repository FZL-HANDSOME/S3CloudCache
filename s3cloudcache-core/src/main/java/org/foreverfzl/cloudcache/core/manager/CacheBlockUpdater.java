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
public class CacheBlockUpdater {
    private static final Logger log = LoggerFactory.getLogger(LogName.CACHE_BLOCK_UPDATER);
    private final CacheBlockManager manager;
    private final S3Client s3Client;
    private final ExecutorService s3VirtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final Semaphore upLoadLimiter;
    private final boolean enableHeadCheck;

    private record Upload(CloudCacheBlock block, BlockMetaData metadata, DefaultMappedFile file,
                          long fileOffset, int index, String key, long size) { }

    public CacheBlockUpdater(CacheBlockManager manager, int maximumUploads, S3Client s3Client, boolean enableHeadCheck) {
        if (maximumUploads <= 0) throw new IllegalArgumentException("maximumUploads must be positive");
        this.manager = manager;
        this.s3Client = s3Client;
        this.upLoadLimiter = new Semaphore(maximumUploads);
        this.enableHeadCheck = enableHeadCheck;
    }

    public void upLoadBlock(CloudCacheBlock block) {
        if (block == null) return;
        BlockMetaData metadata = block.getBlockMetaData();
        if (metadata == null) return;
        Upload upload;
        synchronized (metadata) {
            if (block.getBlockMetaData() != metadata || block.isDelayClean()
                    || manager.getExistingBlock(block.getFileFromOffset(), block.getLogicalIndex()) != block
                    || block.getReferenceCount() != 0 || !metadata.tryStartUpload()) return;
            upload = new Upload(block, metadata, block.getDefaultMappedFile(), block.getFileFromOffset(),
                    block.getLogicalIndex(), block.getS3Key(), block.getWritePosition());
            block.setUnActive();
            block.getReference();
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
        if (success) upload.metadata().completeAllFuture();
        else upload.metadata().failAllFuture();
    }

    private boolean executeUpload(Upload upload) {
        PutObjectRequest request = PutObjectRequest.builder().bucket(manager.bucketName).key(upload.key()).build();
        PutObjectResponse response = s3Client.putObject(request,
                RequestBody.fromByteBuffer(upload.block().getWriteMemorySegment(0, upload.size()).asByteBuffer()));
        if (response.eTag() == null || response.eTag().isBlank()) return false;
        return !enableHeadCheck || s3Client.headObject(HeadObjectRequest.builder()
                .bucket(manager.bucketName).key(upload.key()).build()).contentLength() == upload.size();
    }

    private void failUpload(Upload upload, Exception failure) {
        log.error("S3 upload failed, instance={}, bucket={}, key={}", manager.instanceName, manager.bucketName, upload.key(), failure);
        manager.blockMetaDataManager.markUploadFailed(upload.fileOffset(), upload.index(),
                new DeadDataInfo(manager.instanceName, manager.bucketName, upload.fileOffset(), upload.index(), upload.key()));
    }

    private void releaseUpload(Upload upload) {
        try {
            synchronized (upload.metadata()) {
                upload.block().setDelayClean();
                upload.block().releaseReference();
            }
        } finally {
            upload.file().release();
            manager.upCount.decrementAndGet();
        }
    }

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
