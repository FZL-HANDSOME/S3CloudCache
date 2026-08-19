package org.foreverfzl.cloudcache.core.manager;

import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudcache.metadata.entity.DeadDataInfo;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.exception.CoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

/**
 * 专门用于将Block上传到S3服务器
 */
public class CacheBlockUpdater {

    private static final Logger log = LoggerFactory.getLogger(LogName.CACHE_BLOCK_UPDATER);
    private final CacheBlockManager manager;
    private final S3Client s3Client;

    // 1. 引入专用的虚拟线程执行器（它负责管辖和优雅停机，但不限制并发数）
    private final ExecutorService s3VirtualExecutor;

    // 2. 核心控制阀：利用信号量，把对 S3 的真正网络并发限制在安全范围内
    private final Semaphore upLoadLimiter;

    private boolean enableHeadCheck = false;

    public CacheBlockUpdater(CacheBlockManager manager, int blockUpLoadMaxCount, S3Client s3Client, boolean enableHeadCheck) {
        this.manager = manager;
        this.s3Client = s3Client;
        s3VirtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
        upLoadLimiter = new Semaphore(blockUpLoadMaxCount);
        this.enableHeadCheck = enableHeadCheck;
    }

    /**
     * 采用虚拟线程将Block上传到S3服务器，并且限制并发数量
     */
    public void upLoadBlock(CloudCacheBlock block) {
        if (block == null) {
            throw new IllegalArgumentException("block cannot be null");
        }
        long fileFromOffset = block.getFileFromOffset();
        int logicalIndex = block.getLogicalIndex();
        if (!manager.blockMetaDataManager.tryStartUpload(fileFromOffset, logicalIndex)) {
            //如果CAS标记为上传中 失败，说明其它线程进行上传了
            return;
        }
        // 在任务提交前就登记并持有 Block，避免关闭流程在任务尚未调度时误判上传已完成。
        block.getReference();
        manager.upCount.incrementAndGet();
        try {
//            executeUploadTask(block);
            s3VirtualExecutor.submit(() -> executeUploadTask(block));
        } catch (RejectedExecutionException e) {
            manager.upCount.decrementAndGet();
            block.releaseReference();
            throw e;
        }
    }

    private void executeUploadTask(CloudCacheBlock block) {
        boolean permitAcquired = false;
        try {
            upLoadLimiter.acquire();
            permitAcquired = true;
            long fileFromOffset = block.getFileFromOffset();
            int logicalIndex = block.getLogicalIndex();
            boolean isSuccess = false;
            for (int i = 0; i < 3; i++) {
//                isSuccess = executeUpload(block);
                if (isSuccess) {
                    manager.blockMetaDataManager.markUploadSuccess(fileFromOffset, logicalIndex);
                    block.getDefaultMappedFile().ackUpLoadPosition(logicalIndex);
                    //将block标记为清除回收
                    block.setClean();
                    break;
                }
                Thread.sleep(1000);
            }
            if (!isSuccess) {
                throw new CoreException("Failed to upload block");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            handleUploadFailure(block);
        } catch (Exception e) {
            log.error("S3 upload failed, instance={}, bucket={}, block={}",
                    manager.instanceName, manager.bucketName, block, e);
            handleUploadFailure(block);
        } finally {
            if (permitAcquired) {
                upLoadLimiter.release();
            }
            manager.upCount.decrementAndGet();
            block.releaseReference();
        }
    }


    /**
     * 真正执行上传的方法，使用S3client进行上传
     */
    private boolean executeUpload(CloudCacheBlock block) {
        // 只上传实际写入的数据
        ByteBuffer byteBuffer = block.getUpdateMemorySegment().asByteBuffer();
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(manager.bucketName)
                .key(block.getS3Key())
                .build();
        PutObjectResponse response = s3Client.putObject(request, RequestBody.fromByteBuffer(byteBuffer));
        // 方式1：校验ETag（文件唯一指纹，不为空说明文件落地）
        String etag = response.eTag();
        if (etag == null || etag.isBlank()) {
            return false;
        }
        //进行一次主动校验，看看是否上传成功
        if (enableHeadCheck) {
            HeadObjectRequest headReq = HeadObjectRequest.builder()
                    .bucket(manager.bucketName)
                    .key(block.getS3Key())
                    .build();
            HeadObjectResponse headResp = s3Client.headObject(headReq);
            if (headResp.contentLength() != block.getWritePosition()) {
                return false;
            }
        }
        return true;
    }

    /**
     * 上传失败后执行
     */
    private void handleUploadFailure(CloudCacheBlock block) {
        //将元数据设置为失败，并上传到死信队列中
        CacheBlockManager cacheBlockManager = block.getManager();
        DeadDataInfo deadDataInfo = new DeadDataInfo(cacheBlockManager.instanceName, cacheBlockManager.bucketName,
                block.getFileFromOffset(), block.getLogicalIndex(), block.getS3Key());
        //将block标记为清除回收
        block.setClean();
        manager.blockMetaDataManager.markUploadFailed(block.getFileFromOffset(), block.getLogicalIndex(), deadDataInfo);
    }

    public void close() {
        s3VirtualExecutor.shutdownNow();
    }
}
