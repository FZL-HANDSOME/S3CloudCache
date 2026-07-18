package org.foreverfzl.cloudcache.core.manager;

import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.LogName;
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
        // 扔给执行器托管，解决优雅停机和追踪问题
        s3VirtualExecutor.submit(() -> {
            try {
                // 3. 必须先抢到网络准入令牌，抢不到的虚拟线程会被 JVM 自动、高效地卸载挂起
                upLoadLimiter.acquire();
                //先将元数据改为上传中
                long fileFromOffset = block.getFileFromOffset();
                int logicalIndex = block.getLogicalIndex();
                manager.blockMetaDataManager.tryStartUpload(fileFromOffset, logicalIndex);
                // 执行重度网络 I/O
                boolean isSuccess = executeUpload(block);
                if (isSuccess) {
                    //将元数据设置为上传成功
                    manager.blockMetaDataManager.markUploadSuccess(fileFromOffset, logicalIndex);
                    //然后ack确认上传指针
                    DefaultMappedFile defaultMappedFile = block.getDefaultMappedFile();
                    defaultMappedFile.ackUpLoadPosition(logicalIndex);
                    // 成功后归还内存
                    manager.recycleBlock(block);
                } else {
                    log.warn("instance={},bucket={}:Failed to upload this block=>{}", manager.instanceName, manager.bucketName, block);
                    handleUploadFailure(block);
                }
            } catch (Throwable t) {
                log.warn("Exception is={},instance={},bucket={}:Failed to upload this block=>{}", t, manager.instanceName, manager.bucketName, block);
                handleUploadFailure(block);
            } finally {
                // 4. 无论成功失败，释放令牌，让下一个块上云
                upLoadLimiter.release();
            }
        });
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
        //将元数据设置为失败
        manager.blockMetaDataManager.markUploadFailed(block.getFileFromOffset(), block.getLogicalIndex());
    }

    public void close() throws Exception {
        s3VirtualExecutor.shutdown();
    }
}
