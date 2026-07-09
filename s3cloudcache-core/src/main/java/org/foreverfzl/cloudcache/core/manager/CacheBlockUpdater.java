package org.foreverfzl.cloudcache.core.manager;

import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.exception.CloudCacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;


import java.lang.foreign.MemorySegment;
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

    public CacheBlockUpdater(CacheBlockManager manager, int blockUpLoadMaxCount, S3Client s3Client) {
        this.manager = manager;
        this.s3Client = s3Client;
        s3VirtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
        upLoadLimiter = new Semaphore(blockUpLoadMaxCount);
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

                // 执行重度网络 I/O
                executeUpload(block);
                // 成功后归还内存
                manager.recycleBlock(block);
            } catch (Throwable t) {
                log.warn("instance={},bucket={}:Failed to upload this block=>{}", manager.instanceName, manager.bucketName, block);
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
    private void executeUpload(CloudCacheBlock block) {
        // 只上传实际写入的数据
        ByteBuffer byteBuffer = block.getUpdateMemorySegment().asByteBuffer();
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(manager.bucketName)
                .key(block.getS3Key())
                .build();
        s3Client.putObject(request, RequestBody.fromByteBuffer(byteBuffer));
    }

    /**
     * 上传失败后执行
     */
    private void handleUploadFailure(CloudCacheBlock block) {

    }

    public void close() throws Exception {
        s3VirtualExecutor.shutdown();
    }
}
