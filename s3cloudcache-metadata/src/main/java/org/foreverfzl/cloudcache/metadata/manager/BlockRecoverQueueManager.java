package org.foreverfzl.cloudcache.metadata.manager;

import org.foreverfzl.cloudcache.metadata.entity.RecoverTask;
import org.foreverfzl.cloudcache.metadata.entity.UploadTask;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 如果物理Block写入数据失败，对应的元数据isBroken为true
 * 然后等该block封口的时候去判断isBroken是否为true，如果为true则创建一个恢复数据任务
 * 该任务本质就是去对应文件的对应逻辑block中读取数据重新放入到物理Block中。
 */
public class BlockRecoverQueueManager {
    /**
     * 上传任务队列
     */
    private final BlockingQueue<RecoverTask> uploadQueue = new LinkedBlockingQueue<>();

    /**
     * 提交上传任务。
     *
     * @param task 上传任务
     * @return true 表示成功加入队列；false 表示该 Block 已经在队列中。
     */
    public boolean submit(RecoverTask task) {
        return uploadQueue.offer(task);
    }

    /**
     * Core 上传线程阻塞获取任务。
     */
    public RecoverTask take() throws InterruptedException {
        return uploadQueue.take();
    }

    /**
     * 非阻塞获取一个上传任务。
     */
    public RecoverTask poll() {
        return uploadQueue.poll();
    }

    /**
     * 当前等待上传的任务数量。
     */
    public int size() {
        return uploadQueue.size();
    }

    /**
     * 当前是否没有待上传任务。
     */
    public boolean isEmpty() {
        return uploadQueue.isEmpty();
    }

    /**
     * 清空所有上传任务。
     */
    public void clear() {
        uploadQueue.clear();
    }

}
