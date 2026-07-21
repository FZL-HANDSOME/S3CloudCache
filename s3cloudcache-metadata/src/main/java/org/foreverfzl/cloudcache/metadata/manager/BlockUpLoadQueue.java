package org.foreverfzl.cloudcache.metadata.manager;

import org.foreverfzl.cloudcache.metadata.entity.UploadTask;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 一部分上传任务会放入到这个类中队列中，core模块监听这个队列，如有新的任务则进行上传
 */
public class BlockUpLoadQueue {
    /**
     * 上传任务队列
     */
    private final BlockingQueue<UploadTask> uploadQueue = new LinkedBlockingQueue<>();

    /**
     * 提交上传任务。
     *
     * @param task 上传任务
     * @return true 表示成功加入队列；false 表示该 Block 已经在队列中。
     */
    public boolean submit(UploadTask task) {
        return uploadQueue.offer(task);
    }

    /**
     * Core 上传线程阻塞获取任务。
     */
    public UploadTask take() throws InterruptedException {
        return uploadQueue.take();
    }

    /**
     * 非阻塞获取一个上传任务。
     */
    public UploadTask poll() {
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
