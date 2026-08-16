package org.foreverfzl.cloudcache.metadata.manager;

import org.foreverfzl.cloudcache.metadata.entity.UploadTask;
import org.foreverfzl.cloudchache.common.LogName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 一部分上传任务会放入到这个类中队列中，core模块监听这个队列，如有新的任务则进行上传
 */
public class BlockUpLoadQueue {
    private static final Logger log = LoggerFactory.getLogger(LogName.UPLOAD_QUEUE);
    /**
     * 上传任务队列
     */
    private final BlockingQueue<UploadTask> queue = new LinkedBlockingQueue<>();

    /**
     * 提交上传任务。
     *
     * @param task 上传任务
     * @return true 表示成功加入队列；false 表示该 Block 已经在队列中。
     */
    public boolean submit(UploadTask task) {
        log.info("fileFromOffset={},blockIndex={} is submited to the upLoadQueue", task.getFileFromOffset(), task.getLogicalIndex());
        return queue.offer(task);
    }

    /**
     * Core 上传线程阻塞获取任务。
     */
    public UploadTask take() throws InterruptedException {
        return queue.take();
    }

    /**
     * 非阻塞获取一个上传任务。
     */
    public UploadTask poll() {
        return queue.poll();
    }

    /**
     * 当前等待上传的任务数量。
     */
    public int size() {
        return queue.size();
    }

    /**
     * 当前是否没有待上传任务。
     */
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * 清空所有上传任务。
     */
    public void clear() {
        queue.clear();
    }
}
