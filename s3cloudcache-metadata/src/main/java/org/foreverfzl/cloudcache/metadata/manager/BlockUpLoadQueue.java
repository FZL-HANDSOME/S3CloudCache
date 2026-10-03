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
/**
 * 中文：Bucket 级上传候选队列；只转交任务定位，不持有物理块/WAL 租约。
 * English: Bucket-scoped upload candidate queue; transfers task coordinates without holding block/WAL leases.
 */
public class BlockUpLoadQueue {
    /**
     * 中文：记录队列提交定位的日志。
     * English: Logs coordinates of queued items.
     */
    private static final Logger log = LoggerFactory.getLogger(LogName.UPLOAD_QUEUE);
    /**
     * 上传任务队列
     */
    /**
     * 中文：无显式容量配置的 FIFO 上传候选阻塞队列；offer 不做逻辑去重，实际容量上限为 Integer.MAX_VALUE。
     * English: FIFO blocking queue of upload candidates with no configured capacity; offer does not deduplicate, and the actual capacity limit is Integer.MAX_VALUE.
     */
    private final BlockingQueue<UploadTask> queue = new LinkedBlockingQueue<>();

    /**
     * 提交上传任务。
     *
     * @param task 上传任务
     * @return true 表示成功加入队列；false 表示该 Block 已经在队列中。
     */
    /**
     * 中文：提交上传候选；补充更正：不会检测“该 Block 已在队列”，去重/资格判断由上层状态机负责。
     * English: Enqueues an upload candidate; correction: it does not detect an already queued block, so the surrounding state machine handles deduplication/eligibility.
     * @param task 非 null 的队列记录；English: Non-null queue item.
     * @return 底层 offer 是否接受，不表示上传成功；English: Whether offer accepted the item, not upload success.
     */
    public boolean submit(UploadTask task) {
        log.info("fileFromOffset={},blockIndex={} is submited to the upLoadQueue", task.getFileFromOffset(), task.getLogicalIndex());
        return queue.offer(task);
    }

    /**
     * Core 上传线程阻塞获取任务。
     */
    /**
     * 中文：等待并移除队首上传候选，供 Core 上传调度线程消费。
     * English: Waits for and removes the head upload candidate for its consumer.
     * @return 取得的上传候选；English: Dequeued upload candidate.
     * @throws InterruptedException 等待被中断；English: If waiting is interrupted.
     */
    public UploadTask take() throws InterruptedException {
        return queue.take();
    }

    /**
     * 非阻塞获取一个上传任务。
     */
    /**
     * 中文：立即移除队首上传候选；队列空时不等待。
     * English: Immediately removes the head upload candidate without waiting on an empty queue.
     * @return 队首记录，空时为 null；English: Head item, or null when empty.
     */
    public UploadTask poll() {
        return queue.poll();
    }

    /**
     * 当前等待上传的任务数量。
     */
    /**
     * 中文：读取当前待消费数量快照，不含已取出的在途任务。
     * English: Reads a queued-item count snapshot, excluding dequeued in-flight tasks.
     * @return 当前队列长度；English: Current queue length.
     */
    public int size() {
        return queue.size();
    }

    /**
     * 当前是否没有待上传任务。
     */
    /**
     * 中文：读取当前是否没有排队记录，不代表没有在途工作。
     * English: Reads whether no items are queued; in-flight work may still exist.
     * @return 队列当前是否为空；English: Whether the queue is currently empty.
     */
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * 清空所有上传任务。
     */
    /**
     * 中文：丢弃当前排队记录；不取消已取出的任务，也不完成 Future 或释放其关联资源。
     * English: Discards queued items without cancelling dequeued work, completing futures or releasing associated resources.
     */
    public void clear() {
        queue.clear();
    }
}
