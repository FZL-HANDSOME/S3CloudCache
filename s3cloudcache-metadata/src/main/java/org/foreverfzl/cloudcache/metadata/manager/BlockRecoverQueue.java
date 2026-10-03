package org.foreverfzl.cloudcache.metadata.manager;

import org.foreverfzl.cloudcache.metadata.entity.RecoverTask;
import org.foreverfzl.cloudchache.common.LogName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 如果物理Block写入数据失败，对应的元数据isBroken为true
 * 先判断该block是否封口，如果封口了则建一个恢复数据任务
 * 否则等该block封口的时候去判断isBroken是否为true，如果为true则创建一个恢复数据任务
 * 该任务本质就是去对应文件的对应逻辑block中读取数据重新放入到物理Block中。
 */
/**
 * 中文：Bucket 级 WAL 重放候选队列；只转交任务定位，不持有物理块/WAL 租约。
 * English: Bucket-scoped WAL replay candidate queue; transfers task coordinates without holding block/WAL leases.
 */
public class BlockRecoverQueue {
    /**
     * 中文：记录队列提交定位的日志。
     * English: Logs coordinates of queued items.
     */
    private static final Logger log = LoggerFactory.getLogger(LogName.RECOVE_RQUEUE);
    /**
     * 上传任务队列
     */
    /**
     * 中文：实际保存恢复候选而非上方旧称“上传任务”的 FIFO 阻塞队列；offer 不去重，未配置容量时上限为 Integer.MAX_VALUE。
     * English: FIFO blocking queue of recovery candidates, not the upload tasks described above; offer does not deduplicate and the default capacity is Integer.MAX_VALUE.
     */
    private final BlockingQueue<RecoverTask> queue = new LinkedBlockingQueue<>();

    /**
     * 提交上传任务。
     *
     * @param task 上传任务
     * @return true 表示成功加入队列；false 表示该 Block 已经在队列中。
     */
    /**
     * 中文：提交 WAL 重放候选；补充更正：不会检测“该 Block 已在队列”，去重/资格判断由上层状态机负责。
     * English: Enqueues a WAL replay candidate; correction: it does not detect an already queued block, so the surrounding state machine handles deduplication/eligibility.
     * @param task 非 null 的队列记录；English: Non-null queue item.
     * @return 底层 offer 是否接受，不表示恢复成功；English: Whether offer accepted the item, not recovery success.
     */
    public boolean submit(RecoverTask task) {
        log.info("fileFromOffset={},blockIndex={} is submited to the recovery queue", task.getFileFromOffset(), task.getBlockIndex());
        return queue.offer(task);
    }

    /**
     * Core 上传线程阻塞获取任务。
     */
    /**
     * 中文：等待并移除队首 WAL 重放候选，供 storage 恢复线程消费。
     * English: Waits for and removes the head WAL replay candidate for its consumer.
     * @return 取得的 WAL 重放候选；English: Dequeued WAL replay candidate.
     * @throws InterruptedException 等待被中断；English: If waiting is interrupted.
     */
    public RecoverTask take() throws InterruptedException {
        return queue.take();
    }

    /**
     * 非阻塞获取一个上传任务。
     */
    /**
     * 中文：立即移除队首 WAL 重放候选；队列空时不等待。
     * English: Immediately removes the head WAL replay candidate without waiting on an empty queue.
     * @return 队首记录，空时为 null；English: Head item, or null when empty.
     */
    public RecoverTask poll() {
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
