package org.foreverfzl.cloudcache.metadata.entity;

/**
 * 中文：表示从 WAL 重放一个逻辑块的候选；不持有资源引用，也不自行决定何时恢复或丢弃。
 * English: Candidate for replaying one logical block from WAL; it holds no resource references and does not decide when to recover or discard.
 */
public class RecoverTask {

    /**
     * 中文：目标 WAL 文件的逻辑起始字节偏移。
     * English: Logical starting byte offset of the target WAL file.
     */
    private final long fileFromOffset;
    /**
     * 中文：目标文件内零基逻辑块索引。
     * English: Zero-based logical block index in the target file.
     */
    private final int blockIndex;
    /**
     * 中文：补充更正：此字段仅为显式调用 incrementTimes 的计数；当前恢复消费者不按此值丢弃任务，也不保证并发原子性。
     * English: Correction: counts explicit incrementTimes calls only; the current recovery consumer does not discard by this value, and increments are not atomic.
     */
    private int times; //被取出放回的次数，如果超过3次该任务就会从队列中删除

    /**
     * 中文：保存恢复定位，并将历史重排队计数初始化为 0。
     * English: Stores recovery coordinates and initializes the legacy requeue count to zero.
     * @param fileFromOffset WAL 文件逻辑起始字节偏移；English: Logical WAL file start in bytes.
     * @param blockIndex 文件内逻辑块索引；English: Logical block index within the file.
     */
    public RecoverTask(long fileFromOffset, int blockIndex) {
        this.fileFromOffset = fileFromOffset;
        this.blockIndex = blockIndex;
        times = 0;
    }

    /**
     * 中文：返回待恢复文件的逻辑基址。
     * English: Returns the logical base offset of the file to recover.
     * @return 文件逻辑起始字节偏移；English: Logical file start in bytes.
     */
    public long getFileFromOffset() {
        return fileFromOffset;
    }

    /**
     * 中文：返回待恢复块索引。
     * English: Returns the block index to recover.
     * @return 零基逻辑索引；English: Zero-based logical index.
     */
    public int getBlockIndex() {
        return blockIndex;
    }

    /**
     * 中文：对历史重排队计数加一；不会重排队或丢弃任务，须由调用方协调并发。
     * English: Increments the legacy requeue count without requeuing or discarding; callers coordinate concurrent access.
     */
    public void incrementTimes() {
        times++;
    }

    /**
     * 中文：读取历史计数值，不代表自动重试上限。
     * English: Reads the legacy count, not an automatic retry limit.
     * @return 显式增量调用累计值；English: Accumulated explicit increments.
     */
    public int getTimes() {
        return times;
    }
}
