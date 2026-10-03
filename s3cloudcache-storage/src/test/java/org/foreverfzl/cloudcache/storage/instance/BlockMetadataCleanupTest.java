package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * 中文：不分配 WAL/堆外内存的稀疏元数据清理回归，模拟重启跳过已确认 Block 后的索引缺口。
 * English: Sparse metadata cleanup regression without WAL/native allocation, modeling index gaps after restart skips confirmed blocks.
 */
public class BlockMetadataCleanupTest {
    /** 中文：第零块缺失也须清理后续及最大 10-bit 下标，不能误删别的文件；English: absent block zero must not hide later/maximal 10-bit indexes or affect another file. */
    @Test
    public void missingBlockZeroDoesNotLeakRecoveredMetadata() {
        BlockMetaDataManager manager = new BlockMetaDataManager();
        manager.getOrCreate(4096, 1);
        manager.getOrCreate(4096, 3);
        manager.getOrCreate(4096, 1023);
        var otherFile = manager.getOrCreate(8192, 1);
        manager.deleteFileAllBlockMetaData(4096);
        assertNull(manager.getBlockMetaData(4096, 1));
        assertNull(manager.getBlockMetaData(4096, 3));
        assertNull(manager.getBlockMetaData(4096, 1023));
        assertSame(otherFile, manager.getBlockMetaData(8192, 1));
    }

    /** 中文：内部缺口不提前终止扫描，重复删除应保持幂等；English: internal gaps must not stop scanning; repeated deletion stays idempotent. */
    @Test
    public void interiorGapAndRepeatedCleanupAreSafe() {
        BlockMetaDataManager manager = new BlockMetaDataManager();
        manager.getOrCreate(0, 0);
        manager.getOrCreate(0, 5);
        manager.deleteFileAllBlockMetaData(0);
        manager.deleteFileAllBlockMetaData(0);
        assertNull(manager.getBlockMetaData(0, 0));
        assertNull(manager.getBlockMetaData(0, 5));
    }
}
