package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.junit.Test;

import static org.junit.Assert.*;

public class BlockMetadataCleanupTest {
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
