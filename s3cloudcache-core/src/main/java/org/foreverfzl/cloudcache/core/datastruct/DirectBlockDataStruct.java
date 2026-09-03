package org.foreverfzl.cloudcache.core.datastruct;

import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * 针对堆外数据
 */
public class DirectBlockDataStruct implements BlockDataStruct{

    private static final Logger log = LoggerFactory.getLogger(DirectBlockDataStruct.class);

    private final DefaultMappedFile defaultMappedFile;
    private final int blockIndex;
    private final int fromOffset;
    private final int dataLen;
    private final MemorySegment dataSegment;

    public DirectBlockDataStruct(DefaultMappedFile defaultMappedFile, int blockIndex,  MemorySegment dataSegment,int fromOffset, int dataLen) {
        this.defaultMappedFile = defaultMappedFile;
        this.blockIndex = blockIndex;
        this.fromOffset = fromOffset;
        this.dataLen = dataLen;
        this.dataSegment = dataSegment;
    }

    @Override
    public boolean writeTo(MemorySegment target) {
        try {
            MemorySegment.copy(
                    dataSegment,
                    ValueLayout.JAVA_BYTE,
                    fromOffset,
                    target,
                    ValueLayout.JAVA_BYTE,
                    0,
                    dataLen
            );
            return true;
        } catch (Exception e) {
            log.error("DirectBlockDataStruct.writeTo failed: fromOffset={}, dataLen={}, srcSize={}, dstSize={}",
                    fromOffset, dataLen, dataSegment.byteSize(), target.byteSize(), e);
            return false;
        }
    }


    @Override
    public int getBlockIndex() {
        return blockIndex;
    }

    @Override
    public int getDataLen() {
        return dataLen;
    }


    @Override
    public DefaultMappedFile getDefaultMappedFile() {
        return defaultMappedFile;
    }
}
