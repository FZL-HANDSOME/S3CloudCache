package org.foreverfzl.cloudcache.core.datastruct;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * 针对堆外数据
 */
public class DirectBlockDataStruct implements BlockDataStruct{

    private final long fileFromOffset;
    private final int blockIndex;
    private final int fromOffset;
    private final int dataLen;
    private final MemorySegment dataSegment;

    public DirectBlockDataStruct(long fileFromOffset, int blockIndex, int fromOffset, int dataLen, MemorySegment dataSegment) {
        this.fileFromOffset = fileFromOffset;
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
                    fromOffset,
                    target,
                    ValueLayout.JAVA_BYTE,
                    0,
                    dataLen
            );
            return true;
        }catch (Exception e){
            return false;
        }
    }

    @Override
    public long getFileFromOffset() {
        return fileFromOffset;
    }


    @Override
    public int getBlockIndex() {
        return blockIndex;
    }

    @Override
    public int getDataLen() {
        return dataLen;
    }

}
