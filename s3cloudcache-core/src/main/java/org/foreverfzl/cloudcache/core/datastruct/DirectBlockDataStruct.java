package org.foreverfzl.cloudcache.core.datastruct;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * 针对堆外数据
 */
public class DirectBlockDataStruct implements BlockDataStruct{

    private final String fileName;
    private final int blockIndex;
    private final int blockExpectedValidBytes;
    private final int fromOffset;
    private final int dataLen;
    private final MemorySegment dataSegment;

    public DirectBlockDataStruct(String fileName, int blockIndex, int blockExpectedValidBytes, int fromOffset, int dataLen, MemorySegment dataSegment) {
        this.fileName = fileName;
        this.blockIndex = blockIndex;
        this.blockExpectedValidBytes = blockExpectedValidBytes;
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
    public String getFileName() {
        return fileName;
    }

    @Override
    public int getBlockIndex() {
        return blockIndex;
    }

    @Override
    public int getDataLen() {
        return dataLen;
    }

    public int getBlockExpectedValidBytes() {
        return blockExpectedValidBytes;
    }
}
