package org.foreverfzl.cloudcache.core.datastruct;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * 针对堆内数据
 */
public class HeapBlockDataStruct implements BlockDataStruct{

    private final String fileName;
    private final int blockIndex;
    private final int fromOffset;
    private final int dataLen;
    private final byte[] dataBytes;


    public HeapBlockDataStruct(String fileName, int blockIndex, byte[] dataBytes, int fromOffset, int dataLen) {
        this.fileName = fileName;
        this.blockIndex = blockIndex;
        this.dataBytes = dataBytes;
        this.fromOffset=fromOffset;
        this.dataLen=dataLen;
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

    @Override
    public void writeTo(MemorySegment target) {
        MemorySegment.copy(
                dataBytes,
                fromOffset,
                target,
                ValueLayout.JAVA_BYTE,
                0,
                dataLen
        );
    }
}
