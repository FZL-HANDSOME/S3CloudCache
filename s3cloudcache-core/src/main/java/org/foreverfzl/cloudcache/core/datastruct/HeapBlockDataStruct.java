package org.foreverfzl.cloudcache.core.datastruct;

import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * 针对堆内数据
 */
public class HeapBlockDataStruct implements BlockDataStruct{

    private final DefaultMappedFile defaultMappedFile;
    private final int blockIndex;
    private final int fromOffset;
    private final int dataLen;
    private final byte[] dataBytes;


    public HeapBlockDataStruct(DefaultMappedFile defaultMappedFile, int blockIndex, byte[] dataBytes, int fromOffset, int dataLen) {
        this.defaultMappedFile = defaultMappedFile;
        this.blockIndex = blockIndex;
        this.dataBytes = dataBytes;
        this.fromOffset=fromOffset;
        this.dataLen=dataLen;
    }

    @Override
    public boolean writeTo(MemorySegment target) {
        try {
            MemorySegment.copy(
                    dataBytes,
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
