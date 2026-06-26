package org.foreverfzl.cloudcache.core.cache;

public class BlockDataStruct {
    private final String fileName;
    private final int blockIndex;
    private final byte[] data;


    public BlockDataStruct(String fileName, int blockIndex, byte[] data) {
        this.fileName = fileName;
        this.blockIndex = blockIndex;
        this.data = data;
    }

    public String getFileName() {
        return fileName;
    }

    public int getBlockIndex() {
        return blockIndex;
    }

    public byte[] getData() {
        return data;
    }
}
