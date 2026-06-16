package org.foreverfzl.cloudcache.core.cache;

public class BlockDataStruct {
    //blockKey由 文件名+逻辑block组成
    private final String blockKey;
    private final byte[] data;

    public BlockDataStruct(String uniqueKey, String blockKey, byte[] data) {

        this.blockKey = blockKey;
        this.data = data;
    }


    public String getBlockKey() {
        return blockKey;
    }

    public byte[] getData() {
        return data;
    }
}
