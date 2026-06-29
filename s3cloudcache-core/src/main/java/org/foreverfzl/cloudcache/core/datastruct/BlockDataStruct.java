package org.foreverfzl.cloudcache.core.datastruct;

import java.lang.foreign.MemorySegment;

/**
 * CoreBlock协议格式
 */
public interface BlockDataStruct {

    void writeTo(MemorySegment target);

    String getFileName();

    int getBlockIndex();

    int getDataLen();

}
