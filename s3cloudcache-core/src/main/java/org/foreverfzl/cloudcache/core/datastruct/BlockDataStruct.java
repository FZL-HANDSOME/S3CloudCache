package org.foreverfzl.cloudcache.core.datastruct;

import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;

import java.lang.foreign.MemorySegment;

/**
 * CoreBlock协议格式
 */
public interface BlockDataStruct {

    boolean writeTo(MemorySegment target);


    int getBlockIndex();

    int getDataLen();

    DefaultMappedFile getDefaultMappedFile();



}
