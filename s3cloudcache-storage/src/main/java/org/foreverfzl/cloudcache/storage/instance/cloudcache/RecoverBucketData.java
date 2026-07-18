package org.foreverfzl.cloudcache.storage.instance.cloudcache;

import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;

/**
 * bucket数据恢复 的各个信息
 */
public class RecoverBucketData {

    //旧key的前缀
    private String prefix;

    //旧文件的大小
    private long fileSize;

    //旧文件的blockSize
    private int blockSize;

    //最后文件的起始位置，该文件之前的文件需要数据恢复
    private long endFileFromOffset;

    //wal管理者
    private MappedFileManager mappedFileManager;

    //block管理者
    private CacheBlockManager cacheBlockManager;




}
