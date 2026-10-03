package org.foreverfzl.cloudcache.wal.storefile;

/**
 * 中文：将文件操作接口与引用计数基类组合；不添加自动刷盘、回收或关闭行为。
 * English: Combines the file interface with reference bookkeeping; it adds no automatic flushing, reclamation, or shutdown.
 */
public abstract class AbstractMappedFile extends MappedFiledReferenceResource implements MappedFile{
}
