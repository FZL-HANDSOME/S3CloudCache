package org.foreverfzl.cloudcache.metadata.manager;

/**
 * 如果物理Block写入数据失败，对应的元数据isBroken为true
 * 然后等该block封口的时候去判断isBroken是否为true，如果为true则创建一个恢复数据任务
 * 该任务本质就是去对应文件的对应逻辑block中读取数据重新放入到物理Block中。
 */
public class BlockRecoverQueueManager {
}
