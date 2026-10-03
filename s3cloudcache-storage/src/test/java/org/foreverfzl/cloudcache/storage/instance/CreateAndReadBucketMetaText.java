package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.wal.Util.BucketMetaInfoUtil;
import org.foreverfzl.cloudcache.wal.datastruct.BucketMetaInfo;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 中文：历史手动元数据示例，使用硬编码真实本地目录，不属于离线 JUnit；运行前必须确认目标目录没有需要保留的数据。
 * English: Legacy manual metadata demo targeting a hard-coded local directory, not offline JUnit; verify the target has no data that must be preserved before running.
 * 中文：当前示例未关闭静态 Arena，不应复制这种资源管理方式到生产；正式调用者必须显式关闭其映射拥有者。
 * English: This demo leaves its static arena open; do not copy that ownership pattern into production, where mapping owners must be explicitly closed.
 */
public class CreateAndReadBucketMetaText {

    /** 中文：示例映射所有者，当前生命周期延续到进程退出；English: demo mapping owner whose current lifetime extends to process exit. */
    static Arena arena = Arena.ofShared();

    /**
     * 中文：创建/映射并回读 bucketMeta，可能改变指定目录的 dirty 状态；没有自动断言，不代表持久化回归通过。
     * English: Creates/maps and reads bucketMeta, potentially changing dirty state; there are no assertions, so this is not a passing durability regression.
     * @param args 中文：未使用；English: unused.
     */
    static void main(String[] args) {
        Path path = Paths.get("C:/Users/21653/CloudCache/store/textinstance/textbucket");
        BucketMetaInfoUtil.createAndMapBucketMetaFile(new BucketMetaInfo(8*1024*1024,32*1024*1024L,"aaa"),path,arena);
        BucketMetaInfo bucketMetaInfo = BucketMetaInfoUtil.readBucketMetaFile(path);
    }


}
