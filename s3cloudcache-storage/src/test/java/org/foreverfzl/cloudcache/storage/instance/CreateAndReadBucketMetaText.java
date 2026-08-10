package org.foreverfzl.cloudcache.storage.instance;

import org.foreverfzl.cloudcache.wal.Util.BucketMetaInfoUtil;
import org.foreverfzl.cloudcache.wal.datastruct.BucketMetaInfo;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CreateAndReadBucketMetaText {

    static Arena arena = Arena.ofShared();

    static void main(String[] args) {
        Path path = Paths.get("C:/Users/21653/CloudCache/store/textinstance/textbucket");
        BucketMetaInfoUtil.createAndMapBucketMetaFile(new BucketMetaInfo(8*1024*1024,32*1024*1024L,"aaa"),path,arena);
        BucketMetaInfo bucketMetaInfo = BucketMetaInfoUtil.readBucketMetaFile(path);
    }


}
