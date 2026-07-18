package org.foreverfzl.cloudcache.storage.instance.cloudcache;


import org.foreverfzl.cloudcache.core.global.CoreInstanceBucketManager;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterWriter;
import org.foreverfzl.cloudcache.wal.datastruct.MetaInfo;
import org.foreverfzl.cloudcache.wal.global.WalInstanceBucketManager;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.foreverfzl.cloudchache.common.exception.CloudCacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;
import java.util.zip.CRC32;


public class S3CloudCacheInstance extends AbstractCloudCacheInstance {
    private static final Logger log = LoggerFactory.getLogger(LogName.CLOUD_CACHE_INSTANCE);
    /**
     * 名字一定要唯一并且不要更改
     */
    protected final String instanceName;

    /**
     * 全局配置文件
     */
    private final S3CloudCacheConfig config;
    /**
     * 持久化WAL文件管理者
     */
    private final WalInstanceBucketManager walInstanceBucketManager;
    /**
     * Cache的管理者
     */
    private final CoreInstanceBucketManager coreInstanceBucketManager;

    private final S3Client s3Client;


    public S3CloudCacheInstance(S3Client s3Client, S3CloudCacheConfig config) {
        this.s3Client = s3Client;
        this.config = config;
        this.instanceName = config.instanceName;
        config.walPath = config.walPath != null ?
                config.walPath + ProjectUtil.WAL_FILE_ADDRESS :
                ProjectUtil.USER_HOME + ProjectUtil.WAL_FILE_ADDRESS;
        walInstanceBucketManager = new WalInstanceBucketManager(instanceName, config);
        coreInstanceBucketManager = new CoreInstanceBucketManager(instanceName, s3Client, config);
    }

    /**
     * 获取BucketName对应的Bucket操作句柄
     */
    public BucketWriterWriter getBucketWriterInstance(String bucketName) {
        if (bucketName == null || bucketName.isBlank()) {
            throw new CloudCacheException("bucketName can not null");
        }
        //去容器中看看有没有对应的manager，有则返回，没有则创建
        MappedFileManager bucketWalManager = walInstanceBucketManager.getOrCreateBucketFileManager(bucketName);
        CacheBlockManager bucketCoreManager = coreInstanceBucketManager.getOrCreateBlockManager(bucketName, bucketWalManager.blockMetaDataManager);
        return new BucketWriterWriter(bucketName, bucketWalManager, bucketCoreManager, bucketWalManager.blockMetaDataManager, this);
    }

    //start负责扫描目录、恢复数据等
    public void start() {
        //检查指定目录下instanceName的所有目录和文件,方法结束后所有的东西都恢复到结束前的状态，并且recoverBucketsMap填写好
        try (Stream<Path> bucketList = Files.list(Paths.get(config.walPath + File.separator + instanceName));) {
            //遍历该instance下的所有bucket
            bucketList.forEach(path -> {
                try {
                    this.chackDirectoryAndFile(path, path.getFileName().toString());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

        } catch (Exception e) {

        }
    }

    private void chackDirectoryAndFile(Path path, String bucketName) throws IOException {
        if (path == null || !Files.exists(path)) {
            throw new CloudCacheException("bucket directory not exists: " + path);
        }
        if (!Files.isDirectory(path)) {
            throw new CloudCacheException("path is not directory: " + path);
        }
        //创建该bucket对应manager
        MappedFileManager fileManager = walInstanceBucketManager.getOrCreateBucketFileManager(bucketName);
        CacheBlockManager blockManager = coreInstanceBucketManager.getOrCreateBlockManager(bucketName, fileManager.blockMetaDataManager);

        /*
         * 1.读取bucketMeta
         */
        Path bucketMetaPath = path.resolve("bucketMeta");
        if (!Files.exists(bucketMetaPath) || !Files.isRegularFile(bucketMetaPath)) {
            throw new CloudCacheException("bucketMeta not exists: " + bucketMetaPath);
        }
        byte[] metaBytes = Files.readAllBytes(bucketMetaPath);
        MemorySegment segment = MemorySegment.ofArray(metaBytes);
        long pos = 0;
        // 读取 fileSize (long, 8 bytes)
        long oldFileSize = segment.get(ValueLayout.JAVA_LONG, pos);
        pos += 8;
        // 读取 blockSize (int, 4 bytes)
        int oldblockSize = segment.get(ValueLayout.JAVA_INT, pos);
        pos += 4;
        // 读取 CRC (int, 4 bytes)
        int crc = segment.get(ValueLayout.JAVA_INT, pos);
        pos += 4;
        // 读取 dataLen (int, 4 bytes)
        int dataLen = segment.get(ValueLayout.JAVA_INT, pos);
        pos += 4;
        // 读取 data (byte[])
        byte[] data = new byte[dataLen];
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, pos, data, 0, dataLen);
        //校验数据
        CRC32 crc32 = new CRC32();
        crc32.update(dataLen);
        crc32.update(data);
        if ((int) crc32.getValue() != crc) {
            //如果不一样说明数据不正确
            throw new CloudCacheException("An error occurred in the bucketMeta data. " + path);
        }
        String oldPrefix = new String(data, StandardCharsets.UTF_8);
        /*
         * 2.扫描获取wal目录下的所有文件的绝对路径
         */
        Path walPath = path.resolve("wal");
        if (!Files.exists(walPath) || !Files.isDirectory(walPath)) {
            throw new CloudCacheException("wal directory not exists: " + walPath);
        }
        //获取所有的文件绝对地址
        try (Stream<Path> stream = Files.list(walPath)) {
            //根据文件名字进行排序
            List<Path> walFiles = stream.filter(Files::isRegularFile)
                    .sorted(Comparator.comparingLong(this::parseWalFileOffset))
                    .toList();
            ByteBuffer temp = ByteBuffer.allocate(4 * 1024);
            //遍历所有的文件并且读取前4KB数据并创建文件，然后进行数据恢复
            for (Path curPath : walFiles) {
                long fileFromOffset = Long.parseLong(curPath.getFileName().toString());
                try (FileChannel fileChannel = FileChannel.open(curPath, StandardOpenOption.READ)) {
                    fileChannel.read(temp);
                    //获取该文件的读和上传指针
                    long readPosition = temp.getLong();
                    long upLoadPosition = temp.getLong();
                    //然后创建文件，并且修改文件的指针状态
                    DefaultMappedFile defaultMappedFile = fileManager.synCreateMappedFile(fileFromOffset);
                    defaultMappedFile.wrotePosition = readPosition;
                    defaultMappedFile.upLoadPosition = upLoadPosition;
                } catch (IOException e) {
                    log.warn("recovering=>{} file created failed", fileFromOffset, e);
                }
            }
        }


    }

    private long parseWalFileOffset(Path path) {
        // path 是 /data/bucket-1/wal/1000
        // getFileName() 获取 1000
        // toString() 转为 "1000"
        // Long.parseLong() 解析为 1000L
        String fileName = path.getFileName().toString();
        return Long.parseLong(fileName);
    }


    //todo 关闭资源
    public void close() {
        walInstanceBucketManager.close();
        coreInstanceBucketManager.close();

    }


}
