package org.foreverfzl.cloudcache.storage.instance.cloudcache;


import org.foreverfzl.cloudcache.core.cache.CloudCacheBlock;
import org.foreverfzl.cloudcache.core.datastruct.HeapBlockDataStruct;
import org.foreverfzl.cloudcache.core.global.CoreInstanceBucketManager;
import org.foreverfzl.cloudcache.core.manager.CacheBlockManager;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.storage.instance.bucket.BucketWriterWriter;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.global.WalInstanceBucketManager;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.foreverfzl.cloudchache.common.exception.CloudCacheException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
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

    //数据恢复线程池
    private ExecutorService recoverExecutorService = null;


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

    /**
     * 使用原始 S3Client 将指定内存段上传为一个对象。
     *
     * @param bucketName 目标 Bucket 名称
     * @param s3Key      目标对象 Key
     * @param data       待上传数据，可为堆内或堆外 MemorySegment
     */
    public PutObjectResponse s3RawPutObject(String bucketName, String s3Key, MemorySegment data) {
        if (bucketName == null || bucketName.isBlank()) {
            throw new CloudCacheException("bucketName can not be null or blank");
        }
        if (s3Key == null || s3Key.isBlank()) {
            throw new CloudCacheException("s3Key can not be null or blank");
        }
        if (data == null) {
            throw new CloudCacheException("data can not be null");
        }

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
        PutObjectResponse response = s3Client.putObject(request, RequestBody.fromByteBuffer(data.asByteBuffer()));
        return response;
    }

    //start负责扫描目录、恢复数据等
    public void start() {
        //1：检查指定目录下instanceName的所有目录和文件,方法结束后所有的东西都恢复到结束前的状态，并且recoverBucketsMap填写好
        try (Stream<Path> list = Files.list(Paths.get(config.walPath + File.separator + instanceName))) {
            List<Path> bucketPathList = list.toList();
            int bucketCount = bucketPathList.size();
            recoverExecutorService = Executors.newFixedThreadPool(bucketCount);
            for (Path path : bucketPathList) {
                String bucketName = path.getFileName().toString();
                this.chackDirectoryAndFile(path, bucketName);
            }

        } catch (Exception e) {

        }
    }

    //恢复一个具体bucket中的数据
    private void chackDirectoryAndFile(Path path, String bucketName) throws IOException {
        if (path == null || !Files.exists(path)) {
            throw new CloudCacheException("bucket directory not exists: " + path);
        }
        if (!Files.isDirectory(path)) {
            throw new CloudCacheException("path is not directory: " + path);
        }

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
        crc32.update(Math.toIntExact(oldFileSize));
        crc32.update(oldblockSize);
        crc32.update(dataLen);
        crc32.update(data);
        String oldPrefix = new String(data, StandardCharsets.UTF_8);
        boolean out = false;
        if ((int) crc32.getValue() != crc) {
            //bucketMeta文件损坏，以前的文件无法恢复
            log.warn("An error occurred in the bucketMeta data.old file can not recover. file path is{}", path);
            out = true;
        }

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
                    .sorted(Comparator.comparingLong(this::getFileFromOffset))
                    .toList();
            long endFileFromOffset = getFileFromOffset(walFiles.getLast()) + oldFileSize;
            //创建该bucket对应manager
            MappedFileManager fileManager = walInstanceBucketManager.getOrCreateBucketFileManager(bucketName, endFileFromOffset);
            CacheBlockManager blockManager = coreInstanceBucketManager.getOrCreateBlockManager(bucketName, fileManager.blockMetaDataManager);
            if (out) {
                //bucket元数据错误，不恢复以前的数据
                return;
            }
            //这里创建新的CacheBlockManager专门进行数据恢复，因为新的配置和老的配置可能不一样，因此用旧的配置创建一个CacheBlockManager
            String recoverBucketName = bucketName + "_" + "recover";
            long cacheSize = 128 * 1024 * 1024L;
            int blockUpLoadCount = (int) ProjectUtil.divideByPower(cacheSize, oldblockSize);
            CacheBlockManager recoverBlockManager = coreInstanceBucketManager.getOrCreateBlockManager(recoverBucketName, fileManager.blockMetaDataManager,
                    null, cacheSize, oldblockSize, blockUpLoadCount, false);
            recoverExecutorService.execute(() -> {
                recoverFile(walFiles, fileManager, recoverBlockManager, oldFileSize, oldblockSize, oldPrefix);
                //等待恢复完成删除recoverBlockManager
                coreInstanceBucketManager.removeBlockManager(recoverBucketName);
            });
        } catch (Exception e) {

        }
    }

    //恢复一个bucket中的文件
    private void recoverFile(List<Path> walFiles, MappedFileManager fileManager, CacheBlockManager recoverBlockManager,
                             long oldFileSize, int oldblockSize, String prefix) {
        CRC32 crc32 = new CRC32();
        //遍历所有的文件并且读取前4KB数据并创建文件，然后进行数据恢复
        long curPos = 0;
        long endPos = 0;
        for (Path curPath : walFiles) {
            String fileName = curPath.getFileName().toString();
            long fileFromOffset = Long.parseLong(fileName);
            File file = curPath.toFile();
            try {
                //创建该文件的Java对象
                DefaultMappedFile defaultMappedFile = new DefaultMappedFile(curPath.getParent().toString(), fileName, fileFromOffset, oldFileSize,
                        file, oldblockSize, false, false, false, fileManager);
                //先读取4KB的元数据区域，获取指针
                long readPosition = defaultMappedFile.getLong(curPos);
                curPos += 8;
                long upLoadPosition = defaultMappedFile.getLong(curPos);
                defaultMappedFile.wrotePosition = readPosition;
                defaultMappedFile.readPosition = readPosition;
                defaultMappedFile.upLoadPosition = upLoadPosition;
                //将该文件添加到manage中
                fileManager.addMappedFile(defaultMappedFile);
                //不需要恢复数据，将文件关闭，让后台线程自己检测删除
                if (readPosition == upLoadPosition) {
                    defaultMappedFile.close();
                    continue;
                }
                //需要数据恢复的文件，读取文件指定区域数据进行恢复
                curPos = upLoadPosition;
                int blockCount = (int) ProjectUtil.divideByPower((readPosition - upLoadPosition), oldblockSize);
                //获取元数据管路者
                BlockMetaDataManager blockMetaDataManager = fileManager.blockMetaDataManager;
                for (int i = 1; i <= blockCount; i++) {
                    endPos = curPos + oldblockSize;
                    int blockIndex = Math.toIntExact(ProjectUtil.divideByPower(curPos, oldblockSize));
                    //如果可以读int并且是正常数据则读取
                    //如果一个Block结束了会在结尾打上end标志，end标志占用4字节，如果结尾位置4字节都不够默认就是结束了
                    while (endPos - curPos >= 4 && defaultMappedFile.getInt(curPos) == DataStruct.MAGIC_NUMBER) {
                        curPos += 4;
                        int checksum = defaultMappedFile.getInt(curPos);
                        curPos += 4;
                        int valueLen = defaultMappedFile.getInt(curPos);
                        curPos += 4;
                        byte[] orgData = defaultMappedFile.getOrgData(curPos, valueLen);
                        crc32.update(orgData);
                        if ((int) crc32.getValue() != checksum) {
                            //数据错误直接退出
                            break;
                        }
                        blockMetaDataManager.addExpectedBytes(fileFromOffset, blockIndex, valueLen);
                        blockMetaDataManager.addPageCacheBytes(fileFromOffset, blockIndex, valueLen);
                        recoverBlockManager.appendData(new HeapBlockDataStruct(defaultMappedFile, fileFromOffset, blockIndex, orgData, 0, valueLen), prefix);
                        crc32.reset();
                    }
                    //尝试封口
                    blockMetaDataManager.trySeal(fileFromOffset, blockIndex);
                    //该block数据结束了，将该block上传
                    CloudCacheBlock block = recoverBlockManager.getExistingBlock(fileFromOffset, blockIndex);
                    recoverBlockManager.updateBlock(block);
                    curPos = upLoadPosition + ((long) i * oldblockSize);
                }
                //将该文件设置为清除
                defaultMappedFile.upLoadPosition = readPosition;
                defaultMappedFile.close();
            } catch (Exception e) {
                log.warn("recovering=>{} file created failed", fileFromOffset, e);
            }
        }
    }


    private long getFileFromOffset(Path path) {
        return Long.parseLong(path.getFileName().toString());
    }

    //todo 关闭资源
    public void close() {
        walInstanceBucketManager.close();
        coreInstanceBucketManager.close();

    }


}
