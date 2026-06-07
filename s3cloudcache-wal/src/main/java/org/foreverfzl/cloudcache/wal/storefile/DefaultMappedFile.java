package org.foreverfzl.cloudcache.wal.storefile;

import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.config.S3CloudCacheConfig;
import org.foreverfzl.cloudchache.common.disk.WalDataStruct;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * 代表一个 操作系统文件，用于持久化数据
 */
public class DefaultMappedFile extends AbstractMappedFile {

    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> COMMITTED_POSITION_UPDATER;
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> FLUSHED_POSITION_UPDATER;
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> UPLOAD_POSITION;

    protected long fileFromOffset; //文件的起始位点，也就是文件的地址
    protected volatile int wrotePosition; //数据写入位置
    protected volatile int committedPosition; //写入到操作系统文件缓冲区的位置
    protected volatile int flushedPosition; //文件刷盘位置
    protected volatile int upLoadPosition; //该文件上传到云服务器的位置


    protected File file;
    protected RandomAccessFile randomAccessFile;
    protected String fileName;
    protected long fileSize;
    protected FileChannel fileChannel;
    protected Arena arena;
    protected MemorySegment mappedMemorySegment; //本质是MMP内存映射

    static {
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "committedPosition");
        FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "flushedPosition");
        UPLOAD_POSITION = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "upLoadPosition");
    }

    public DefaultMappedFile() {
    }

    public DefaultMappedFile(String fileName, long fileSize) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        arena = Arena.ofShared(); //创建MS的控制对象
        init(fileName, fileSize);
    }

    /**
     * 创建并初始化WAL文件，并且将channel、指针等初始化
     *
     * @param fileName
     * @param fileSize
     */
    @Override
    public void init(String fileName, long fileSize) {
        if (fileName == null || fileName.isBlank()) {
            throw new IllegalArgumentException("fileName cannot be null");
        }

        if (fileSize <= 0) {
            throw new IllegalArgumentException("fileSize must be greater than 0");
        }
        try {
            // 创建目录
            File dir = new File(ProjectUtil.DISK_PERSISTENT_ADDRESS);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new RuntimeException("Failed to create directory: " + dir);
            }
            // 创建文件对象
            this.file = new File(dir, fileName);

            // 创建文件并设置大小
            randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.setLength(fileSize);
            fileChannel = randomAccessFile.getChannel();
            mappedMemorySegment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize, arena);

            //进行文件预热，每16384页刷盘一次，防止脏页过多
            warm(16384);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize cache file: " + fileName,
                    e
            );
        }
    }

    /**
     * 预热 PageCache 并且根据用户配置判断是否锁定映射内存，锁定可以确保其不会被换出到虚拟内存中，也不会被操作系统移动。
     *
     * @param pages 预热多少页后就执行一次强制刷盘
     */
    @Override
    public void warm(int pages) {
        if (mappedMemorySegment == null) {
            throw new IllegalStateException("mappedMemorySegment has not been mapped yet.");
        }
        if (pages <= 0) {
            throw new IllegalArgumentException("pages must be greater than 0");
        }

        long size = mappedMemorySegment.byteSize();
        // 动态获取操作系统页大小，若获取不到则默认使用 4096 字节
        int pageSize = ProjectUtil.OS_PAGE_SIZE;
        long pageCount = (size + pageSize - 1) / pageSize;

        // 1. 预热 PageCache
        for (long i = 0; i < pageCount; i++) {
            long offset = i * pageSize;
            if (offset < size) {
                //写入一个0进行预热，触发页中断
                mappedMemorySegment.set(ValueLayout.JAVA_BYTE, offset, (byte) 0);
            }
            // 每预热指定页数或者到达最后一页时，执行刷盘，防止脏页太多造成操作系统卡顿
            if ((i + 1) % pages == 0 || (i + 1) == pageCount) {
                mappedMemorySegment.force();
            }
        }
        //根据用户选择是否锁定内存
        if (S3CloudCacheConfig.getIsLockMappedFilePageCache()) {
            ProjectUtil.lockMemory(this.mappedMemorySegment);
        }
    }

    @Override
    public String getFileName() {
        return this.fileName;
    }

    @Override
    public FileChannel getFileChannel() {
        return this.fileChannel;
    }

    @Override
    public boolean getData(long offset, int size, ByteBuffer byteBuffer) {
        return false;
    }

    @Override
    public boolean appendData(long offset, int size, WalDataStruct walDataStruct) {
        return false;
    }


    //todo 删除对应文件并释放资源的方法
    @Override
    public boolean clean() {
        return false;
    }


}


