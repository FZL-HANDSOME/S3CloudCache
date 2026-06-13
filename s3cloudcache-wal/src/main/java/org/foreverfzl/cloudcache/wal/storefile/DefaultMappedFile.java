package org.foreverfzl.cloudcache.wal.storefile;

import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.ProjectUtil;
import org.foreverfzl.cloudchache.common.exception.WalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;


/**
 * 代表一个 操作系统文件，用于持久化数据
 */
public class DefaultMappedFile extends AbstractMappedFile {

    protected static final Logger log = LoggerFactory.getLogger(LogName.WAL_STORE_FILE);

    public static final AtomicLongFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;
    public static final AtomicLongFieldUpdater<DefaultMappedFile> READ_POSITION_UPDATER;
    public static final AtomicLongFieldUpdater<DefaultMappedFile> UPLOAD_POSITION_UPDATER;

    public volatile long fileFromOffset;
    public volatile long wrotePosition; //数据写入位置
    public volatile long readPosition; //可读位置，0~readPosition位置可读,此位置一定是写入到了文件中
    public volatile long upLoadPosition; //该文件上传到云服务器的位置


    protected File file;
    protected String filePath;
    protected String fileName;
    protected long fileSize;
    protected FileChannel fileChannel;
    protected Arena arena;
    protected MemorySegment mappedMemorySegment; //本质是MMP内存映射

    //Block、upLoadPosition更新相关
    protected int totalBlockCount; //该文件逻辑上对应多少个Block
    protected int blockSize;
    protected long[] blockEndOffsetInMappedFile; //upLoadPosition指针更新辅助数组指针更新辅助数组
    protected AtomicInteger nextUploadBlockIndex = new AtomicInteger(0); //upLoadPosition指针期望下次更新index


    static {
        WROTE_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        READ_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(DefaultMappedFile.class, "readPosition");
        UPLOAD_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(DefaultMappedFile.class, "upLoadPosition");
    }

    public DefaultMappedFile() {

    }

    public DefaultMappedFile(final String filePath, final String fileName, final long fileFromOffset, final long fileSize, final int blockSize) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.fileFromOffset = fileFromOffset;
        this.filePath = filePath;
        this.blockSize = blockSize;
        this.totalBlockCount = (int) Math.ceil((double) fileSize / blockSize);
        blockEndOffsetInMappedFile = new long[totalBlockCount];
        arena = Arena.ofShared(); //创建MS的控制对象
        init();
    }

    /**
     * 创建并初始化WAL文件，并且将channel、指针等初始化
     *
     */
    @Override
    public void init() {
        if (fileName == null || fileName.isBlank()) {
            throw new WalException("fileName cannot be null");
        }
        if (filePath == null || filePath.isBlank()) {
            throw new WalException("fileName cannot be null");
        }
        if (fileSize <= 0) {
            throw new WalException("fileSize must be greater than 0");
        }
        try {
            // 创建目录
            File dir = new File(filePath);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new WalException("Failed to create directory: " + dir);
            }
            // 创建文件对象
            this.file = new File(dir, fileName);

            // 创建文件并设置大小
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.setLength(fileSize);
            fileChannel = randomAccessFile.getChannel();
            mappedMemorySegment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize, arena);

            //进行文件预热，每16384页刷盘一次，防止脏页过多
            warm(16384);
        } catch (Exception e) {
            throw new WalException(
                    "Failed to initialize cache file: " + fileName, e
            );
        }
    }


    // 物理和逻辑Block解耦 + 无锁账本 推进 upLoadPosition指针
    public void ackUpLoadPosition(int logicalIndex, long endOffsetInFile) {

    }

    /**
     * 预热 PageCache 并且根据用户配置判断是否锁定映射内存，锁定可以确保其不会被换出到虚拟内存中，也不会被操作系统移动。
     *
     * @param pages 预热多少页后就执行一次强制刷盘
     */
    @Override
    public void warm(int pages) {
        if (mappedMemorySegment == null) {
            throw new WalException("mappedMemorySegment has not been mapped yet.");
        }
        if (pages <= 0) {
            throw new WalException("pages must be greater than 0");
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
//        //根据用户选择是否锁定内存
//        if (S3CloudCacheConfig.getIsLockMappedFilePageCache()) {
//            ProjectUtil.lockMemory(this.mappedMemorySegment);
//        }
    }


    /**
     * 从该文件中读取指定区域的数据，并反序列化为 WalDataStruct 对象。
     * <p>
     * 读取方式与 doAppend 的写入方式对称：
     * 1. 从 mappedMemorySegment 中按字段逐个读取 Header（Magic、KeyLen、ValueLen、Checksum）
     * 2. 批量拷贝 KeyBytes 和 ValueBytes
     * 3. 校验 Magic 和 CRC32，确保数据完整性
     * </p>
     *
     * @param readOffset 起始位置（相对于文件起始的偏移量）
     * @param size       要读取的字节数（应 >= WalDataStruct.HEADER_LENGTH + keyLen + valueLen）
     * @return 反序列化后的 WalDataStruct 对象，读取失败时返回 null
     */
    @Override
    public WalDataStruct getData(long readOffset, long size) {
        // 1. 参数校验
        if (readOffset < 0 || size <= 0) {
            log.warn("getData: invalid parameters, readOffset={}, size={}", readOffset, size);
            return null;
        }

        // 至少需要能容纳协议头部
        if (size < WalDataStruct.HEADER_LENGTH) {
            log.warn("getData: size={} is less than HEADER_LENGTH={}, fileName={}",
                    size, WalDataStruct.HEADER_LENGTH, fileName);
            return null;
        }

        // 边界检查：确保读取范围不超过已写入区域
        long currentWrotePosition = WROTE_POSITION_UPDATER.get(this);
        if (readOffset + size > currentWrotePosition) {
            log.warn("getData: read range [{}, {}) exceeds wrotePosition {}, fileName={}",
                    readOffset, readOffset + size, currentWrotePosition, fileName);
            return null;
        }

        try {
            // 2. 切片出目标区域，与 doAppend 对称，slice 内部偏移从 0 开始
            MemorySegment slice = mappedMemorySegment.asSlice(readOffset, size);
            long pos = 0;
            // 读取 Header 字段
            long magic = slice.get(ValueLayout.JAVA_LONG, pos);
            pos += 8;

            int keyLen = slice.get(ValueLayout.JAVA_INT, pos);
            pos += 4;

            int valueLen = slice.get(ValueLayout.JAVA_INT, pos);
            pos += 4;

            long checksum = slice.get(ValueLayout.JAVA_LONG, pos);
            pos += 8;

            // 3. 校验魔数
            if (magic != WalDataStruct.MAGIC_NUMBER) {
                log.warn("getData: invalid magic number 0x{} at readOffset={}, fileName={}",
                        Long.toHexString(magic), readOffset, fileName);
                return null;
            }

            // 4. 校验数据长度是否合法
            if (keyLen < 0 || valueLen < 0) {
                log.warn("getData: invalid keyLen={} or valueLen={} at readOffset={}, fileName={}",
                        keyLen, valueLen, readOffset, fileName);
                return null;
            }
            if (WalDataStruct.HEADER_LENGTH + keyLen + valueLen > size) {
                log.warn("getData: data length (header={} + keyLen={} + valueLen={}) exceeds read size={}, fileName={}",
                        WalDataStruct.HEADER_LENGTH, keyLen, valueLen, size, fileName);
                return null;
            }

            // 5. 批量拷贝 Key 字节数组
            byte[] keyBytes = new byte[keyLen];
            MemorySegment.copy(
                    slice,
                    ValueLayout.JAVA_BYTE,
                    pos,
                    keyBytes,
                    0,
                    keyLen
            );
            pos += keyLen;

            // 6. 批量拷贝 Value 字节数组
            byte[] valueBytes = new byte[valueLen];
            MemorySegment.copy(
                    slice,
                    ValueLayout.JAVA_BYTE,
                    pos,
                    valueBytes,
                    0,
                    valueLen
            );

            // 7. 构造 WalDataStruct 并返回（构造方法内含 CRC32 校验逻辑）
            return new WalDataStruct(magic, keyLen, valueLen, checksum, keyBytes, valueBytes);
        } catch (Exception e) {
            log.error("getData: failed to read data from file={}, readOffset={}, size={}",
                    fileName, readOffset, size, e);
            return null;
        }
    }

    /**
     * 获取一条完整数据
     *
     * @param readOffset 起始位置
     * @return
     */
    @Override
    public WalDataStruct getData(long readOffset) {
        return null;
    }

    /**
     * 往该文件中追加数据（并发安全）。
     * <p>
     * 流程：
     * 1. 校验数据合法性
     * 2. 通过 CAS 自旋原子性地抢占 wrotePosition 指针，为当前线程分配一段独占的写入区域
     * 3. 抢占成功后调用 doAppend 执行真正的写入操作
     * </p>
     *
     * @param walDataStruct 磁盘持久化协议格式
     * @return true 表示写入成功，false 表示写入失败
     */
    @Override
    public AppendMessageResult appendData(final WalDataStruct walDataStruct) {
        if(!isAvailable())return new AppendMessageResult(AppendMessageResult.AppendStatus.FILE_CLOSED,this.fileName);
        // 1. 参数校验
        if (walDataStruct == null) {
            log.warn("appendData: walDataStruct cannot be null, fileName={}", fileName);
            return AppendMessageResult.fail(AppendMessageResult.AppendStatus.UNKNOWN_ERROR, this.fileName);
        }
        if (!walDataStruct.validateMagic()) {
            log.warn("appendData: invalid magic number in walDataStruct, fileName={}", fileName);
            return AppendMessageResult.fail(AppendMessageResult.AppendStatus.UNKNOWN_ERROR, this.fileName);
        }
        long msgSize = walDataStruct.getSerializedSize();
        // 2. CAS 自旋抢占 wrotePosition，为当前线程分配写入区域
        long currentPos;
        long newPos;
        do {
            currentPos = WROTE_POSITION_UPDATER.get(this);
            newPos = currentPos + msgSize;
            if (newPos > this.fileSize) {
                // 边界检查：剩余空间不足以容纳本条消息
                //todo 将本文件设置为不可写入,然后返回END_OF_FILE，之后尝试去新的文件中写
                close();
                log.warn("appendData: not enough space, currentPos={}, msgSize={}, fileSize={}, fileName={}",
                        currentPos, msgSize, this.fileSize, fileName);
                return AppendMessageResult.fail(AppendMessageResult.AppendStatus.END_OF_FILE, this.fileName);
            }
        } while (!WROTE_POSITION_UPDATER.compareAndSet(this, currentPos, newPos));
        //采用空间预留 解耦物理和逻辑Block
        //先分配逻辑Block，然后将逻辑Block信息放入到AppendMessageResult中，最后调用core模块
        //todo

        // 3. CAS 成功，当前线程独占 [currentPos, newPos) 区间，执行真正写入
        AppendMessageResult result = doAppend(currentPos, msgSize, walDataStruct);

        if (!result.isOk()) {
            // 写入失败，这一部分文件内容会用无效的字节填充
            log.error("appendData: doAppend failed, result={}, attempting to rollback wrotePosition", result);
            return result;
        }
        return result;
    }

    /**
     * 真正将 WalDataStruct 数据写入 mappedMemorySegment（PageCache）的方法。
     * <p>
     * 此方法在 appendData 中通过 CAS 抢到独占写入区间后被调用，
     * 因此 [writeOffset, writeOffset + size) 区间由调用线程独占，无并发问题。
     * </p>
     *
     * @param writeOffset   写入的起始偏移量（由 CAS 抢到的 wrotePosition）
     * @param size          要写入的字节数（应等于 walDataStruct.getSerializedSize()）
     * @param walDataStruct 磁盘持久化协议格式数据
     * @return AppendMessageResult 写入结果
     */
    private AppendMessageResult doAppend(final long writeOffset, final long size, final WalDataStruct walDataStruct) {
        try {
            //创建一个新的 MemorySegment 视图，共享同一块底层内存，仅调整起始地址和长度，不复制数据。
            MemorySegment targetSlice = mappedMemorySegment.asSlice(writeOffset, size);
            long pos = 0;
            // 1. Magic
            targetSlice.set(
                    ValueLayout.JAVA_LONG,
                    pos,
                    walDataStruct.getMagic()
            );
            pos += 8;

            // 2. Key Length
            targetSlice.set(
                    ValueLayout.JAVA_INT,
                    pos,
                    walDataStruct.getKeyLen()
            );
            pos += 4;

            // 3. Value Length
            targetSlice.set(
                    ValueLayout.JAVA_INT,
                    pos,
                    walDataStruct.getValueLen()
            );
            pos += 4;

            // 4. CRC32
            targetSlice.set(
                    ValueLayout.JAVA_LONG,
                    pos,
                    walDataStruct.getChecksum()
            );
            pos += 8;

            // 5. Key Bytes
            byte[] keyBytes = walDataStruct.getKeyBytes();
            MemorySegment.copy(
                    keyBytes,
                    0,
                    targetSlice,
                    ValueLayout.JAVA_BYTE,
                    pos,
                    keyBytes.length
            );

            pos += keyBytes.length;

            // 6. Value Bytes
            byte[] valueBytes = walDataStruct.getValueBytes();
            MemorySegment.copy(
                    valueBytes,
                    0,
                    targetSlice,
                    ValueLayout.JAVA_BYTE,
                    pos,
                    valueBytes.length
            );
            return new AppendMessageResult(
                    AppendMessageResult.AppendStatus.PUT_OK,
                    writeOffset,
                    size,
                    System.currentTimeMillis(),
                    this.fileName
            );
        } catch (Exception e) {
            log.error("doAppend: failed to write data to mappedMemorySegment, writeOffset={}, size={}, fileName={}",
                    writeOffset, size, fileName, e);
            return AppendMessageResult.fail(AppendMessageResult.AppendStatus.UNKNOWN_ERROR, this.fileName);
        }
    }

    /**
     * 删除对应文件并释放资源的方法
     */
    @Override
    public void clean() {
        try {
            //释放mmp
            arena.close();
            fileChannel.close();
            file.delete();
        } catch (Exception e) {
            throw new WalException(
                    "Failed to Clean Filed: " + filePath + File.separator + fileName, e
            );
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

    public MemorySegment getMappedMemorySegment() {
        return mappedMemorySegment;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }
}


