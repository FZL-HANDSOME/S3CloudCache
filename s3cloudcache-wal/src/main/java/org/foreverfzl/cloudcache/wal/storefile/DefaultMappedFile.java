package org.foreverfzl.cloudcache.wal.storefile;

import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.PaddingStruct;
import org.foreverfzl.cloudcache.wal.datastruct.WalDataStruct;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.lang.foreign.ValueLayout.JAVA_INT;


/**
 * 代表一个 操作系统文件，用于持久化数据
 */
public class DefaultMappedFile extends AbstractMappedFile {

    protected static final Logger log = LoggerFactory.getLogger(LogName.WAL_STORE_FILE);

    public static final AtomicLongFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;
    public static final AtomicLongFieldUpdater<DefaultMappedFile> READ_POSITION_UPDATER;
    public static final AtomicLongFieldUpdater<DefaultMappedFile> UPLOAD_POSITION_UPDATER;
    public static final AtomicIntegerFieldUpdater<DefaultMappedFile> IS_CREATE_NEW_FILE;

    public volatile long fileFromOffset;
    public volatile long wrotePosition; //数据写入位置
    public volatile long readPosition; //可读位置，0~readPosition位置可读,此位置一定是写入到了文件中
    public volatile long upLoadPosition; //该文件上传到云服务器的位置
    private final MappedFileManager manager;

    //该属性就是看看超过水位线后是否已经开启创建新文件了，0代表未创建，1代表创建
    private volatile int isCreateNewFile = 0;

    protected File file; //文件的引用
    protected String dirPath;
    protected String fileName;
    public long fileSize;
    protected FileChannel fileChannel;
    protected Arena arena;
    protected MemorySegment mappedMemorySegment; //本质是MMP内存映射

    //每个文件开头元数据区域大小
    public static final long FILE_META_SIZE = 4 * 1024;

    //Block、upLoadPosition更新相关
    protected int totalBlockCount; //该文件逻辑上对应多少个Block
    protected int blockSize;
    protected long[] blockEndOffsetInMappedFile; //upLoadPosition指针更新辅助数组指针更新辅助数组
    protected AtomicInteger nextUploadBlockIndex = new AtomicInteger(0); //upLoadPosition指针期望下次更新index
    //引入数组元素的 VarHandle，用于消灭原生数组的内存可见性缺陷
    private static final VarHandle ARRAY_ELEMENT_HANDLE;
    //如果该文件的指针更新了该属性会被设置为true，然后MappedFileManager有专门的线程去更新该文件的元数据，更新完成后设置为false;
    public volatile boolean metaDirty;


    static {
        WROTE_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        READ_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(DefaultMappedFile.class, "readPosition");
        UPLOAD_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(DefaultMappedFile.class, "upLoadPosition");
        IS_CREATE_NEW_FILE = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "isCreateNewFile");
        // 初始化原生 long[] 数组的元素句柄
        ARRAY_ELEMENT_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);
    }


    public DefaultMappedFile(final String dirPath, final String fileName, final long fileFromOffset,
                             final long fileSize, File file, final int blockSize, boolean isWarm, boolean isLockMemory, MappedFileManager manager) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.fileFromOffset = fileFromOffset;
        this.dirPath = dirPath;
        this.blockSize = blockSize;
        this.file = file;
        this.manager = manager;
        this.metaDirty = false;
        this.totalBlockCount = (int) Math.ceil((double) fileSize / blockSize);
        blockEndOffsetInMappedFile = new long[totalBlockCount];
        arena = Arena.ofShared(); //创建MS的控制对象
        init(isWarm, isLockMemory);
    }

    /**
     * 创建文件方法
     */
    public static DefaultMappedFile createFile(final String dirPath, final String fileName, final long fileFromOffset,
                                               final long fileSize, final int blockSize, boolean isWarm,
                                               boolean isLockMemory, MappedFileManager manager) {
        if (fileName == null || fileName.isBlank()) {
            throw new WalException("fileName cannot be null");
        }
        if (dirPath == null || dirPath.isBlank()) {
            throw new WalException("fileName cannot be null");
        }
        if (fileSize <= 0) {
            throw new WalException("fileSize must be greater than 0");
        }
        // 创建目录
        File dir = new File(dirPath);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new WalException("Failed to create directory: " + dir);
        }
        // 创建文件对象
        File newfile = new File(dir, fileName);
        if (newfile.exists()) {
            //如果文件存在的话就不创建了
            return null;
        }
        //文件不存在则创建
        return new DefaultMappedFile(dirPath, fileName, fileFromOffset, fileSize, newfile, blockSize, isWarm, isLockMemory, manager);
    }


    /**
     * 文件的内存分配以及预热、锁定等
     */
    @Override
    public void init(boolean isWarm, boolean isLockMemory) {
        try {
            // 创建文件并设置大小
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            randomAccessFile.setLength(fileSize);
            fileChannel = randomAccessFile.getChannel();
            mappedMemorySegment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize, arena);
            if (isWarm) {
                //进行文件预热，每16384页刷盘一次，防止脏页过多
                warm(16384, isLockMemory);
            }
        } catch (Exception e) {
            throw new WalException(
                    "Failed to initialize cache file: " + fileName, e
            );
        }
    }

    /**
     * 物理和逻辑Block解耦 + 无锁账本 推进 upLoadPosition指针，该方法只是将预期结果填到坑里面，有专门的线程去检查指针
     *
     * @param logicalIndex 当前完成上传的 Block 在本文件内部的逻辑序号 (0, 1, 2...)
     */
    public void ackUpLoadPosition(int logicalIndex) {
        // 边界防御：防止脏数据引发数组越界
        if (logicalIndex < 0 || logicalIndex >= totalBlockCount) {
            log.error("Invalid logical index: {}, totalBlockCount: {}", logicalIndex, totalBlockCount);
            return;
        }
        // 1. 物理填坑：利用 VarHandle 的 Volatile 语义写入，确保其他 CPU 核心立即可见
        ARRAY_ELEMENT_HANDLE.setVolatile(this.blockEndOffsetInMappedFile, logicalIndex, (long) (logicalIndex + 1) * blockSize);
    }

    /**
     * 预热 PageCache 并且根据用户配置判断是否锁定映射内存，锁定可以确保其不会被换出到虚拟内存中，也不会被操作系统移动。
     *
     * @param pages 预热多少页后就执行一次强制刷盘
     */
    @Override
    public void warm(int pages, boolean isLockMemory) {
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
        //todo 还不知道是否真正能锁定内存
        //根据用户选择是否锁定内存
        if (isLockMemory) {
            ProjectUtil.lockMappedPages(this.mappedMemorySegment);
        }
    }


    /**
     * 从该文件中读取指定区域的数据，并反序列化为 WalDataStruct 对象。
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

            int magic = slice.get(JAVA_INT, pos);
            pos += 4;

            int checksum = slice.get(JAVA_INT, pos);
            pos += 4;

            int valueLen = slice.get(JAVA_INT, pos);
            pos += 4;

            // 3. 校验魔数
            if (magic != WalDataStruct.MAGIC_NUMBER) {
                log.warn("getData: invalid magic number 0x{} at readOffset={}, fileName={}",
                        Long.toHexString(magic), readOffset, fileName);
                return null;
            }

            // 批量拷贝 Value 字节数组
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
            return new WalDataStruct(magic, checksum, 0, valueLen, valueBytes);
        } catch (Exception e) {
            log.error("getData: failed to read data from file={}, readOffset={}, size={}",
                    fileName, readOffset, size, e);
            return null;
        }
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
     * @param dataStruct 磁盘持久化协议格式
     * @return true 表示写入成功，false 表示写入失败
     */
    @Override
    public AppendMessageResult appendData(final DataStruct dataStruct) {
        if (!isAvailable())
            return new AppendMessageResult(AppendMessageResult.AppendStatus.FILE_CLOSED, this.fileFromOffset);
        long msgSize = dataStruct.getSerializedSize();
        // 2. CAS 自旋抢占 wrotePosition，为当前线程分配写入区域
        long currentPos;
        long newPos;
        //采用空间预留 解耦物理和逻辑Block，先分配逻辑Block，然后将逻辑Block信息放入到AppendMessageResult中
        //logicalIndex算出该数据在哪个逻辑Block中，divideByPower(newPos,blockSize)等价于 newPos/blockSize
        int logicalIndex;
        while (true) {
            currentPos = WROTE_POSITION_UPDATER.get(this);
            newPos = currentPos + msgSize;
            logicalIndex = Math.toIntExact(ProjectUtil.divideByPower(currentPos, blockSize));
            // 检查该数据是否跨逻辑Block了。currentPos & (this.blockSize - 1)等价于 currentPos%blockSize
            long blockOffset = currentPos & (this.blockSize - 1);
            long remainingInBlock = this.blockSize - blockOffset;
            long paddingPos;
            if (msgSize > remainingInBlock) {
                // 发现空间不够写整条消息，强行将写指针推到当前 Block 的绝对终点（即下一个 Block 的起点）
                paddingPos = currentPos + remainingInBlock;
                //看看文件是否结尾
                if (paddingPos == this.fileSize) {
                    manager.blockMetaDataManager.trySeal(this.fileFromOffset, logicalIndex); //将该block设置为封口
                    close(); //关闭文件
                    return AppendMessageResult.fail(AppendMessageResult.AppendStatus.END_OF_FILE, this.fileFromOffset);
                }
                // 尝试 CAS 抢占这段残渣空间用来做 Padding
                if (WROTE_POSITION_UPDATER.compareAndSet(this, currentPos, paddingPos)) {
                    manager.blockMetaDataManager.trySeal(this.fileFromOffset, logicalIndex); //将该block设置为封口
                    // 占位成功，当前线程负责将 [currentPos, paddingPos) 区间执行 Padding 填充
                    doPadding(currentPos, (int) remainingInBlock);
                    // 核心：当前线程的真实业务数据并未写成功，必须继续循环去抢占下一个全新 Block 的空间
                    continue;
                }
                Thread.onSpinWait();
                continue;
            }
            //检查该block是否已经封口
            if (manager.blockMetaDataManager.isSealed(this.fileFromOffset, logicalIndex)) {
                //封口了则尝试将指针设置为下一个Block起点
                paddingPos = currentPos + remainingInBlock;
                // 尝试 CAS 抢占这段残渣空间用来做 Padding
                if (WROTE_POSITION_UPDATER.compareAndSet(this, currentPos, paddingPos)) {
                    // 占位成功，当前线程负责将 [currentPos, paddingPos) 区间执行 Padding 填充
                    doPadding(currentPos, (int) remainingInBlock);
                    // 核心：当前线程的真实业务数据并未写成功，必须继续循环去抢占下一个全新 Block 的空间
                    continue;
                }
                Thread.onSpinWait();
            }
            if (WROTE_POSITION_UPDATER.compareAndSet(this, currentPos, newPos)) {
                //抢成功跳出循环
                break;
            }
            // CAS失败，提示CPU这是spin等待
            Thread.onSpinWait();
        }
        //看看该文件是否超过了水位线,超过水位线触发触发 预创建文件
        if (isCreateNewFile == 0 && newPos >= manager.fileWaterMark && IS_CREATE_NEW_FILE.compareAndSet(this, 0, 1)) {
            manager.tryCreateNextFileWhenReachFileWaterMark(fileFromOffset + fileSize);
        }
        // 3. CAS 成功，当前线程独占 [currentPos, newPos) 区间，执行真正写入
        //因为文件开头4KB是元数据区域，因此真正的开头为 FILE_META_SIZE+currentPos
        AppendMessageResult result = doAppend(FILE_META_SIZE + currentPos, msgSize, dataStruct);
        if (result.isOk()) {
            this.metaDirty = true;
            //增加对应Block的期望字节数
            result.setLogicalIndex(logicalIndex);
            manager.blockMetaDataManager.addExpectedBytes(this.fileFromOffset, logicalIndex, dataStruct.getDataLen());
            log.info("WAL======>文件={}，block逻辑索引={}，写入数据长度={}", this.fileFromOffset, logicalIndex,
                    dataStruct.getDataLen());
        }
        return result;
    }

    private void doPadding(long startPos, int length) {
        // 假设你的 WalDataStruct 魔数是特定的，我们这里用一个绝对不会撞车的 PADDING_MAGIC
        // 只需要在残渣的开头写 8 个字节，后面的空间根本不需要去 fill(0)！
        MemorySegment segment = mappedMemorySegment.asSlice(startPos, length);
        segment.set(JAVA_INT, 0, PaddingStruct.PADDING_MAGIC);
        segment.set(JAVA_INT, 4, length);
        // 性能开销几乎为 0，同时完美解决了文件自解析和读取器阻塞的问题
    }

    /**
     * 真正将 WalDataStruct 数据写入 mappedMemorySegment（PageCache）的方法。
     * <p>
     * 此方法在 appendData 中通过 CAS 抢到独占写入区间后被调用，
     * 因此 [writeOffset, writeOffset + size) 区间由调用线程独占，无并发问题。
     * </p>
     *
     * @param writeOffset 写入的起始偏移量（由 CAS 抢到的 wrotePosition）
     * @param size        要写入的字节数（应等于 walDataStruct.getSerializedSize()）
     * @param dataStruct  磁盘持久化协议格式数据
     * @return AppendMessageResult 写入结果
     */
    private AppendMessageResult doAppend(final long writeOffset, final long size, final DataStruct dataStruct) {
        try {
            //创建一个新的 MemorySegment 视图，共享同一块底层内存，仅调整起始地址和长度，不复制数据。
            MemorySegment targetSlice = mappedMemorySegment.asSlice(writeOffset, size);
            //将数据写入到targetSlice中
            dataStruct.writeTo(targetSlice);
            return new AppendMessageResult(
                    AppendMessageResult.AppendStatus.PUT_OK,
                    System.currentTimeMillis(),
                    this.fileFromOffset
            );
        } catch (Exception e) {
            log.error("doAppend: failed to write data to mappedMemorySegment, writeOffset={}, size={}, fileName={}",
                    writeOffset, size, fileName, e);
            return AppendMessageResult.fail(AppendMessageResult.AppendStatus.UNKNOWN_ERROR, this.fileFromOffset);
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
                    "Failed to Clean Filed: " + dirPath + File.separator + fileName, e
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

    //返回true则代表还会继续更新upLoad指针
    public boolean isContinueUpdateUpLoadPosition() {
        return nextUploadBlockIndex.get() != totalBlockCount;
    }

}


