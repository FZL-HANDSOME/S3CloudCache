package org.foreverfzl.cloudcache.wal.storefile;

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
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
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

    public volatile long fileFromOffset;
    public volatile long wrotePosition; //数据写入位置
    public volatile long readPosition; //可读位置，0~readPosition位置可读,此位置一定是写入到了文件中
    public volatile long upLoadPosition; //该文件上传到云服务器的位置
    private final MappedFileManager manager;

    protected File file;
    protected String dirPath;
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
    //引入数组元素的 VarHandle，用于消灭原生数组的内存可见性缺陷
    private static final VarHandle ARRAY_ELEMENT_HANDLE;


    static {
        WROTE_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        READ_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(DefaultMappedFile.class, "readPosition");
        UPLOAD_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(DefaultMappedFile.class, "upLoadPosition");
        // 初始化原生 long[] 数组的元素句柄
        ARRAY_ELEMENT_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);
    }


    public DefaultMappedFile(final String dirPath, final String fileName, final long fileFromOffset,
                             final long fileSize, final int blockSize, boolean isWarm, boolean isLockMemory, MappedFileManager manager) {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.fileFromOffset = fileFromOffset;
        this.dirPath = dirPath;
        this.blockSize = blockSize;
        this.manager = manager;
        this.totalBlockCount = (int) Math.ceil((double) fileSize / blockSize);
        blockEndOffsetInMappedFile = new long[totalBlockCount];
        arena = Arena.ofShared(); //创建MS的控制对象
        init(isWarm, isLockMemory);
    }


    /**
     * 创建并初始化WAL文件，并且将channel、指针等初始化
     *
     */
    @Override
    public void init(boolean isWarm, boolean isLockMemory) {
        if (fileName == null || fileName.isBlank()) {
            throw new WalException("fileName cannot be null");
        }
        if (dirPath == null || dirPath.isBlank()) {
            throw new WalException("fileName cannot be null");
        }
        if (fileSize <= 0) {
            throw new WalException("fileSize must be greater than 0");
        }
        try {
            // 创建目录
            File dir = new File(dirPath);
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
     * 物理和逻辑Block解耦 + 无锁账本 推进 upLoadPosition指针
     *
     * @param logicalIndex    当前完成上传的 Block 在本文件内部的逻辑序号 (0, 1, 2...)
     * @param endOffsetInFile 当前 Block 在文件内部的绝对结束位点 (如 8192, 16384...)
     */
    public void ackUpLoadPosition(int logicalIndex, long endOffsetInFile) {
        // 边界防御：防止脏数据引发数组越界
        if (logicalIndex < 0 || logicalIndex >= totalBlockCount) {
            log.error("Invalid logical index: {}, totalBlockCount: {}", logicalIndex, totalBlockCount);
            return;
        }
        // 1. 物理填坑：利用 VarHandle 的 Volatile 语义写入，确保其他 CPU 核心立即可见
        ARRAY_ELEMENT_HANDLE.setVolatile(this.blockEndOffsetInMappedFile, logicalIndex, endOffsetInFile);
        // 2. 级联推进滑窗：尝试多线程无锁并发检查并更新连续下标
        int currIndex;
        // 顺着多米诺骨牌的期望线一路向右检查
        while ((currIndex = nextUploadBlockIndex.get()) < totalBlockCount) {
            // 3. 利用 VarHandle 的 Volatile 语义读取，防止因缓存延迟读到旧值 0
            long nextOffset = (long) ARRAY_ELEMENT_HANDLE.getVolatile(this.blockEndOffsetInMappedFile, currIndex);
            if (nextOffset == 0) {
                // 核心卡点：当前大盘正在死等的那块骨牌还没传完，滑窗卡住，直接退出
                break;
            }
            // 4. 抢夺骨牌推进权：谁能把期望指针从 currIndex 顶到 currIndex + 1，谁就接管了这一档的推进权
            if (nextUploadBlockIndex.compareAndSet(currIndex, currIndex + 1)) {
                // 5. 守护连续性：通过 CAS 自旋把大盘位点顶高到 nextOffset
                long currentPos;
                while ((currentPos = UPLOAD_POSITION_UPDATER.get(this)) < nextOffset) {
                    // 如果大盘位点已经被速度更快的级联线程推得更高了，当前线程的 CAS 就会失败并顺势退出
                    if (UPLOAD_POSITION_UPDATER.compareAndSet(this, currentPos, nextOffset)) {
                        break;
                    }
                }
                // 抢到权的线程不能歇着，继续进入下一次 while 循环，检查下一颗骨牌是不是早就被别的网络线程填好了
            } else {
                // CAS 失败说明别的线程手快，已经把期望指针推上去了，当前线程继续自旋跟进
                Thread.onSpinWait(); // 提示 CPU 此时线程处于自旋等待状态，可以适当降低消耗进行优化
            }
        }
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

            int version = slice.get(JAVA_INT, pos);
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
            return new WalDataStruct(magic, version, checksum, valueLen, valueBytes);
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
     * @param walDataStruct 磁盘持久化协议格式
     * @return true 表示写入成功，false 表示写入失败
     */
    @Override
    public AppendMessageResult appendData(final WalDataStruct walDataStruct) {
        if (!isAvailable()) return new AppendMessageResult(AppendMessageResult.AppendStatus.FILE_CLOSED, this.fileName);
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
        while (true) {
            currentPos = WROTE_POSITION_UPDATER.get(this);
            newPos = currentPos + msgSize;
            if (newPos > this.fileSize) {
                close(); //文件设置为关闭
                return AppendMessageResult.fail(AppendMessageResult.AppendStatus.END_OF_FILE, this.fileName);
            }
            // 检查该数据是否跨逻辑Block了
            long blockOffset = currentPos & (this.blockSize - 1);
            long remainingInBlock = this.blockSize - blockOffset;
            if (msgSize > remainingInBlock) {
                // 发现空间不够写整条消息，强行将写指针推到当前 Block 的绝对终点（即下一个 Block 的起点）
                long paddingPos = currentPos + remainingInBlock;
                if (paddingPos == this.fileSize) {
                    close();
                    return AppendMessageResult.fail(AppendMessageResult.AppendStatus.END_OF_FILE, this.fileName);
                }
                // 尝试 CAS 抢占这段残渣空间用来做 Padding
                if (WROTE_POSITION_UPDATER.compareAndSet(this, currentPos, paddingPos)) {
                    // 占位成功，当前线程负责将 [currentPos, paddingPos) 区间执行 Padding 填充
                    doPadding(currentPos, (int) remainingInBlock);
                    // 核心：当前线程的真实业务数据并未写成功，必须继续循环去抢占下一个全新 Block 的空间
                    continue;
                }
                Thread.onSpinWait();
                continue;
            }
            if (WROTE_POSITION_UPDATER.compareAndSet(this, currentPos, newPos)) {
                break;
            }
            // CAS失败，提示CPU这是spin等待
            Thread.onSpinWait();
        }
        //采用空间预留 解耦物理和逻辑Block，先分配逻辑Block，然后将逻辑Block信息放入到AppendMessageResult中，最后调用core模块
        int logicalIndex = Math.toIntExact(newPos / blockSize);
        // 3. CAS 成功，当前线程独占 [currentPos, newPos) 区间，执行真正写入
        AppendMessageResult result = doAppend(currentPos, msgSize, walDataStruct);
        result.setLogicalIndex(logicalIndex);
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
            //  Magic
            targetSlice.set(
                    JAVA_INT,
                    pos,
                    walDataStruct.getMagic()
            );
            pos += 4;

            //version
            targetSlice.set(
                    JAVA_INT,
                    pos,
                    walDataStruct.getVersion()
            );
            pos += 4;

            // CRC32
            targetSlice.set(
                    JAVA_INT,
                    pos,
                    walDataStruct.getChecksum()
            );
            pos += 4;

            // Value Length
            targetSlice.set(
                    JAVA_INT,
                    pos,
                    walDataStruct.getValueLen()
            );
            pos += 4;

            // Value Bytes
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

    public long getFileSize() {
        return fileSize;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }
}


