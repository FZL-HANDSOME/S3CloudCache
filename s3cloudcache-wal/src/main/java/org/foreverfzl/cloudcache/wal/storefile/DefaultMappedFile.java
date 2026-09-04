package org.foreverfzl.cloudcache.wal.storefile;

import org.foreverfzl.cloudcache.metadata.entity.BlockMetaData;
import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.FileMetaInfo;
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
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;


/**
 * 代表一个 操作系统文件，用于持久化数据
 */
public class DefaultMappedFile extends AbstractMappedFile {

    protected static final Logger log = LoggerFactory.getLogger(LogName.WAL_STORE_FILE);

    public static final AtomicLongFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;
    public static final AtomicLongFieldUpdater<DefaultMappedFile> READ_POSITION_UPDATER;
    public static final AtomicLongFieldUpdater<DefaultMappedFile> UPLOAD_POSITION_UPDATER;
    public static final AtomicIntegerFieldUpdater<DefaultMappedFile> IS_CREATE_NEW_FILE;

    public volatile boolean posActive; //该属性决定所有的指针是否可以更新
    public volatile long fileFromOffset;
    //文件分为两部分【元数据区域】【数据区域】
    //wrotePosition、readPosition、upLoadPosition都是针对数据区域的，这些指针为0则代表是元数据区域的0位置(实际位置为元数据区域+指针大小)
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

    protected int totalBlockCount; //该文件逻辑上对应多少个Block
    protected int blockSize;

    //blockStateArray[] 为0代表该block既没写也没上传，1代表该block数据全部写入到PageCache中，3代表该block已经上传到服务器
    protected short[] blockStateArray;
    //引入数组元素的 VarHandle，用于消灭原生数组的内存可见性缺陷
    private static final VarHandle SHORT_ARRAY_HANDLE;

    protected volatile int nextUploadBlockIndex = 0; //upLoadPosition指针期望下次更新index
    public static final AtomicIntegerFieldUpdater<DefaultMappedFile> NEXT_UPLOAD_INDEX_UPDATER;

    protected volatile int readBlockIndex = 0; //readPosition指针期望下次更新index
    public static final AtomicIntegerFieldUpdater<DefaultMappedFile> NEXT_READ_INDEX_UPDATER;
    protected static volatile int forceSize = 2 * 1024 * 1024; //每次刷盘大小

    //如果该文件的指针更新了该属性会被设置为1，然后MappedFileManager有专门的线程去更新该文件的元数据，更新完成后设置为0;
    public volatile int metaDirty;
    public static final AtomicIntegerFieldUpdater<DefaultMappedFile> DIRTY_UPDATER;


    static {
        WROTE_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        READ_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(DefaultMappedFile.class, "readPosition");
        UPLOAD_POSITION_UPDATER = AtomicLongFieldUpdater.newUpdater(DefaultMappedFile.class, "upLoadPosition");
        IS_CREATE_NEW_FILE = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "isCreateNewFile");
        DIRTY_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "metaDirty");
        NEXT_UPLOAD_INDEX_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "nextUploadBlockIndex");
        NEXT_READ_INDEX_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "readBlockIndex");
        // 初始化原生 long[] 数组的元素句柄
        SHORT_ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(short[].class);
    }


    public DefaultMappedFile(final String dirPath, final String fileName, final long fileFromOffset, final long fileSize, File file, final int blockSize, boolean isWarm, boolean isLockMemory, MappedFileManager manager) {
        this.posActive = true;
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.fileFromOffset = fileFromOffset;
        this.dirPath = dirPath;
        this.blockSize = blockSize;
        this.file = file;
        this.manager = manager;
        this.metaDirty = 0;
        this.totalBlockCount = (int) Math.ceil((double) fileSize / blockSize);
        blockStateArray = new short[totalBlockCount];
        init(isWarm, isLockMemory);
    }

    /**
     * 创建文件方法
     */
    public static DefaultMappedFile createFile(final String dirPath, final String fileName, final long fileFromOffset, final long fileSize, final int blockSize, boolean isWarm, boolean isLockMemory, MappedFileManager manager) {
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
        File file = new File(dirPath, fileName);
        //文件不存在则创建
        return new DefaultMappedFile(dirPath, fileName, fileFromOffset, fileSize, file, blockSize, isWarm, isLockMemory, manager);
    }


    /**
     * 文件的内存分配以及预热、锁定等
     */
    public void init(boolean isWarm, boolean isLockMemory) {
        try {
            arena = Arena.ofShared(); // 创建 MemorySegment 的生命周期控制对象
            // 1. 在打开句柄前，检测磁盘文件的真实存在性
            boolean fileExists = file.exists();
            if (!fileExists) {
                // 文件不存在：自动创建父级目录（避免 FileNotFoundException）
                File parentFile = file.getParentFile();
                if (parentFile != null && !parentFile.exists()) {
                    parentFile.mkdirs();
                }
            }
            // 2. 以 "rw" 读写模式打开文件句柄
            // 说明：RandomAccessFile 在 "rw" 模式下，若文件存在则直接打开且【不会覆盖/清空原数据】；若不存在则自动创建 0 字节文件。
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            long physicalFileSize = FileMetaInfo.FILE_META_SIZE + fileSize;
            if (randomAccessFile.length() < physicalFileSize) {
                // 文件由“4KB 元数据区 + fileSize 数据区”组成。
                randomAccessFile.setLength(physicalFileSize);
            }
            // 3. 获取 FileChannel 并完成内存映射
            fileChannel = randomAccessFile.getChannel();
            mappedMemorySegment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, physicalFileSize, arena);
            // 4. 按需预热
            if (isWarm) {
                // 进行文件预热，每 16384 页（64MB）刷盘一次，防止脏页过多
                warm(16384, isLockMemory);
            }
        } catch (Exception e) {
            throw new WalException("Failed to initialize cache file: " + fileName, e);
        }
    }


    /**
     * 推进read指针的更新，目前read指针的更新没有涉及到多线程争抢
     */
    public void ackReadPosition() {
        while (true) {
            if (!posActive) return;
            int curReadBlockIndex = NEXT_READ_INDEX_UPDATER.get(this);
            if (curReadBlockIndex == totalBlockCount) {
                return;
            }
            short state = (short) SHORT_ARRAY_HANDLE.getVolatile(this.blockStateArray, curReadBlockIndex);
            //如果状态为1或者3，则代表数据全部落入到PageCache中
            if (state == 0) {
                break;
            }
            long curReadPosition = READ_POSITION_UPDATER.get(this);
            long expectedNewPosition = (curReadBlockIndex + 1L) * blockSize;
            int count = (int) ProjectUtil.divideByPower(blockSize, forceSize);
            MemorySegment target = null;
            long curPos = curReadPosition;
            boolean isSuccess = true;
            try {
                for (int i = 0; i < count; i++) {
                    target = mappedMemorySegment.asSlice(FileMetaInfo.FILE_META_SIZE + curPos, forceSize);
                    target.force();
                    curPos += forceSize;
                }
            } catch (Exception e) {
                isSuccess = false;
                log.warn("fileName= {} ackReadPosition failed,newReadpos= {}.  ", this.fileFromOffset, expectedNewPosition,e);
            }
            //刷盘成功更新指针
            if (isSuccess) {
                READ_POSITION_UPDATER.set(this, expectedNewPosition);
                NEXT_READ_INDEX_UPDATER.incrementAndGet(this);
                DIRTY_UPDATER.set(this, 1);
                log.info("fileName= {} ackReadPosition successfully,newReadpos= {}", this.fileFromOffset, expectedNewPosition);
            }
        }
    }

    /**
     * 物理和逻辑Block解耦 + 无锁账本 推进 upLoadPosition指针，该方法只是将预期结果填到坑里面，有专门的线程去检查指针
     *
     * @param logicalIndex 当前完成上传的 Block 在本文件内部的逻辑序号 (0, 1, 2...)
     */
    public void ackUpLoadPosition(int logicalIndex) {
        // 1. 物理填坑：利用 VarHandle 的 Volatile 语义写入，确保其他 CPU 核心立即可见
        setBlockStateArrayFinishedUpLoad(logicalIndex);
        //每个线程都去看一下是否能进行更新
        while (true) {
            if (!posActive) return;
            // 在循环外固定当前要检查的索引
            int currentIndex = NEXT_UPLOAD_INDEX_UPDATER.get(this);
            if (currentIndex == totalBlockCount) {
                return;
            }
            // 检查当前索引位置是否已填坑
            short state = (short) SHORT_ARRAY_HANDLE.getVolatile(this.blockStateArray, currentIndex);
            if (state == 0 || state == 1) {
                // 当前 block 还没上传完，无法推进
                break;
            }
            // 尝试原子推进上传指针
            long curUpLoadPosition = UPLOAD_POSITION_UPDATER.get(this);
            long expectedNewPosition = (long) (currentIndex + 1) * blockSize;
            if (curUpLoadPosition >= expectedNewPosition) {
                break;
            }
            if (UPLOAD_POSITION_UPDATER.compareAndSet(this, curUpLoadPosition, expectedNewPosition)) {
                if (metaDirty == 0) {
                    DIRTY_UPDATER.compareAndSet(this, 0, 1);
                }
                // CAS 成功，原子递增索引
                NEXT_UPLOAD_INDEX_UPDATER.incrementAndGet(this);
                continue;
            } else {
                break;
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
        log.info("file {} warm pages successfully", this.fileFromOffset);
        //todo 还不知道是否真正能锁定内存
        //根据用户选择是否锁定内存
        if (isLockMemory) {
            ProjectUtil.lockMappedPages(this.mappedMemorySegment);
        }
    }

    //跳过开头元数据区域获取数据
    public int getIntFromDataArea(long fromOffset) {
        return mappedMemorySegment.get(JAVA_INT, FileMetaInfo.FILE_META_SIZE + fromOffset);
    }

    public long getLongFromDataArea(long fromOffset) {
        return mappedMemorySegment.get(JAVA_LONG, FileMetaInfo.FILE_META_SIZE + fromOffset);
    }

    //从指定位置获取大小为sie的原始数据
    public byte[] getOrgDataFromDataArea(long fromOffset, int size) {
        byte[] valueBytes = new byte[size];
        MemorySegment.copy(mappedMemorySegment, ValueLayout.JAVA_BYTE, FileMetaInfo.FILE_META_SIZE + fromOffset, valueBytes, 0, size);
        return valueBytes;
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
        if (!isAvailable()) {
            return AppendMessageResult.fail(this, AppendMessageResult.AppendStatus.FILE_CLOSED, this.fileFromOffset);
        }
        if (dataStruct == null) {
            return AppendMessageResult.fail(this, AppendMessageResult.AppendStatus.INVALID_ARGUMENT, this.fileFromOffset);
        }
        long msgSize = dataStruct.getSerializedSize();
        // 单条消息比一个逻辑 Block 还大时，永远无法写入（会一路 padding 到文件末尾并不断新建文件），直接拒绝
        if (msgSize > this.blockSize) {
            return AppendMessageResult.fail(this, AppendMessageResult.AppendStatus.MESSAGE_TOO_LARGE, this.fileFromOffset);
        }
        // 2. CAS 自旋抢占 wrotePosition，为当前线程分配写入区域
        long currentPos;
        long newPos;
        //采用空间预留 解耦物理和逻辑Block，先分配逻辑Block，然后将逻辑Block信息放入到AppendMessageResult中
        //logicalIndex算出该数据在哪个逻辑Block中，divideByPower(newPos,blockSize)等价于 newPos/blockSize
        int logicalIndex = -1;
        AppendMessageResult result = null;
        BlockMetaData blockMetaData = null;
        BlockMetaDataManager blockMetaDataManager = manager.blockMetaDataManager;
        //获取该文件的引用
        this.hold();
        long blockOffset;
        try {
            while (true) {
                currentPos = WROTE_POSITION_UPDATER.get(this);
                // 文件已写满（wrotePosition 已到达 fileSize，通常是最后一块被封口后指针被推到边界）。
                // 此时本文件已无空间，必须直接返回 END_OF_FILE 让上层切换到新文件重试；
                // 否则会把 wrotePosition 继续推到 fileSize 之外，造成越界写或 "File is closed"。
                if (currentPos >= this.fileSize) {
                    close();
                    return AppendMessageResult.fail(this, AppendMessageResult.AppendStatus.END_OF_FILE, this.fileFromOffset);
                }
                newPos = currentPos + msgSize;
                logicalIndex = Math.toIntExact(ProjectUtil.divideByPower(currentPos, blockSize));
                // 检查该数据是否跨逻辑Block了。currentPos & (this.blockSize - 1)等价于 currentPos%blockSize
                blockOffset = currentPos & (this.blockSize - 1);
                long remainingInBlock = this.blockSize - blockOffset;
                long paddingPos;
                if (msgSize > remainingInBlock) {
                    // 发现空间不够写整条消息，强行将写指针推到当前 Block 的绝对终点（即下一个 Block 的起点）
                    paddingPos = currentPos + remainingInBlock;
                    //看看文件是否结尾
                    // 尝试 CAS 抢占这段残渣空间用来做 Padding
                    if (WROTE_POSITION_UPDATER.compareAndSet(this, currentPos, paddingPos)) {
                        //将该block设置为封口，并根据返回的位掩码处理后续动作
                        // bit0=数据已全部落入PageCache(可推进read指针)
                        // bit1=该Block已满足上传条件(投递上传任务)
                        // bit2=该Block物理写入损坏(投递恢复任务)
                        int sealResult = blockMetaDataManager.trySeal(this.fileFromOffset, logicalIndex);
                        if ((sealResult & 1) == 1) {
                            setBlockStateArrayFinishedPageCache(logicalIndex);
                        }
                        if ((sealResult & (1 << 1)) == (1 << 1)) {
                            // 关键修复：封口时若 expectedBytes==pageCacheBytes==finishedBytes 已对齐，
                            // 必须主动投递上传任务。否则最后一个业务线程的 releaseReference 已先于封口执行，
                            // 该Block会永远停留在"已封口且字节对齐但无人触发上传"的状态。
                            blockMetaDataManager.setTaskToUpdateQueue(this.fileFromOffset, logicalIndex);
                        }
                        if ((sealResult & (1 << 2)) == (1 << 2)) {
                            blockMetaDataManager.setTaskToRecoverQueue(blockMetaDataManager.getBlockMetaData(this.fileFromOffset, logicalIndex), this.fileFromOffset, logicalIndex);
                        }
                        //如果是文件结尾则直接返回
                        if (paddingPos == this.fileSize) {
                            close(); //关闭文件
                            return AppendMessageResult.fail(this, AppendMessageResult.AppendStatus.END_OF_FILE, this.fileFromOffset);
                        }
                        // 核心：当前线程的真实业务数据并未写成功，必须继续循环去抢占下一个全新 Block 的空间
                        continue;
                    }
                    Thread.onSpinWait();
                    continue;
                }
                //检查该block是否已经封口，或者对应的Block已经Broken了
                if (blockMetaDataManager.isSealed(this.fileFromOffset, logicalIndex)) {
                    //封口了则尝试将指针设置为下一个Block起点
                    paddingPos = currentPos + remainingInBlock;
                    // 尝试 CAS 抢占这段残渣空间用来做 Padding
                    if (WROTE_POSITION_UPDATER.compareAndSet(this, currentPos, paddingPos)) {
                        //如果是文件结尾则直接返回
                        if (paddingPos == this.fileSize) {
                            close(); //关闭文件
                            return AppendMessageResult.fail(this, AppendMessageResult.AppendStatus.END_OF_FILE, this.fileFromOffset);
                        }
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
            int dataSize = dataStruct.getDataLen();
            //增加期望字节数
            blockMetaDataManager.addExpectedBytes(this.fileFromOffset, logicalIndex, dataSize);
            //看看该文件是否超过了水位线,超过水位线触发触发 预创建文件
            if (isCreateNewFile == 0 && newPos >= manager.fileWaterMark && IS_CREATE_NEW_FILE.compareAndSet(this, 0, 1)) {
                manager.tryCreateNextFileWhenReachFileWaterMark(fileFromOffset + FileMetaInfo.FILE_META_SIZE + fileSize);
            }
            // 3. CAS 成功，当前线程独占 [currentPos, newPos) 区间，执行真正写入
            //因为文件开头4KB是元数据区域，因此真正的开头为 FILE_META_SIZE+currentPos
            result = doAppend(FileMetaInfo.FILE_META_SIZE + currentPos, msgSize, dataStruct);
            if (result.isOk()) {
                result.setLogicalIndex(logicalIndex);
                result.setBlockOffset(blockOffset);
                //写入成功，增加写入到PageCache字节数
                blockMetaData = blockMetaDataManager.addPageCacheBytes(this.fileFromOffset, logicalIndex, dataSize);
            }
        } finally {
            int release = this.release();
            if (release == 0 && blockMetaData != null && logicalIndex != -1 && blockMetaDataManager.isAllDataWriteInPageCache(blockMetaData)) {
                //最后一个线程去检查全部数据是否全部写入到PageCache中
                setBlockStateArrayFinishedPageCache(logicalIndex);
            }
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
     * @param writeOffset 写入的起始偏移量（由 CAS 抢到的 wrotePosition）
     * @param size        要写入的字节数（应等于 walDataStruct.getSerializedSize()）
     * @param dataStruct  磁盘持久化协议格式数据
     * @return AppendMessageResult 写入结果
     */
    private AppendMessageResult doAppend(final long writeOffset, final long size, final DataStruct dataStruct) {
        try {
            // 注意：这里不再检查 isAvailable()。
            // appendData 开头已检查过 isAvailable()，且当前线程通过 hold() 持有引用，
            // mmap 在 refCount 归零前不会被清理；写指针也由 CAS 保证 <= fileSize。
            // 若这里再检查，会出现“文件刚好切走导致在途线程写入失败(File is closed)”的竞态。
            MemorySegment targetSlice = mappedMemorySegment.asSlice(writeOffset, size);
            //将数据写入到targetSlice中
            dataStruct.writeTo(targetSlice);
            return new AppendMessageResult(this, AppendMessageResult.AppendStatus.PUT_OK, System.currentTimeMillis(), this.fileFromOffset);
        } catch (Exception e) {
            log.error("doAppend: failed to write data to mappedMemorySegment, writeOffset={}, size={}, fileName={}", writeOffset, size, fileName, e);
            //出现异常关闭文件
            this.close();
            return AppendMessageResult.fail(this, AppendMessageResult.AppendStatus.WRITER_FAILED, this.fileFromOffset);
        }
    }


    /**
     * 释放资源的方法
     */
    @Override
    public void clean() {
        try {
            if (isCleanup()) {
                return;
            }
            this.setClean();
            if (arena != null) {
                arena.close();
                arena = null;
            }
            if (fileChannel != null) {
                fileChannel.close();
                fileChannel = null;
            }
            mappedMemorySegment = null;
            blockStateArray = null;
        } catch (Exception e) {
            log.warn("{} file clean failed", this.fileFromOffset, e);
        }
    }

    @Override
    public void delete() {
        //真正删除文件
        String path = null;
        try {
            if (file != null && file.exists()) {
                path = file.getAbsolutePath();
                Files.delete(file.toPath());
                manager.removeMappedFile(this.fileFromOffset);
                log.info("The file has been deleted: {}", path);
            }
        } catch (Exception e) {
            log.warn("{} file delete failed", path, e);
        }

    }

    public void setBlockStateArrayFinishedPageCache(int blockIndex) {
        SHORT_ARRAY_HANDLE.setVolatile(this.blockStateArray, blockIndex, (short) 1);
    }

    public void setBlockStateArrayFinishedUpLoad(int blockIndex) {
        SHORT_ARRAY_HANDLE.setVolatile(this.blockStateArray, blockIndex, (short) 3);
    }

    //是否清除资源，true代表可以
    public boolean canClean() {
        return getRefCount() == 0 && !isAvailable() && readPosition == upLoadPosition;
    }

    @Override
    public String getFileName() {
        return this.fileName;
    }

    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public FileChannel getFileChannel() {
        return this.fileChannel;
    }

    public MemorySegment getBlockMappedMemorySegmentSlice(int blockIndex) {
        long fromOffset = FileMetaInfo.FILE_META_SIZE + (long) blockIndex * blockSize;
        return mappedMemorySegment.asSlice(fromOffset, blockSize);
    }

    public MemorySegment getMappedMemorySegmentSlice(long fromOffset, long size) {
        return mappedMemorySegment.asSlice(fromOffset, size);
    }

    //返回true则代表还会继续更新upLoad指针
    public boolean isContinueUpdateUpLoadPosition() {
        return NEXT_UPLOAD_INDEX_UPDATER.get(this) != totalBlockCount;
    }

    public MappedFileManager getManager() {
        return manager;
    }
}


