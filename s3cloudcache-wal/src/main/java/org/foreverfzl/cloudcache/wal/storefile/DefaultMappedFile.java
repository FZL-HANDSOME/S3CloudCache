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
/**
 * 中文：持有一个WAL文件的共享Arena映射：4096字节头后为fileSize字节数据区。写入预留按逻辑块元数据锁协调，记录复制在锁外；常规刷盘、检查点确认和卸载由文件锁协调，初始化预热另需独占生命周期。close仅关闭准入，引用数不会自动触发清理。
 * English: Owns a shared-arena WAL mapping: a 4096-byte header followed by fileSize data bytes. Reservation is coordinated by logical-block metadata locks, with copying outside them; normal forcing, checkpoint acknowledgement and unmapping use the file monitor, while initial warmup requires exclusive lifecycle access. close only closes admission, and references do not automatically trigger cleanup.
 */
public class DefaultMappedFile extends AbstractMappedFile {

    /**
     * 中文：WAL映射与刷盘诊断日志。
     * English: WAL mapping and forcing diagnostics.
     */
    protected static final Logger log = LoggerFactory.getLogger(LogName.WAL_STORE_FILE);

    /**
     * 中文：原子预留数据区写入字节范围；预留不等于实际复制完成。
     * English: Atomically reserves data-area byte ranges; reservation is not copy completion.
     */
    public static final AtomicLongFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;
    /**
     * 中文：读/恢复检查点更新器，通常在文件锁下推进。
     * English: Read/recovery checkpoint updater, normally advanced under the file monitor.
     */
    public static final AtomicLongFieldUpdater<DefaultMappedFile> READ_POSITION_UPDATER;
    /**
     * 中文：连续上传水位线更新器；逐块确认另存文件头。
     * English: Contiguous upload watermark updater; per-block acknowledgements are stored separately in the header.
     */
    public static final AtomicLongFieldUpdater<DefaultMappedFile> UPLOAD_POSITION_UPDATER;
    /**
     * 中文：预创建提交标志的CAS更新器，不代表预创建已经成功。
     * English: CAS updater for precreation submission, not proof that precreation succeeded.
     */
    public static final AtomicIntegerFieldUpdater<DefaultMappedFile> IS_CREATE_NEW_FILE;

    /**
     * 中文：更正旧概括：当前主要控制read/upload循环推进，不是写入准入门禁；ackUpload仍先写确认字节。
     * English: Clarifies the old summary: currently gates read/upload advancement loops, not write admission; ackUpload still writes the acknowledgement byte first.
     */
    public volatile boolean posActive; //该属性决定所有的指针是否可以更新
    /**
     * 中文：文件身份/逻辑起点，参与名称和S3 Key；不应加到当前映射的文件内访问位置。
     * English: File identity/logical start used by names and S3 keys; do not add it to offsets within this mapping.
     */
    public volatile long fileFromOffset;
    //文件分为两部分【元数据区域】【数据区域】
    //wrotePosition、readPosition、upLoadPosition都是针对数据区域的，这些指针为0则代表是元数据区域的0位置(实际位置为元数据区域+指针大小)
    /**
     * 中文：相对数据区的预留高水位，含记录头、对齐和跳过尾部；0对应物理文件偏移4096，不是头部起点。
     * English: Reservation high-water mark relative to the data area, including headers, alignment, and skipped tails; zero means physical offset 4096, not the header start.
     */
    public volatile long wrotePosition; //数据写入位置
    /**
     * 中文：按块推进的读/恢复检查点，单位数据区字节；正常路径在force后推进，恢复路径也会合并上传确认位置。
     * English: Block-aligned read/recovery checkpoint in data-area bytes; normally advanced after force, and merged with upload acknowledgement during restoration.
     */
    public volatile long readPosition; //可读位置，0~readPosition位置可读,此位置一定是写入到了文件中
    /**
     * 中文：连续完成上传的块末端，单位数据区字节；乱序成功由状态数组和持久化确认字节表示。
     * English: End of consecutively uploaded blocks in data-area bytes; out-of-order success uses the state array and durable acknowledgement bytes.
     */
    public volatile long upLoadPosition; //该文件上传到云服务器的位置
    /**
     * 中文：所属Bucket管理器，提供元数据账本、预创建和索引删除。
     * English: Owning bucket manager providing metadata, precreation, and index removal.
     */
    private final MappedFileManager manager;

    //该属性就是看看超过水位线后是否已经开启创建新文件了，0代表未创建，1代表创建
    /**
     * 中文：0表示尚未提交预创建，1表示已尝试提交；不是新文件存在性的判断。
     * English: Zero means precreation not submitted; one means submission attempted, not that the new file exists.
     */
    private volatile int isCreateNewFile = 0;

    /**
     * 中文：磁盘路径对象；clean后保留以便delete删除文件。
     * English: Disk-path object, retained after clean so delete can remove the file.
     */
    protected File file; //文件的引用
    /**
     * 中文：WAL目录路径。
     * English: WAL directory path.
     */
    protected String dirPath;
    /**
     * 中文：映射文件名，通常是fileFromOffset的十进制表示。
     * English: Mapped filename, normally the decimal fileFromOffset.
     */
    protected String fileName;
    /**
     * 中文：数据区容量，单位字节，不包含4096字节头。
     * English: Data-area capacity in bytes, excluding the 4096-byte header.
     */
    public long fileSize;
    /**
     * 中文：映射用的底层通道，由本文件清理流程关闭。
     * English: Underlying mapping channel closed by this file's cleanup.
     */
    protected FileChannel fileChannel;
    /**
     * 中文：共享Arena拥有整个映射的生命周期；关闭后所有派生切片失效。
     * English: Shared arena owns the mapping lifetime; all derived slices become invalid after closure.
     */
    protected Arena arena;
    /**
     * 中文：整个物理映射，包含头和数据区；不是独立分配的业务Value缓冲。
     * English: Full physical mapping including header and data area, not a standalone value buffer.
     */
    protected MemorySegment mappedMemorySegment; //本质是MMP内存映射

    /**
     * 中文：文件逻辑块数，构造时约束为1..1024。
     * English: Logical block count, constrained to 1..1024 at construction.
     */
    protected int totalBlockCount; //该文件逻辑上对应多少个Block
    /**
     * 中文：逻辑块容量，字节，必须是2次幂以匹配位运算。
     * English: Logical block bytes, required to be a power of two for bit arithmetic.
     */
    protected int blockSize;

    //blockStateArray[] 为0代表该block既没写也没上传，1代表该block数据全部写入到PageCache中，3代表该block已经上传到服务器
    /**
     * 中文：0表示尚未声明可刷盘，并不表示完全没写；1表示完整PageCache数据可刷盘；3表示上传完成。该数组是易失内存账本。
     * English: Zero means not yet declared flushable, not necessarily unwritten; one means complete page-cache data is flushable; three means uploaded. This is a volatile in-memory ledger.
     */
    protected short[] blockStateArray;
    //引入数组元素的 VarHandle，用于消灭原生数组的内存可见性缺陷
    /**
     * 中文：short[]元素的可见性/CAS句柄；更正旧注释中long[]描述。
     * English: Visibility/CAS handle for short[] elements, correcting the historical long[] comment.
     */
    private static final VarHandle SHORT_ARRAY_HANDLE;

    /**
     * 中文：下一待确认的连续块序号，停止于首个上传缺口。
     * English: Next contiguous block index to acknowledge, stopping at the first upload gap.
     */
    protected volatile int nextUploadBlockIndex = 0; //upLoadPosition指针期望下次更新index
    /**
     * 中文：连续上传索引更新器，与上传字节位点配套。
     * English: Updater for the contiguous upload index paired with the upload byte position.
     */
    public static final AtomicIntegerFieldUpdater<DefaultMappedFile> NEXT_UPLOAD_INDEX_UPDATER;

    /**
     * 中文：下一待force的连续块序号；恢复时必须和readPosition一起初始化。
     * English: Next contiguous block to force; restore it together with readPosition.
     */
    protected volatile int readBlockIndex = 0; //readPosition指针期望下次更新index
    /**
     * 中文：读索引更新器，与读字节位点配套。
     * English: Read-index updater paired with the read byte position.
     */
    public static final AtomicIntegerFieldUpdater<DefaultMappedFile> NEXT_READ_INDEX_UPDATER;
    /**
     * 中文：分段force目标大小，默认2MiB；最后一段可更短，必须保持为正数。
     * English: Target force chunk size, default 2MiB; the last chunk may be shorter and the size must stay positive.
     */
    protected static volatile int forceSize = 2 * 1024 * 1024; //每次刷盘大小
    // 保留头部的前 64 字节给位置等元数据。每块一个持久化确认字节，兼容旧文件中的零填充。
    /**
     * 中文：WAL头内偏移64起，每逻辑块使用一个字节，值1代表持久化确认；不是每块一bit的压缩位图。
     * English: One byte per logical block from WAL-header offset 64; value one is a durable acknowledgement, not a packed one-bit bitmap.
     */
    private static final long UPLOADED_BLOCKS_OFFSET = 64;

    //如果该文件的指针更新了该属性会被设置为1，然后MappedFileManager有专门的线程去更新该文件的元数据，更新完成后设置为0;
    /**
     * 中文：位置检查点是否待写入文件头；单块上传确认使用独立的即时force。
     * English: Whether position checkpoints await header persistence; per-block upload acknowledgements use a separate immediate force.
     */
    public volatile int metaDirty;
    /**
     * 中文：检查点dirty标志更新器。
     * English: Checkpoint dirty-flag updater.
     */
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
        // 中文：实际句柄对应short[]；上方long[]是历史注释，不应据此改变元素宽度。
        // English: The handle actually targets short[]; the historical long[] comment is not a reason to change element width.
        SHORT_ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(short[].class);
    }


    /**
     * 中文：校验块布局后创建共享映射；已有文件不因isWarm而被写零，恢复位点必须由调用者显式恢复。
     * English: Validates layout then creates a shared mapping. Existing files are not zero-warmed; callers must explicitly restore recovery positions.
     *
     * @param dirPath 中文：WAL目录；English: WAL directory
     * @param fileName 中文：文件名；English: filename
     * @param fileFromOffset 中文：文件身份编号；English: file identity
     * @param fileSize 中文：数据区字节数；English: data-area bytes
     * @param file 中文：实际文件路径对象；English: actual file-path object
     * @param blockSize 中文：单块字节数；English: bytes per block
     * @param isWarm 中文：是否预热新建文件；English: whether to warm a newly created file
     * @param isLockMemory 中文：预热后是否请求锁页；English: whether to request page locking after warmup
     * @param manager 中文：所属Bucket管理器；English: owning bucket manager
     * @throws IllegalArgumentException 中文：块不是2次幂、文件非整块或超过1024块；English: non-power-of-two block, fractional file layout, or more than 1024 blocks
     */
    public DefaultMappedFile(final String dirPath, final String fileName, final long fileFromOffset, final long fileSize, File file, final int blockSize, boolean isWarm, boolean isLockMemory, MappedFileManager manager) {
        if (blockSize <= 0 || (blockSize & (blockSize - 1)) != 0
                || fileSize <= 0 || fileSize % blockSize != 0 || fileSize / blockSize > 1024) {
            throw new IllegalArgumentException("WAL requires a power-of-two blockSize, an integral block count, and at most 1024 blocks");
        }
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
    /**
     * 中文：检查名称/目录/正容量后委托构造；不自动注册到管理器索引。
     * English: Validates name/directory/positive capacity and delegates construction; it does not register with the manager.
     *
     * @param dirPath 中文：WAL目录；English: WAL directory
     * @param fileName 中文：文件名；English: filename
     * @param fileFromOffset 中文：文件身份；English: file identity
     * @param fileSize 中文：数据区容量，字节；English: data-area capacity in bytes
     * @param blockSize 中文：逻辑块容量，字节；English: block capacity in bytes
     * @param isWarm 中文：是否预热新文件；English: whether to warm new files
     * @param isLockMemory 中文：是否请求锁页；English: whether to request page locking
     * @param manager 中文：所属管理器；English: owning manager
     * @return 中文：已初始化映射对象；English: initialized mapping object
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
    /**
     * 中文：创建共享Arena，必要时扩展到文件头加数据区大小并映射；仅对不存在的新文件做写零预热。此方法不是重复初始化接口。
     * English: Creates a shared arena, extends to header plus data-area size when needed, and maps. Only newly absent files are zero-warmed; this is not a reinitialization API.
     *
     * @param isWarm 中文：新建文件预热开关；English: new-file warmup flag
     * @param isLockMemory 中文：预热路径的锁页请求；English: page-lock request for the warmup path
     * @throws WalException 中文：初始化或映射失败；English: initialization or mapping fails
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
            // 中文：已有文件可能含恢复数据，不能对其执行逐页写零预热。
            // English: Existing files may contain recovery data and must not be zero-warmed page by page.
            if (isWarm && !fileExists) {
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
    /**
     * 中文：在文件锁下逐块force完整PageCache区间，成功后更新读索引/位点/dirty；遇缺口停止，force失败仅记录并退出本轮。它不是仅修改计数的无锁方法。
     * English: Under the file monitor, forces complete page-cache blocks and then advances read index/position/dirty. Stops at gaps; force failure logs and ends this pass. This is not a lock-free counter-only operation.
     */
    public synchronized void ackReadPosition() {
        while (true) {
            if (!posActive || isCleanup()) return;
            int curReadBlockIndex = NEXT_READ_INDEX_UPDATER.get(this);
            if (curReadBlockIndex >= totalBlockCount) {
                return;
            }
            short state = (short) SHORT_ARRAY_HANDLE.getVolatile(this.blockStateArray, curReadBlockIndex);
            //如果状态为1或者3，则代表数据全部落入到PageCache中
            // 中文：连续读水位遇到未完成块即停止，不能越过空洞把后续块一起宣布已刷盘。
            // English: A contiguous read watermark stops at an incomplete block rather than declaring later blocks flushed across a gap.
            if (state == 0) {
                break;
            }
            long curReadPosition = READ_POSITION_UPDATER.get(this);
            long expectedNewPosition = (curReadBlockIndex + 1L) * blockSize;
            long curPos = curReadPosition;
            try {
                while (curPos < expectedNewPosition) {
                    long length = Math.min(forceSize, expectedNewPosition - curPos);
                    MemorySegment target = mappedMemorySegment.asSlice(FileMetaInfo.FILE_META_SIZE + curPos, length);
                    target.force();
                    curPos += length;
                }
            } catch (Exception e) {
                log.warn("fileName= {} ackReadPosition failed,newReadpos= {}.  ", this.fileFromOffset, expectedNewPosition,e);
                return; // 保留原位点，由下一次刷盘重试；禁止在失败位置无限自旋。
            }
            //刷盘成功更新指针
            READ_POSITION_UPDATER.set(this, expectedNewPosition);
            NEXT_READ_INDEX_UPDATER.incrementAndGet(this);
            DIRTY_UPDATER.set(this, 1);
            log.info("fileName= {} ackReadPosition successfully,newReadpos= {}", this.fileFromOffset, expectedNewPosition);
        }
    }

    /**
     * 物理和逻辑Block解耦 + 无锁账本 推进 upLoadPosition指针，该方法只是将预期结果填到坑里面，有专门的线程去检查指针
     *
     * @param logicalIndex 当前完成上传的 Block 在本文件内部的逻辑序号 (0, 1, 2...)
     */
    /**
     * 中文：调用者必须先确认S3上传成功；先写并force单块确认字节，再更新内存状态与连续上传水位线。更正上方旧描述：这些步骤在调用线程持有文件锁时执行，不是无锁或另有专门线程推进；本方法不上传数据，也不完成用户Future。
     * English: Caller must first confirm successful S3 upload. Forces the per-block acknowledgement before advancing memory state and the watermark. Correcting the historical description above, this runs on the caller thread under the file monitor, not lock-free or on a dedicated advancement thread; it neither uploads nor completes user futures.
     *
     * @param logicalIndex 中文：文件内已上传逻辑块序号；English: uploaded logical block index within this file
     * @throws IllegalArgumentException 中文：块序号越界；English: block index is out of bounds
     * @throws WalException 中文：文件已经清理；English: file is already cleaned
     */
    public synchronized void ackUpLoadPosition(int logicalIndex) {
        checkBlockIndex(logicalIndex);
        if (isCleanup()) throw new WalException("Cannot acknowledge a cleaned WAL file");
        // 必须先持久化单块确认，调用方随后才能完成用户 Future。连续水位线无法表达乱序上传。
        // 中文：该字节必须先force成功，正常调用方之后才可发布用户成功结果；只有连续水位不足以保护乱序完成块。
        // English: This byte must be forced before normal callers publish success; the contiguous watermark alone cannot protect out-of-order completed blocks.
        mappedMemorySegment.set(ValueLayout.JAVA_BYTE, UPLOADED_BLOCKS_OFFSET + logicalIndex, (byte) 1);
        mappedMemorySegment.asSlice(0, FileMetaInfo.FILE_META_SIZE).force();
        // 1. 物理填坑：利用 VarHandle 的 Volatile 语义写入，确保其他 CPU 核心立即可见
        setBlockStateArrayFinishedUpLoad(logicalIndex);
        //每个线程都去看一下是否能进行更新
        while (true) {
            if (!posActive) return;
            // 在循环外固定当前要检查的索引
            int currentIndex = NEXT_UPLOAD_INDEX_UPDATER.get(this);
            if (currentIndex >= totalBlockCount) {
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

    /** 恢复所有与位置关联的内存账本；旧格式文件按连续上传位点推断已完成块。 */
    /**
     * 中文：从头部快照与逐块确认重建所有相关位点、索引和状态；旧文件只可从连续上传位点推断已确认范围。调用者应在发布给并发使用者之前调用。
     * English: Rebuilds positions, indexes, and states from checkpoints and per-block acknowledgements; legacy files can infer only their contiguous acknowledged prefix. Call before publishing to concurrent users.
     *
     * @param persistedReadPosition 中文：持久化读位点，数据区字节；English: persisted data-area read position in bytes
     * @param persistedUploadPosition 中文：持久化连续上传位点，数据区字节；English: persisted contiguous upload position in bytes
     * @throws WalException 中文：位点不合法或不按块对齐；English: positions are invalid or not block-aligned
     */
    public synchronized void restorePositions(long persistedReadPosition, long persistedUploadPosition) {
        if (persistedReadPosition < 0 || persistedUploadPosition < 0
                || persistedReadPosition > fileSize || persistedUploadPosition > fileSize
                || persistedReadPosition % blockSize != 0 || persistedUploadPosition % blockSize != 0) {
            throw new WalException("Invalid WAL positions in " + fileName);
        }
        // 中文：上传检查点可能领先本地读检查点；恢复以已确认范围为基线，并非在这里重新执行数据force。
        // English: The upload checkpoint may lead the local read checkpoint; restoration uses the acknowledged range as a baseline without re-forcing data here.
        readPosition = Math.max(persistedReadPosition, persistedUploadPosition);
        wrotePosition = readPosition;
        upLoadPosition = persistedUploadPosition;
        readBlockIndex = (int) (readPosition / blockSize);
        nextUploadBlockIndex = (int) (upLoadPosition / blockSize);
        for (int i = 0; i < totalBlockCount; i++) {
            boolean uploaded = i < nextUploadBlockIndex
                    || mappedMemorySegment.get(ValueLayout.JAVA_BYTE, UPLOADED_BLOCKS_OFFSET + i) == 1;
            SHORT_ARRAY_HANDLE.setVolatile(blockStateArray, i,
                    uploaded ? (short) 3 : i < readBlockIndex ? (short) 1 : (short) 0);
        }
        while (nextUploadBlockIndex < totalBlockCount && isBlockUploaded(nextUploadBlockIndex)) {
            nextUploadBlockIndex++;
            upLoadPosition = (long) nextUploadBlockIndex * blockSize;
        }
        DIRTY_UPDATER.set(this, 1);
    }

    /**
     * 中文：查询内存账本中的状态3；恢复初始化负责把持久化确认导入账本，本方法本身不访问S3。
     * English: Queries state three in memory; restoration imports durable acknowledgements into that ledger. This method does not contact S3.
     *
     * @param blockIndex 中文：文件内块序号；English: block index within the file
     * @return 中文：是否被账本记录为已上传；English: whether the ledger records uploaded state
     */
    public boolean isBlockUploaded(int blockIndex) {
        checkBlockIndex(blockIndex);
        return (short) SHORT_ARRAY_HANDLE.getVolatile(blockStateArray, blockIndex) == 3;
    }

    /**
     * 中文：验证块序号在数组容量内。
     * English: Checks the block index against array capacity.
     *
     * @param blockIndex 中文：待检查序号；English: index to validate
     * @throws IllegalArgumentException 中文：序号越界；English: index is out of bounds
     */
    private void checkBlockIndex(int blockIndex) {
        if (blockIndex < 0 || blockIndex >= totalBlockCount) {
            throw new IllegalArgumentException("Invalid WAL block index: " + blockIndex);
        }
    }

    /**
     * 预热 PageCache 并且根据用户配置判断是否锁定映射内存，锁定可以确保其不会被换出到虚拟内存中，也不会被操作系统移动。
     *
     * @param pages 预热多少页后就执行一次强制刷盘
     */
    /**
     * 中文：逐页写零触发映射并分批force；只能用于允许覆盖的新/空文件。锁页只是平台相关请求，不能据此宣称一定防换出或防操作系统移动。
     * English: Writes zero per page to touch the mapping and forces in batches; use only on new/empty files safe to overwrite. Page locking is platform-dependent, not a guarantee against swapping or OS movement.
     *
     * @param pages 中文：每批force之间的页数，必须为正；English: positive number of pages between force batches
     * @param isLockMemory 中文：是否调用平台锁页帮助方法；English: whether to invoke platform page-lock support
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
    /**
     * 中文：将数据区偏移加4096后读取本机字节序int；调用者保证边界、对齐和映射生命周期。
     * English: Adds 4096 to the data-area offset and reads a native-order int; callers guarantee bounds, alignment, and mapping lifetime.
     *
     * @param fromOffset 中文：数据区字节偏移；English: data-area byte offset
     * @return 中文：读取的int；English: read int value
     */
    public int getIntFromDataArea(long fromOffset) {
        return mappedMemorySegment.get(JAVA_INT, FileMetaInfo.FILE_META_SIZE + fromOffset);
    }

    /**
     * 中文：从数据区读取本机字节序long，不用于读取文件头。
     * English: Reads a native-order long from the data area, not the file header.
     *
     * @param fromOffset 中文：数据区字节偏移，需要long对齐；English: data-area byte offset requiring long alignment
     * @return 中文：读取的long；English: read long value
     */
    public long getLongFromDataArea(long fromOffset) {
        return mappedMemorySegment.get(JAVA_LONG, FileMetaInfo.FILE_META_SIZE + fromOffset);
    }

    //从指定位置获取大小为sie的原始数据
    /**
     * 中文：复制指定数据区范围到新堆内数组；不验证记录CRC，不获取文件引用。
     * English: Copies a data-area range to a new heap array without checking record CRC or acquiring a file reference.
     *
     * @param fromOffset 中文：数据区字节偏移；English: data-area byte offset
     * @param size 中文：复制字节数；English: bytes to copy
     * @return 中文：独立堆内副本；English: independent heap copy
     */
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
    /**
     * 中文：在逻辑块锁内把空间CAS预留与Value期望计数同时登记，锁外复制记录。WAL位点计入头/对齐，metadata计数只计Value；返回结果不是boolean或最终提交ACK。
     * English: Registers CAS space reservation and expected value bytes together under the logical-block lock, then copies outside it. WAL positions include headers/alignment; metadata counts only values. Returns a result, not a boolean or final commit ACK.
     *
     * @param dataStruct 中文：记录序列化对象；null或单条过大返回拒绝状态；English: record serializer; null or oversized records return rejection status
     * @return 中文：PageCache复制结果和预留定位；English: page-cache copy result and reserved location
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
        // 中文：过大记录换新文件也写不下，提前返回以避免不断创建空文件。
        // English: An oversized record cannot fit after rotation either, so reject it before creating endless empty files.
        if (msgSize > this.blockSize) {
            return AppendMessageResult.fail(this, AppendMessageResult.AppendStatus.MESSAGE_TOO_LARGE, this.fileFromOffset);
        }
        // 2. CAS 自旋抢占 wrotePosition，为当前线程分配写入区域
        long currentPos;
        long newPos;
        long remainingInBlock;
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
                blockOffset = currentPos & (this.blockSize - 1);
                remainingInBlock = this.blockSize - blockOffset;
                BlockMetaData reservationMeta = blockMetaDataManager.getOrCreate(fileFromOffset, logicalIndex);
                // 与空闲封口、写满封口使用同一个元数据锁。复制字节不持锁，只有预留和登记是原子的。
                // 中文：封口同样持有该逻辑块锁；CAS预留与expected登记之间不能暴露给封口检查。
                // English: Sealing holds the same logical-block monitor; it must not observe the gap between reservation CAS and expected-byte registration.
                synchronized (reservationMeta) {
                    if (WROTE_POSITION_UPDATER.get(this) != currentPos) continue;
                    if (msgSize > remainingInBlock || reservationMeta.getState() != BlockMetaData.OPEN) {
                        long paddingPos = currentPos + remainingInBlock;
                        if (WROTE_POSITION_UPDATER.compareAndSet(this, currentPos, paddingPos)) {
                            sealBlock(logicalIndex);
                            if (paddingPos == this.fileSize) {
                                close();
                                return AppendMessageResult.fail(this, AppendMessageResult.AppendStatus.END_OF_FILE, this.fileFromOffset);
                            }
                        }
                        continue;
                    }
                    if (WROTE_POSITION_UPDATER.compareAndSet(this, currentPos, newPos)) {
                        // 中文：计数只增加Value字节；newPos已按完整记录含头和对齐前移，两种单位不要混用。
                        // English: Counts only value bytes; newPos already advanced by the full header/aligned record, so keep the units distinct.
                        reservationMeta.addExpectedBytes(dataStruct.getDataLen());
                        break;
                    }
                }
                // CAS失败，提示CPU这是spin等待
                Thread.onSpinWait();
            }
            int dataSize = dataStruct.getDataLen();
            //看看该文件是否超过了水位线,超过水位线触发触发 预创建文件
            if (isCreateNewFile == 0 && newPos >= manager.fileWaterMark && IS_CREATE_NEW_FILE.compareAndSet(this, 0, 1)) {
                manager.tryCreateNextFileWhenReachFileWaterMark(fileFromOffset + FileMetaInfo.FILE_META_SIZE + fileSize);
            }
            // 3. CAS 成功，当前线程独占 [currentPos, newPos) 区间，执行真正写入
            //因为文件开头4KB是元数据区域，因此真正的开头为 FILE_META_SIZE+currentPos
            // 中文：复制已经离开元数据锁，各线程只写自己预留的范围；PageCache完成仍不等于持久化或S3完成。
            // English: Copying is outside the metadata monitor and each thread owns its reserved range; page-cache completion is still not durability or S3 completion.
            result = doAppend(FileMetaInfo.FILE_META_SIZE + currentPos, msgSize, dataStruct);
            // 预留后的失败也需要精确定位，供上层完成该块的失败结算。
            result.setLogicalIndex(logicalIndex);
            result.setBlockOffset(blockOffset);
            if (result.isOk()) {
                //写入成功，增加写入到PageCache字节数
                blockMetaData = blockMetaDataManager.addPageCacheBytes(this.fileFromOffset, logicalIndex, dataSize);
                // 本次写入恰好填满当前 Block（blockOffset + msgSize == blockSize）。
                // 这种情况不会触发上面的“跨 Block 封口”逻辑，必须在这里主动封口，
                // 否则该 Block 永远停留在 OPEN 状态，数据永远不会被上传（静默丢数据）。
                if (remainingInBlock==msgSize) {
                    sealBlock(logicalIndex);
                }
            }
        } finally {
            // 中文：外层Manager仍可能持有文件引用，因此用逻辑块字节账本判断可刷盘，而不是等待整个文件引用归零。
            // English: The outer manager may still hold a file reference, so flush readiness comes from per-block bytes rather than the entire file reference count.
            if (blockMetaData != null && logicalIndex != -1 && blockMetaDataManager.isAllDataWriteInPageCache(blockMetaData)) {
                // 文件引用包含外层 Manager 和其他块，不能用引用归零判断本逻辑块是否写完。
                setBlockStateArrayFinishedPageCache(logicalIndex);
            }
            this.release();
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
    /**
     * 中文：把独占的物理文件切片交给序列化器；故意不再次检查available，避免正常轮转拒绝在途写。异常关闭写入准入并返回WRITER_FAILED，不回退预留。
     * English: Passes an exclusively reserved physical file slice to the serializer. Deliberately does not recheck availability, avoiding rejection of in-flight writes during rotation. Failure closes admission and returns WRITER_FAILED without rolling back reservation.
     *
     * @param writeOffset 中文：物理文件字节偏移，已包含4096头；English: physical byte offset already including the 4096-byte header
     * @param size 中文：含头/对齐的记录预留字节数；English: reserved record bytes including header/alignment
     * @param dataStruct 中文：负责复制的记录对象；English: record serializer
     * @return 中文：本次复制的状态；English: copy-stage status
     */
    private AppendMessageResult doAppend(final long writeOffset, final long size, final DataStruct dataStruct) {
        try {
            // 注意：这里不再检查 isAvailable()。
            // appendData 开头已检查过 isAvailable()，且当前线程通过 hold() 持有引用，
            // mmap 在 refCount 归零前不会被清理；写指针也由 CAS 保证 <= fileSize。
            // 若这里再检查，会出现“文件刚好切走导致在途线程写入失败(File is closed)”的竞态。
            // 中文：上述保护依赖管理器遵守 canClean/有序关闭；public clean 自身不检查引用数，外部强制调用仍可破坏在途映射。
            // English: That protection relies on manager canClean/ordered-shutdown discipline; public clean does not check references and a forced external call can invalidate live mappings.
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
    /**
     * 中文：标记并释放Arena/Channel，再清空映射账本；不删除文件、不检查引用数。调用方必须已排空使用者；异常记录日志，不保证标志置位后每项资源都成功释放。
     * English: Marks cleanup, releases the arena/channel, and clears mapping state. Does not delete the file or check references; callers must drain users first. Errors are logged, so a set flag does not guarantee every release succeeded.
     */
    @Override
    public synchronized void clean() {
        try {
            if (isCleanup()) {
                return;
            }
            // 中文：调用者必须已证明清理安全；本方法自身不会等待引用归零或上传结束。
            // English: The caller must already establish cleanup safety; this method itself does not await zero references or upload completion.
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

    /**
     * 中文：删除文件成功后移除管理器索引；本方法不验证上传状态、不自动clean，需先由调用方确认安全。
     * English: Removes the manager index after successful file deletion. Does not verify upload status or call clean; callers must establish safety first.
     */
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

    /**
     * 中文：仅CAS 0到1，保留已上传的3，防止迟到WAL回调倒退状态；调用方应已确认全块PageCache完成。
     * English: CASes only zero to one, preserving uploaded state three against late WAL callbacks; caller must already establish whole-block page-cache completion.
     *
     * @param blockIndex 中文：文件内逻辑块序号；English: logical block index within the file
     */
    public void setBlockStateArrayFinishedPageCache(int blockIndex) {
        checkBlockIndex(blockIndex);
        // 写完回调可能迟于上传完成，不能将 3 降回 1。
        SHORT_ARRAY_HANDLE.compareAndSet(this.blockStateArray, blockIndex, (short) 0, (short) 1);
    }

    /**
     * 中文：只更新内存状态3，不force；正常上传确认必须使用ackUpLoadPosition以持久化确认字节。
     * English: Sets memory state three only, without forcing; normal acknowledgement must use ackUpLoadPosition to persist its byte.
     *
     * @param blockIndex 中文：文件内逻辑块序号；English: logical block index within the file
     */
    public void setBlockStateArrayFinishedUpLoad(int blockIndex) {
        SHORT_ARRAY_HANDLE.setVolatile(this.blockStateArray, blockIndex, (short) 3);
    }

    //是否清除资源，true代表可以
    /**
     * 中文：判断已关闭且无引用，并且读/上传水位都覆盖预留范围；尾块位点按块推进，所以比较使用大于等于。它返回瞬时判断，不直接回收。
     * English: Checks closed admission, no references, and read/upload watermarks covering reservations. Watermarks are block-rounded, hence greater-than-or-equal comparisons. Returns a snapshot decision without reclaiming.
     *
     * @return 中文：当前是否满足删除前置条件；English: whether deletion prerequisites currently hold
     */
    public synchronized boolean canClean() {
        return !isCleanup() && getRefCount() == 0 && !isAvailable()
                && readPosition >= wrotePosition && upLoadPosition >= wrotePosition;
    }

    /**
     * 中文：统一封口及位掩码处理，通知可刷盘、可上传或需恢复；不会在这里执行S3上传或WAL force。
     * English: Centralizes sealing and result-bit handling for flush readiness, upload, or recovery; performs neither S3 upload nor WAL force here.
     *
     * @param blockIndex 中文：已有元数据的逻辑块序号；English: logical block index with existing metadata
     */
    private void sealBlock(int blockIndex) {
        BlockMetaDataManager metadata = manager.blockMetaDataManager;
        BlockMetaData block = metadata.getBlockMetaData(fileFromOffset, blockIndex);
        if (block == null) return;
        synchronized (block) {
            int result = metadata.trySeal(fileFromOffset, blockIndex, block);
            if ((result & 1) != 0 || metadata.isAllDataWriteInPageCache(block)) {
                setBlockStateArrayFinishedPageCache(blockIndex);
            }
            if ((result & 2) != 0) metadata.setTaskToUpdateQueue(fileFromOffset, blockIndex);
            if ((result & 4) != 0) metadata.setTaskToRecoverQueue(block, fileFromOffset, blockIndex);
        }
    }

    /** 关闭时由停止写入后的管理者调用，封口同时完成刷盘/上传/恢复通知。 */
    /**
     * 中文：遍历所有已有逻辑块元数据执行完整封口通知；主要由禁止写入并排空后的关闭流程调用。
     * English: Seals existing logical-block metadata with full notifications, primarily after close admission and write draining.
     */
    public void sealAllBlocks() {
        for (int i = 0; i < totalBlockCount; i++) sealBlock(i);
    }

    /**
     * 中文：返回文件名，不获取引用。
     * English: Returns the filename without acquiring a reference.
     *
     * @return 中文：文件名；English: filename
     */
    @Override
    public String getFileName() {
        return this.fileName;
    }

    /**
     * 中文：返回逻辑块容量。
     * English: Returns logical block capacity.
     *
     * @return 中文：块字节数；English: block bytes
     */
    public int getBlockSize() {
        return blockSize;
    }

    /**
     * 中文：借用底层文件通道，清理后可能为空；不要独立关闭。
     * English: Borrows the underlying channel, possibly null after cleanup; do not close independently.
     *
     * @return 中文：底层通道；English: underlying channel
     */
    @Override
    public FileChannel getFileChannel() {
        return this.fileChannel;
    }

    /**
     * 中文：返回一个逻辑块的借用映射切片，包含WAL记录头；不自动持有文件引用。
     * English: Returns a borrowed logical-block slice including WAL record headers, without acquiring a file reference.
     *
     * @param blockIndex 中文：文件内逻辑块序号；English: logical block index within the file
     * @return 中文：依附文件Arena的可写切片；English: writable slice tied to the file arena
     */
    public MemorySegment getBlockMappedMemorySegmentSlice(int blockIndex) {
        long fromOffset = FileMetaInfo.FILE_META_SIZE + (long) blockIndex * blockSize;
        return mappedMemorySegment.asSlice(fromOffset, blockSize);
    }

    /**
     * 中文：按物理文件偏移获取借用切片，可访问文件头；与数据区偏移API区别使用，生命周期由调用方协调。
     * English: Obtains a borrowed slice by physical file offset, including possible header access. Distinguish this from data-area-offset APIs and coordinate lifetime externally.
     *
     * @param fromOffset 中文：物理文件字节偏移；English: physical file byte offset
     * @param size 中文：切片字节数；English: slice size in bytes
     * @return 中文：依附文件Arena的切片；English: slice tied to the file arena
     */
    public MemorySegment getMappedMemorySegmentSlice(long fromOffset, long size) {
        return mappedMemorySegment.asSlice(fromOffset, size);
    }

    //返回true则代表还会继续更新upLoad指针
    /**
     * 中文：判断连续上传索引是否尚未到文件块总数，不表示必然还有有效记录或正在上传。
     * English: Checks whether the upload index is short of total block count; it does not prove valid records remain or an upload is running.
     *
     * @return 中文：索引尚未到总块数时为true；English: true when the index has not reached total blocks
     */
    public boolean isContinueUpdateUpLoadPosition() {
        return NEXT_UPLOAD_INDEX_UPDATER.get(this) != totalBlockCount;
    }

    /**
     * 中文：返回所属Bucket文件管理器。
     * English: Returns the owning bucket file manager.
     *
     * @return 中文：所属管理器；English: owning manager
     */
    public MappedFileManager getManager() {
        return manager;
    }
}


