package org.foreverfzl.cloudcache.wal.manager;

import org.foreverfzl.cloudcache.metadata.manager.BlockMetaDataManager;
import org.foreverfzl.cloudcache.wal.Util.BucketMetaInfoUtil;
import org.foreverfzl.cloudcache.wal.Util.FileMetaInfoUtil;
import org.foreverfzl.cloudcache.wal.datastruct.BucketMetaInfo;
import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.FileMetaInfo;
import org.foreverfzl.cloudcache.wal.storefile.AppendMessageResult;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudcache.wal.storefile.MappedFiledReferenceResource;
import org.foreverfzl.cloudchache.common.LogName;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.exception.WalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;


/**
 * 这个类专门管理该Bucket存在的文件
 */
/**
 * 中文：管理单个Bucket的活跃/历史WAL、序号文件和三个维护线程；不负责S3上传或用户Future完成。上层先停止业务准入并排空写入/恢复/上传，再执行最终刷盘和资源关闭。
 * English: Manages one bucket's active/historical WAL, sequence file, and three maintenance threads; it does not upload to S3 or complete user futures. The upper layer closes admission and drains writes/recovery/uploads before final forcing and resource shutdown.
 */
public class MappedFileManager {
    /**
     * 中文：Bucket文件管理诊断日志。
     * English: Bucket file-management diagnostics.
     */
    private static final Logger log = LoggerFactory.getLogger(LogName.MAPPED_FILE_MANAGER);
    /**
     * 中文：管理器所属实例名称。
     * English: Owning instance name.
     */
    public final String instanceName;
    /**
     * 中文：管理器对应的S3 Bucket名称。
     * English: S3 bucket associated with this manager.
     */
    public final String bucketName;

    // 3. 【核心骨架】：并发跳表。Key 是文件的起始 Offset，天生按位点升序排列
    /**
     * 中文：按文件身份排序的已注册映射；不等于磁盘目录全部文件的快照。
     * English: Registered mappings ordered by file identity, not a snapshot of every file on disk.
     */
    private final ConcurrentSkipListMap<Long, DefaultMappedFile> mappedFiles = new ConcurrentSkipListMap<>();

    //检查所有的文件，控制文件是否要删除，把globalUpLoadPosition指针之前的文件全部删除，因为前面的文件已经上传到服务了
    /**
     * 中文：清理检查周期，毫秒。
     * English: Cleanup-check interval in milliseconds.
     */
    private final int chackMappedFileTime;
    /**
     * 中文：检查canClean后执行元数据删除、unmap和路径删除的维护线程。
     * English: Maintenance thread that checks canClean before removing metadata, unmapping, and deleting paths.
     */
    private final Thread chackMappedFileThread;
    /**
     * 中文：清理线程循环开关；关闭还需要interrupt及join。
     * English: Cleanup loop flag; shutdown also requires interrupt and join.
     */
    private volatile boolean chackMappedFileThreadState = true;
    //当前活跃文件的最后一个文件
    /**
     * 中文：当前写入目标的原子引用；轮转CAS后旧文件仍可留在mappedFiles供刷盘/恢复。
     * English: Atomic current write target; rotated files may remain registered for flushing/recovery.
     */
    private final AtomicReference<DefaultMappedFile> activeMappedFile = new AtomicReference<>();

    //该bucket的wal目录绝对地址
    /**
     * 中文：Bucket目录，内部同时保存bucketMeta、next-file-offset和wal子目录。
     * English: Bucket directory containing bucketMeta, next-file-offset, and wal.
     */
    private final String dirPath;
    //Bucket级别配置文件
    /**
     * 中文：Bucket配置引用；上层负责使用已校验快照，运行期间不要修改。
     * English: Bucket configuration reference; the upper layer provides a validated snapshot that should not mutate during use.
     */
    public final BucketConfig config;
    //WAL持久化文件地址
    /**
     * 中文：实际WAL数据文件目录。
     * English: Directory containing WAL data files.
     */
    private final String WAL_FILE_PATH;
    /**
     * 中文：Bucket级编号高水位文件路径，与具体WAL生命周期分离。
     * English: Bucket-level sequence high-watermark path, independent of individual WAL lifetimes.
     */
    private final Path nextFileOffsetPath;
    /**
     * 中文：已force的下一文件编号，更新由reserveNextFileOffset锁保护；允许跳号，不应回退复用。
     * English: Next file identity already forced to disk, updated under reserveNextFileOffset's lock; gaps are allowed, reuse is not.
     */
    private long reservedNextFileOffset;

    //文件水位线，活跃文件超过这个水位线会分配新的线程去创建新的文件
    /**
     * 中文：触发预创建的数据区字节水位，构造时取配置文件容量的70%。
     * English: Data-area byte watermark for precreation, set to 70% of configured file capacity.
     */
    public final long fileWaterMark;

    //到达水位线 创建新文件的时候都会使用这个线程池
    /**
     * 中文：单线程预创建执行器，由本管理器拥有，必须在卸载映射前停止。
     * English: Owned single-thread precreation executor that must stop before mappings are released.
     */
    private final ExecutorService createNewFileExecutor = Executors.newSingleThreadExecutor();

    //该bucket对应的Block元数据管理者
    /**
     * 中文：与Core共享的逻辑块Value字节账本及任务队列，不是文件头的持久化对象。
     * English: Logical-block value-byte ledger/task queues shared with Core, not a persisted file-header object.
     */
    public BlockMetaDataManager blockMetaDataManager;

    /**
     * 中文：文件头检查点刷新周期，毫秒。
     * English: File-header checkpoint flush interval in milliseconds.
     */
    private final int flushFileMetaTime;
    //刷新文件元数据的线程，将文件的各个信息写入到对应文件的开头4KB
    /**
     * 中文：刷新dirty文件头位置快照的维护线程。
     * English: Maintenance thread flushing dirty file-header position snapshots.
     */
    private final Thread fileMetaFlushThread;
    /**
     * 中文：元数据刷新循环开关。
     * English: Metadata-flush loop flag.
     */
    private volatile boolean fileMetaFlushThreadState = true;

    //刷新所有文件的读指针，默认5S
    /**
     * 中文：数据块force检查周期，5000毫秒。
     * English: Data-block force-check interval of 5000 milliseconds.
     */
    private final int flushReadPositionTime = 5000;
    /**
     * 中文：对完整PageCache块force并推进读位置的线程。
     * English: Thread forcing complete page-cache blocks and advancing read positions.
     */
    private final Thread flushFileReadPositionThread;
    /**
     * 中文：数据force循环开关。
     * English: Data-force loop flag.
     */
    private volatile boolean flushFileReadPositionThreadState = true;

    //bucket元数据mmp
    /**
     * 中文：本管理器拥有的Bucket元数据映射生命周期。
     * English: Bucket-metadata mapping lifetime owned by this manager.
     */
    private Arena bucketMetaFileArena;
    /**
     * 中文：bucketMeta的借用返回源；Arena关闭后所有使用都无效。
     * English: Source mapping exposed for borrowed bucketMeta access; all uses expire when its arena closes.
     */
    private MemorySegment bucketMetaFileSegment;


    //该bucket下有多少线程正在写入
    /**
     * 中文：只统计本管理器appendData调用，不覆盖随后物理Block写入/上传，不能单独代表整个业务已排空。
     * English: Counts only this manager's appendData calls, not subsequent physical-block writes/uploads; it cannot alone prove business-wide draining.
     */
    private final AtomicLong walWriteCount = new AtomicLong(0L);


    /**
     * 中文：读取编号高水位并创建/映射Bucket元数据，先创建活跃文件再启动维护线程。该构造器有磁盘及线程副作用。
     * English: Reads the sequence watermark, creates/maps bucket metadata, creates the active file, then starts maintenance threads. Construction has disk and thread side effects.
     *
     * @param dirPath 中文：Bucket目录；English: bucket directory
     * @param instanceName 中文：实例名称；English: instance name
     * @param bucketName 中文：Bucket名称；English: bucket name
     * @param config 中文：已验证的Bucket配置；English: validated bucket configuration
     * @param fromOffset 中文：请求起点，与持久化下一编号取最大值；English: requested start, maximized against persisted next identity
     */
    public MappedFileManager(String dirPath, String instanceName, String bucketName, BucketConfig config, long fromOffset) {
        this.instanceName = instanceName;
        this.bucketName = bucketName;
        this.dirPath = dirPath;
        this.config = config;
        this.WAL_FILE_PATH = dirPath + File.separator + "wal";
        this.nextFileOffsetPath = Path.of(dirPath, "next-file-offset");
        this.reservedNextFileOffset = readNextFileOffset();
        this.fileWaterMark = (long) (config.walFileSize * 0.7);
        this.blockMetaDataManager = new BlockMetaDataManager();
        this.bucketMetaFileArena = Arena.ofShared();
        bucketMetaFileSegment = BucketMetaInfoUtil.createAndMapBucketMetaFile(new BucketMetaInfo(config.blockSize, config.walFileSize, config.s3KeyPrefix), Path.of(dirPath), bucketMetaFileArena);//保存该bucket的元数据
        this.chackMappedFileTime = config.chackMappedFileTime;
        this.chackMappedFileThread = new Thread(this::chackMappedFileTask);
        this.flushFileMetaTime = config.flushFileMetaInfoTime;
        this.fileMetaFlushThread = new Thread(this::flushFileMeta);
        this.flushFileReadPositionThread = new Thread(this::flushReadPositionTask);
        init(Math.max(fromOffset, reservedNextFileOffset));
    }


    //启动线程，创建初始文件等
    /**
     * 中文：先成功创建文件，再启动三个维护线程，避免文件创建失败后仍有维护线程运行。
     * English: Creates the file successfully before starting three maintenance threads, avoiding live maintenance workers after file-creation failure.
     *
     * @param fromOffset 中文：首个活跃文件编号；English: first active file identity
     */
    private void init(long fromOffset) {
        //刚开始的时候一个文件也没有，因此我们必须初始化一个文件
        DefaultMappedFile startFile = synCreateMappedFile(fromOffset);
        activeMappedFile.compareAndSet(null, startFile);
        chackMappedFileThread.start();
        fileMetaFlushThread.start();
        flushFileReadPositionThread.start();
    }


    /**
     * 中文：持有每次选中文件的引用并尝试追加；只有文件关闭/结尾才轮转重试，参数或复制失败直接返回。finally归还同一个文件引用并减少WAL计数。
     * English: Holds each selected file while appending; retries through rotation only for closed/end-of-file status, returning argument/copy failures directly. Finally releases the same file and decrements the WAL count.
     *
     * @param dataStruct 中文：待追加记录；English: record to append
     * @return 中文：WAL复制阶段结果，不是远端提交ACK；English: WAL-copy result, not a remote commit acknowledgement
     */
    public AppendMessageResult appendData(final DataStruct dataStruct) {
        AppendMessageResult result = null;
        walWriteCount.incrementAndGet();
        try {
            while (true) {
                DefaultMappedFile oldMappedFile = activeMappedFile.get();
                oldMappedFile.hold();
                try {
                    // 先去目前活跃的文件中添加数据
                    result = oldMappedFile.appendData(dataStruct);
                } finally {
                    oldMappedFile.release();
                }
                AppendMessageResult.AppendStatus status = result.getStatus();
                // 写成功，结束循环
                if (status == AppendMessageResult.AppendStatus.PUT_OK) {
                    break;
                }
                if (status != AppendMessageResult.AppendStatus.END_OF_FILE && status != AppendMessageResult.AppendStatus.FILE_CLOSED) {
                    //除了OK、END、CLOSE其它都直接返回
                    break;
                }
                // 文件结尾，创建新的文件
                long nextFileOffset = oldMappedFile.fileFromOffset + FileMetaInfo.FILE_META_SIZE + oldMappedFile.fileSize;
                DefaultMappedFile newFile = synCreateMappedFile(nextFileOffset);
                if (newFile != null) {
                    activeMappedFile.compareAndSet(oldMappedFile, newFile);
                } else {
                    break;
                }
            }
        } finally {
            walWriteCount.decrementAndGet();
        }
        return result;
    }


// 中文：下方保留的是历史实现注释，不参与编译；真实轮转/引用归还行为以上方活动实现为准。
// English: The retained implementation below is historical commented-out code; the active implementation above defines rotation/reference behavior.
//    public AppendMessageResult appendData(final DataStruct dataStruct) {
//        AppendMessageResult result = null;
//        walWriteCount.incrementAndGet();
//        DefaultMappedFile oldMappedFile = null;
//        try {
//            oldMappedFile = activeMappedFile.get();
//            oldMappedFile.hold();
//            try {
//                //先去目前活跃的文件中添加数据
//                result = oldMappedFile.appendData(dataStruct);
//            } finally {
//                oldMappedFile.release();
//            }
//            //如果是文件结尾或者关闭，则创建新的文件进行写
//            AppendMessageResult.AppendStatus status = result.getStatus();
//            if (status == AppendMessageResult.AppendStatus.END_OF_FILE
//                    || status == AppendMessageResult.AppendStatus.FILE_CLOSED) {
//                //获取最新的写文件
//                long nextFileOffset = oldMappedFile.fileFromOffset + oldMappedFile.fileSize;
//                DefaultMappedFile newFile = synCreateMappedFile(nextFileOffset);
//                if (newFile != null) {
//                    activeMappedFile.compareAndSet(oldMappedFile, newFile);
//                    oldMappedFile = activeMappedFile.get();
//                    oldMappedFile.hold();
//                    try {
//                        //然后使用新的文件进行写
//                        result = oldMappedFile.appendData(dataStruct);
//                    } finally {
//                        oldMappedFile.release();
//                    }
//                }
//            }
//        } finally {
//            walWriteCount.decrementAndGet();
//        }
//        return result;
//    }


    /**
     * 当到达水位线70%创建一个:创建新文件的CompletableFuture任务，当DefaultMappedFile文件检测到水位线就会触发这个方法
     */
    /**
     * 中文：向单线程执行器提交预创建工作；更正旧描述：这里用execute提交Runnable，没有返回CompletableFuture。任务异常不作为当前方法返回值。
     * English: Submits precreation to the single-thread executor. Clarifies the historical description: execute submits a Runnable and returns no CompletableFuture; task failures are not returned by this method.
     *
     * @param nextFileFromOffset 中文：下一文件身份编号；English: next file identity
     * @throws java.util.concurrent.RejectedExecutionException 中文：预创建执行器已经关闭；English: precreation executor is shut down
     */
    public void tryCreateNextFileWhenReachFileWaterMark(long nextFileFromOffset) {
        createNewFileExecutor.execute(() -> {
            synCreateMappedFile(nextFileFromOffset);
        });
    }


    /**
     * 同步创建新的文件并放入到容器中（默认使用配置文件 创建文件）
     */
    /**
     * 中文：按当前Bucket配置同步获取或创建映射。
     * English: Synchronously gets or creates a mapping with current bucket settings.
     *
     * @param fileFromOffset 中文：文件身份编号；English: file identity
     * @return 中文：已有或新映射；English: existing or new mapping
     */
    public DefaultMappedFile synCreateMappedFile(long fileFromOffset) {
        return synCreateMappedFile(fileFromOffset, config.walFileSize, config.blockSize, config.isWarmWalFile, config.isLockMappedFilePageCache);
    }


    //自定义创建文件
    /**
     * 中文：按文件名intern锁二次检查；先持久化下一编号再创建映射并注册。这里只协调本实现的创建，不等于跨进程文件锁。
     * English: Double-checks under an interned-filename lock, persists the next identity before creating/registering a mapping. This coordinates this implementation, not multiple processes.
     *
     * @param fileFromOffset 中文：文件身份编号；English: file identity
     * @param walFileSize 中文：数据区字节数；English: data-area bytes
     * @param blockSize 中文：逻辑块字节数；English: logical block bytes
     * @param isWarm 中文：是否预热新文件；English: whether to warm new files
     * @param isLock 中文：是否请求锁页；English: whether to request page locking
     * @return 中文：索引中的映射对象；English: mapping registered in the index
     */
    public DefaultMappedFile synCreateMappedFile(long fileFromOffset, long walFileSize, int blockSize, boolean isWarm, boolean isLock) {
        String fileName = String.valueOf(fileFromOffset);
        DefaultMappedFile defaultMappedFile = mappedFiles.get(fileFromOffset);
        if (defaultMappedFile != null) {
            return defaultMappedFile;
        }
        //这里水位线线程 和 其它线程可能出现冲突，同时创建文件，需要加锁
        synchronized (fileName.intern()) {
            try {
                //先去看看新文件是否已经创建好了
                defaultMappedFile = mappedFiles.get(fileFromOffset);
                if (defaultMappedFile != null) {
                    return defaultMappedFile;
                }
                // 先持久化编号预留，再创建文件。即使随后崩溃，也只会跳号而不会重用已上传对象的 Key。
                // 中文：编号预留成功后才创建文件；创建失败可留下编号空隙，但不应回退覆盖历史对象身份。
                // English: Create only after reserving the next identity; creation failure may leave gaps but must not roll back to reuse historical object identities.
                reserveNextFileOffset(Math.addExact(fileFromOffset, Math.addExact(FileMetaInfo.FILE_META_SIZE, walFileSize)));
                DefaultMappedFile newFile = DefaultMappedFile.createFile(WAL_FILE_PATH, fileName, fileFromOffset, walFileSize,
                        blockSize, isWarm, isLock, this);
                mappedFiles.put(fileFromOffset, newFile);
                return newFile;
            } catch (Exception e) {
                log.warn("Exception is{} . synCreateMappedFile failed, instance={},bucket={},fileName={}",
                        e, instanceName, bucketName, fileName);
                throw e;
            }
        }
    }

    /**
     * 中文：读取16字节大端序编号文件：8字节下一编号和8字节存放CRC32值。缺失时返回0；存在但损坏时拒绝启动，不静默回退。
     * English: Reads the 16-byte big-endian sequence file: eight-byte next identity and an eight-byte CRC32 value. Returns zero only when absent; corruption fails startup rather than silently falling back.
     *
     * @return 中文：已持久化下一编号或缺失时的0；English: persisted next identity, or zero if absent
     * @throws WalException 中文：长度、CRC、编号或I/O无效；English: invalid length, CRC, identity, or I/O
     */
    private long readNextFileOffset() {
        // 中文：首次升级若旧WAL和编号文件都已不存在，无法推断历史编号；此处没有自动迁移或S3查询。
        // English: If legacy WAL and the sequence file are both absent at upgrade, historical identities cannot be inferred; this performs no migration or S3 lookup.
        if (!Files.exists(nextFileOffsetPath)) return 0;
        try (FileChannel channel = FileChannel.open(nextFileOffsetPath, StandardOpenOption.READ)) {
            if (channel.size() != 16) throw new WalException("Invalid WAL sequence file: " + nextFileOffsetPath);
            ByteBuffer bytes = ByteBuffer.allocate(16);
            while (bytes.hasRemaining()) {
                if (channel.read(bytes) < 0) throw new WalException("Truncated WAL sequence file: " + nextFileOffsetPath);
            }
            bytes.flip();
            long nextOffset = bytes.getLong();
            long expectedCrc = bytes.getLong();
            CRC32 crc = new CRC32();
            crc.update(bytes.array(), 0, Long.BYTES);
            if (nextOffset < 0 || expectedCrc != crc.getValue()) {
                throw new WalException("Corrupt WAL sequence file: " + nextFileOffsetPath);
            }
            return nextOffset;
        } catch (java.io.IOException e) {
            throw new WalException("Cannot read WAL sequence file: " + nextFileOffsetPath, e);
        }
    }

    /**
     * 中文：只增加编号高水位，原位写16字节并force后发布内存值；不是原子替换/双槽事务，撕裂写靠CRC使后续读取拒绝继续。先预留后创建允许跳号。
     * English: Only advances the watermark, overwriting 16 bytes in place and forcing before publishing the memory value. This is not atomic replacement or a two-slot transaction; CRC makes torn writes fail later reads. Reserve-before-create permits gaps.
     *
     * @param nextOffset 中文：需要保证不再复用的下一编号；English: next identity that must not be reused
     * @throws WalException 中文：编号文件不能持久化；English: sequence file cannot be persisted
     */
    private synchronized void reserveNextFileOffset(long nextOffset) {
        if (nextOffset <= reservedNextFileOffset) return;
        ByteBuffer bytes = ByteBuffer.allocate(16);
        bytes.putLong(nextOffset);
        CRC32 crc = new CRC32();
        crc.update(bytes.array(), 0, Long.BYTES);
        bytes.putLong(crc.getValue()).flip();
        try {
            Files.createDirectories(nextFileOffsetPath.getParent());
            try (FileChannel channel = FileChannel.open(nextFileOffsetPath,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                while (bytes.hasRemaining()) channel.write(bytes);
                channel.truncate(16);
                // 中文：这是编号文件原位更新后的force，不是原子rename；校验失败时选择拒绝启动而非复用0。
                // English: This forces an in-place sequence update, not an atomic rename; validation failure rejects startup rather than reusing zero.
                channel.force(true);
            }
            reservedNextFileOffset = nextOffset;
        } catch (java.io.IOException e) {
            // 不允许以默认编号继续写，否则可能覆盖历史 S3 对象。
            throw new WalException("Cannot persist WAL sequence file: " + nextFileOffsetPath, e);
        }
    }


    /**
     * 中文：循环执行数据刷盘检查并休眠；中断或外层异常会结束该维护线程。
     * English: Loops through data-force checks and sleeping; interruption or an outer exception ends this maintenance thread.
     */
    private void flushReadPositionTask() {
        while (flushFileReadPositionThreadState) {
            try {
                flushReadPositionTaskExtracted();
                Thread.sleep(flushReadPositionTime);
            } catch (Exception e) {
                flushFileReadPositionThreadState = false;
                break;
            }
        }
    }

    /**
     * 中文：执行一轮文件force检查，跳过已清理映射；异常记录后返回，供后台或关闭流程复用。
     * English: Runs one file-force pass, skipping cleaned mappings; logs errors and returns for reuse by background or shutdown paths.
     */
    private void flushReadPositionTaskExtracted() {
        try {
            Collection<DefaultMappedFile> values = mappedFiles.values();
            if (!values.isEmpty()) {
                for (DefaultMappedFile file : values) {
                    if (file.isCleanup()) continue;
                    file.ackReadPosition();
                }
            }
        } catch (Exception e) {
            log.warn("flushReadPositionTask failed", e);
        }
    }


    //检查WAL文件的生命周期，
    /**
     * 中文：定期运行清理检查，中断结束循环。
     * English: Periodically runs cleanup checks until interrupted.
     */
    private void chackMappedFileTask() {
        while (chackMappedFileThreadState) {
            try {
                chackMappedFileTaskExtracted();
                Thread.sleep(chackMappedFileTime);
            } catch (InterruptedException e) {
                chackMappedFileThreadState = false;
                break;
            }
        }
    }

    /**
     * 中文：逐文件检查canClean，再移除逻辑元数据、释放映射、删除路径；并非仅比较某个全局上传位点。
     * English: Checks canClean per file, then removes logical metadata, releases mappings, and deletes paths; it does not merely compare a global upload position.
     */
    private void chackMappedFileTaskExtracted() {
        try {
            Collection<DefaultMappedFile> values = mappedFiles.values();
            if (!values.isEmpty()) {
                for (DefaultMappedFile file : values) {
                    // 中文：先验证上传/读位点覆盖预留范围和引用条件；不允许仅凭read等于upload就删除。
                    // English: First require read/upload coverage of reservations and the reference conditions; equality of read and upload alone is insufficient.
                    if (file.canClean()) {
                        long fileFromOffset = file.fileFromOffset;
                        //删除该文件对应的所有元数据
                        blockMetaDataManager.deleteFileAllBlockMetaData(fileFromOffset);
                        //清除资源
                        file.clean();
                        //删除文件
                        file.delete();
                    }
                }
            }
        } catch (Exception e) {
            log.warn("chackMappedFileTask failed", e);
        }
    }

    //刷新文件元数据区域(刷新的都是需要改变的数据)
    /**
     * 中文：按配置周期刷新dirty头部，异常或中断结束外层循环。
     * English: Flushes dirty headers at the configured interval; outer exceptions or interruption end the loop.
     */
    private void flushFileMeta() {
        while (fileMetaFlushThreadState) {
            try {
                flushFileMetaExtracted();
                Thread.sleep(flushFileMetaTime);
            } catch (Exception e) {
                fileMetaFlushThreadState = false;
                break;
            }
        }
    }

    /**
     * 中文：只刷新dirty文件头；真正force和dirty清除由FileMetaInfoUtil在文件锁下完成。
     * English: Flushes only dirty headers; FileMetaInfoUtil performs force and dirty clearing under the file monitor.
     */
    private void flushFileMetaExtracted() {
        for (Map.Entry<Long, DefaultMappedFile> entry : mappedFiles.entrySet()) {
            DefaultMappedFile mappedFile = entry.getValue();
            if (DefaultMappedFile.DIRTY_UPDATER.get(mappedFile) == 0) {
                //如果不是脏数据则直接跳过
                continue;
            }
            //刷新元数据
            FileMetaInfoUtil.flushFileMetaInfo(mappedFile);
        }
    }

    /**
     * 中文：注册外部打开的恢复文件，不切换活跃写文件；同编号会替换原映射引用，调用方需保证唯一性。
     * English: Registers an externally opened recovery file without changing the active writer. The same identity replaces the prior reference, so callers must ensure uniqueness.
     *
     * @param mappedFile 中文：已初始化的恢复映射；English: initialized recovery mapping
     */
    public void addMappedFile(DefaultMappedFile mappedFile) {
        mappedFiles.put(mappedFile.fileFromOffset, mappedFile);
    }

    /**
     * 中文：按编号移除索引，不执行clean或delete。
     * English: Removes an index entry by identity without cleaning or deleting.
     *
     * @param fileFromOffset 中文：文件身份编号；English: file identity
     */
    public void removeMappedFile(long fileFromOffset) {
        mappedFiles.remove(fileFromOffset);
    }

    /**
     * 中文：只查找映射，不自动获取引用。
     * English: Looks up a mapping without acquiring a reference.
     *
     * @param fileFromOffset 中文：文件身份编号；English: file identity
     * @return 中文：借用映射，未找到时null；English: borrowed mapping, or null if absent
     */
    public DefaultMappedFile getMappedFile(long fileFromOffset) {
        return mappedFiles.get(fileFromOffset);
    }

    /**
     * 中文：暴露活跃引用本体供内部协调；不转移文件生命周期所有权。
     * English: Exposes the active reference for internal coordination without transferring file-lifecycle ownership.
     *
     * @return 中文：当前活跃引用对象；English: active reference object
     */
    public AtomicReference<DefaultMappedFile> getActiveMappedFile() {
        return activeMappedFile;
    }

    //只有项目关闭时才会调用
    /**
     * 中文：关闭所有已注册文件的写入准入，不force、不unmap、不删除。
     * English: Closes write admission on registered files without forcing, unmapping, or deleting.
     */
    public void closeAllFile() {
        //关闭文件
        mappedFiles.values().forEach((MappedFiledReferenceResource::close));
    }

    /**
     * 中文：对全部文件执行完整封口通知，供上层在业务写入排空后使用。
     * English: Runs complete sealing notifications for all files after the upper layer drains business writes.
     */
    public void sealAllBlocks() {
        mappedFiles.values().forEach(DefaultMappedFile::sealAllBlocks);
    }

    /**
     * 中文：关闭read/upload循环推进开关；不阻止预留写指针或ackUpload最先写入确认字节，不能替代业务排空。
     * English: Disables read/upload advancement loops; it does not block reservations or ackUpload's initial acknowledgement write, so it cannot replace draining.
     */
    public void stopUpdateAllFilePosition() {
        mappedFiles.values().forEach((defaultMappedFile -> {
            defaultMappedFile.posActive = false;
        }));
    }


    //让线程执行最后一次
    /**
     * 中文：同步执行最后一轮数据force检查；调用方应先停止并join后台线程，避免并行维护。
     * English: Synchronously runs a final data-force pass; stop and join background threads first to avoid concurrent maintenance.
     */
    public void endFlushFileReadPosition() {
        flushReadPositionTaskExtracted();
    }

    /**
     * 中文：同步刷新dirty文件检查点；不隐含先force业务数据。
     * English: Synchronously flushes dirty checkpoints without implicitly forcing business data first.
     */
    public void endMetaFlush() {
        flushFileMetaExtracted();
    }

    /**
     * 中文：同步执行一轮安全清理检查；只有满足canClean的文件才会删除。
     * English: Synchronously runs a cleanup pass; only files satisfying canClean are deleted.
     */
    public void endChackMappedFile() {
        chackMappedFileTaskExtracted();
    }


    /**
     * 仅关闭后台线程、线程池及堆外内存资源
     */
    /**
     * 中文：依次停止维护线程、等待预创建线程池退出、释放WAL和Bucket映射；本方法不执行最终业务排空/刷盘，须由上层先完成。
     * English: Stops maintenance workers, awaits precreation termination, and releases WAL/bucket mappings. Final business draining/forcing is the caller's prerequisite, not performed here.
     */
    public void close() {
        log.info("Closing MappedFileManager resources for instance: {}, bucket: {}", instanceName, bucketName);

        stopAllThread();
        // 先确保预创建任务退出，避免清理完成后又向 mappedFiles 添加映射。
        // 中文：预创建任务可能向索引新增映射，必须确认它退出后再清空所有映射。
        // English: Precreation can add mappings to the index, so confirm its termination before clearing mappings.
        closeThreadPool();

        // 2. 释放跳表及 activeMappedFile 中的 DefaultMappedFile 内存引用与句柄
        closeMappedFiles();

        // 3. 卸载 Bucket 级 FFM (Foreign Function & Memory) 堆外内存区域
        closeBucketMetaArena();

        log.info("MappedFileManager resources closed successfully for bucket: {}", bucketName);
    }


    /**
     * 第二步：关闭并清理所有 DefaultMappedFile 的内存与句柄
     */
    /**
     * 中文：释放并清空已注册映射，不删除磁盘WAL；应在所有文件使用者结束后调用。
     * English: Releases/clears registered mappings without deleting WAL on disk; call only after all file users finish.
     */
    private void closeMappedFiles() {
        for (DefaultMappedFile mappedFile : mappedFiles.values()) {
            if (mappedFile != null) {
                try {
                    mappedFile.clean();
                } catch (Exception e) {
                    log.error("Failed to close MappedFile in bucket: {}", bucketName, e);
                }
            }
        }
        activeMappedFile.set(null);
        mappedFiles.clear();
    }

    /**
     * 第三步：释放 JDK 21+ FFM Arena 堆外物理内存
     */
    /**
     * 中文：关闭Bucket元数据Arena并清空引用，借用segment随之失效；错误记录日志。
     * English: Closes the bucket-metadata arena and clears references, invalidating borrowed segments; errors are logged.
     */
    private void closeBucketMetaArena() {
        if (bucketMetaFileArena != null) {
            try {
                bucketMetaFileArena.close(); // 触发底层的 unmap，立即释放物理内存映射
                bucketMetaFileArena = null;
                bucketMetaFileSegment = null;
            } catch (Exception e) {
                log.error("Failed to close bucketMetaFileArena for bucket: {}", bucketName, e);
            }
        }
    }

    /**
     * 第四步：关闭文件创建线程池
     */
    /**
     * 中文：先优雅等待30秒，再中断并等待30秒；仍未结束或等待被中断则抛错，避免继续卸载可能被任务使用的映射。
     * English: Waits gracefully for thirty seconds, then interrupts and waits another thirty; throws on nontermination/interruption to avoid unmapping resources still in use.
     *
     * @throws WalException 中文：预创建任务无法安全停止；English: precreation tasks cannot be stopped safely
     */
    private void closeThreadPool() {
        createNewFileExecutor.shutdown();
        try {
            if (!createNewFileExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                createNewFileExecutor.shutdownNow();
                if (!createNewFileExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    throw new WalException("WAL creation task did not stop for " + bucketName);
                }
            }
        } catch (InterruptedException e) {
            createNewFileExecutor.shutdownNow();
            Thread.currentThread().interrupt();
            throw new WalException("Interrupted while stopping WAL creation for " + bucketName, e);
        }
    }

    //关闭的时候会触发
    /**
     * 中文：旧的WAL阶段等待辅助方法：每500毫秒检查一次计数，超时仅记录并返回，也会吞掉等待异常；不能证明整个WAL到Block链路已完成。
     * English: Legacy WAL-stage wait helper: polls every 500ms, logs and returns on timeout, and swallows wait exceptions. It cannot prove completion of the entire WAL-to-block path.
     *
     * @param deadline 中文：绝对Unix纪元毫秒截止时间；English: absolute epoch-millisecond deadline
     */
    public void waitWriterFinished(long deadline) {
        //1：等待所有的线程写入完成
        while (walWriteCount.get() != 0) {
            try {
                if (System.currentTimeMillis() >= deadline) {
                    //超时退出
                    log.warn("{} close timeout, force shutdown", bucketName);
                    return;
                }
                Thread.sleep(500);
            } catch (Exception e) {
            }
        }
    }

    /**
     * 中文：设置三个循环停止标记并中断，各自最多join10秒；不关闭预创建线程池或映射。重复调用可用于分阶段关闭。
     * English: Stops/interrupts three loops and joins each for up to ten seconds, without closing precreation or mappings. Repeated calls support staged shutdown.
     *
     * @throws WalException 中文：线程仍存活或等待被中断；English: a worker remains alive or waiting is interrupted
     */
    public void stopAllThread() {
        chackMappedFileThreadState = false;
        fileMetaFlushThreadState = false;
        flushFileReadPositionThreadState = false;
        chackMappedFileThread.interrupt();
        fileMetaFlushThread.interrupt();
        flushFileReadPositionThread.interrupt();
        try {
            for (Thread thread : new Thread[]{chackMappedFileThread, fileMetaFlushThread, flushFileReadPositionThread}) {
                if (thread == Thread.currentThread()) continue;
                thread.join(10000);
                if (thread.isAlive()) throw new WalException("WAL background task did not stop: " + thread.getName());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new WalException("Interrupted while stopping WAL tasks for " + bucketName, e);
        }
    }

    /**
     * 中文：只检查内存索引是否为空，不扫描磁盘目录。
     * English: Checks only whether the in-memory index is empty; does not scan disk.
     *
     * @return 中文：注册映射数量是否为0；English: whether no mappings are registered
     */
    public boolean mappedFileIsEmpty() {
        return mappedFiles.isEmpty();
    }

    /**
     * 中文：借用Bucket元数据映射，调用者不能独立关闭所属Arena。
     * English: Borrows the bucket-metadata mapping; callers must not independently close its arena.
     *
     * @return 中文：有效期受管理器Arena约束的segment；English: segment whose lifetime is governed by the manager's arena
     */
    public MemorySegment getBucketMetaFileSegment() {
        return bucketMetaFileSegment;
    }
}


