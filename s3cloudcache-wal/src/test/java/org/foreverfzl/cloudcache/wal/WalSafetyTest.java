package org.foreverfzl.cloudcache.wal;

import org.foreverfzl.cloudcache.wal.datastruct.DataStruct;
import org.foreverfzl.cloudcache.wal.datastruct.FileMetaInfo;
import org.foreverfzl.cloudcache.wal.datastruct.WalDataStruct;
import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.AppendMessageResult;
import org.foreverfzl.cloudcache.wal.storefile.DefaultMappedFile;
import org.foreverfzl.cloudchache.common.config.BucketConfig;
import org.foreverfzl.cloudchache.common.exception.WalException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/** Small, deterministic WAL regressions. No S3 server or user data directories are used. */
/**
 * 中文：在JUnit临时目录测试WAL生命周期、刷盘和确认账本，不访问S3或用户目录；直接ack模拟上传成功，不验证网络或掉电级耐久性。
 * English: Tests WAL lifecycle, forcing, and acknowledgement bookkeeping in JUnit temporary directories without S3 or user paths. Direct ack simulates successful upload; these tests do not verify networking or power-loss durability.
 */
public class WalSafetyTest {
    /**
     * 中文：4KiB逻辑块，故意小于生产默认2MiB force批次以覆盖小块分支。
     * English: 4KiB logical blocks, deliberately smaller than the default 2MiB force chunk to exercise small-block handling.
     */
    private static final int BLOCK_SIZE = 4096;

    /**
     * 中文：JUnit拥有的独立临时目录，每例测试结束后清理，不触碰用户WAL。
     * English: JUnit-owned isolated temporary directory, cleaned after each test without touching user WAL.
     */
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    /**
     * 中文：创建小型真实映射并立即停止维护线程，由测试显式驱动force/cleanup以控制时序；调用者必须finally关闭管理器。
     * English: Creates small real mappings and immediately stops maintenance, letting tests explicitly drive force/cleanup ordering; callers must close the manager in finally.
     *
     * @param directory 中文：该测试独占的临时Bucket目录；English: temporary bucket directory exclusive to the test
     * @return 中文：后台维护已停止的管理器；English: manager with background maintenance stopped
     */
    private MappedFileManager manager(Path directory) {
        BucketConfig config = new BucketConfig().setS3KeyPrefix("wal-safety")
                .setBlockSize(BLOCK_SIZE).setWalFileSize(4L * BLOCK_SIZE)
                .setWarmWalFile(false).setLockMappedFilePageCache(false);
        config.chackMappedFileTime = 60000;
        MappedFileManager manager = new MappedFileManager(directory.toString(), "test", "bucket", config, 0);
        manager.stopAllThread();
        return manager;
    }

    /**
     * 中文：断言close不等于可删除、force不等于上传；读取真实文件核对Value字节，最后模拟上传后才允许canClean。读回正确不等于验证断电丢页场景。
     * English: Asserts close does not imply deletability and force does not imply upload. Reads value bytes from the real file and permits canClean only after simulated upload; readback is not a power-loss test.
     *
     * @throws Exception 中文：临时文件、映射或断言准备失败；English: temporary-file, mapping, or setup failure
     */
    @Test
    public void unuploadedTailCannotBeDeletedAndSmallBlocksAreFlushed() throws Exception {
        Path directory = temporaryFolder.newFolder("tail").toPath();
        MappedFileManager manager = manager(directory);
        try {
            byte[] payload = new byte[73];
            Arrays.fill(payload, (byte) 0x5a);
            assertTrue(manager.appendData(new WalDataStruct(payload)).isOk());
            DefaultMappedFile file = manager.getActiveMappedFile().get();
            file.close();
            assertFalse(file.canClean());
            manager.sealAllBlocks();
            file.ackReadPosition();
            assertEquals(BLOCK_SIZE, file.readPosition);
            assertFalse("Flushing is not an upload acknowledgement", file.canClean());
            try (FileChannel channel = FileChannel.open(directory.resolve("wal").resolve(file.getFileName()), StandardOpenOption.READ)) {
                ByteBuffer bytes = ByteBuffer.allocate(payload.length);
                channel.position(FileMetaInfo.FILE_META_SIZE + DataStruct.HEADER_LENGTH);
                while (bytes.hasRemaining()) assertTrue(channel.read(bytes) > 0);
                assertArrayEquals(payload, bytes.array());
            }
            file.ackUpLoadPosition(0);
            assertTrue(file.canClean());
        } finally {
            manager.close();
        }
    }

    /**
     * 中文：先确认块1再发送迟到PageCache回调，验证状态3不降回1；确认块0后连续位点应跨越两块。
     * English: Acknowledges block one, sends a late page-cache callback, and verifies state three is not downgraded; acknowledging block zero must advance across both blocks.
     *
     * @throws Exception 中文：临时映射操作失败；English: temporary mapping operations fail
     */
    @Test
    public void latePageCacheCallbackCannotUndoUploadAcknowledgement() throws Exception {
        MappedFileManager manager = manager(temporaryFolder.newFolder("monotonic").toPath());
        try {
            DefaultMappedFile file = manager.getActiveMappedFile().get();
            file.ackUpLoadPosition(1);
            assertEquals(0, file.upLoadPosition);
            file.setBlockStateArrayFinishedPageCache(1);
            assertTrue(file.isBlockUploaded(1));
            file.ackUpLoadPosition(0);
            assertEquals(2L * BLOCK_SIZE, file.upLoadPosition);
        } finally {
            manager.close();
        }
    }

    /**
     * 中文：关闭原映射后重开，检查持久化块1确认和恢复读索引；补确认块0后可连续推进。注入检查点是测试输入，并非模拟完整进程崩溃。
     * English: Reopens after closing the original mapping to check durable block-one acknowledgement and restored read index; acknowledging block zero then advances contiguously. Injected checkpoints are test input, not a full process-crash simulation.
     *
     * @throws Exception 中文：重开文件或映射失败；English: file reopen or mapping fails
     */
    @Test
    public void restoreKeepsOutOfOrderDurableAcknowledgements() throws Exception {
        Path directory = temporaryFolder.newFolder("restore").toPath();
        MappedFileManager manager = manager(directory);
        DefaultMappedFile original = manager.getActiveMappedFile().get();
        String fileName = original.getFileName();
        long offset = original.fileFromOffset;
        original.ackUpLoadPosition(1);
        manager.close();
        Path walPath = directory.resolve("wal");
        DefaultMappedFile restored = new DefaultMappedFile(walPath.toString(), fileName, offset,
                4L * BLOCK_SIZE, walPath.resolve(fileName).toFile(), BLOCK_SIZE, false, false, manager);
        try {
            restored.restorePositions(2L * BLOCK_SIZE, 0);
            assertFalse(restored.isBlockUploaded(0));
            assertTrue(restored.isBlockUploaded(1));
            assertEquals(2L * BLOCK_SIZE, restored.readPosition);
            restored.ackReadPosition();
            assertEquals("Restored read index must not restart from zero", 2L * BLOCK_SIZE, restored.readPosition);
            restored.ackUpLoadPosition(0);
            assertEquals(2L * BLOCK_SIZE, restored.upLoadPosition);
        } finally {
            restored.clean();
        }
    }

    /**
     * 中文：使用两个Latch暂停已预留的首条复制，由第二条跨块封口；未完成前读位点不能推进，释放首写入后必须可刷盘。finally解除等待并关闭执行器防止测试挂起。
     * English: Uses two latches to pause the first reserved copy while a second record seals the block. The read position must not advance before completion, but must become flushable afterwards; finally unblocks and stops the executor.
     *
     * @throws Exception 中文：等待、Future或临时文件操作失败；English: waiting, future, or temporary-file operations fail
     */
    @Test
    public void lastInFlightWalWriterMakesSealedBlockFlushable() throws Exception {
        MappedFileManager manager = manager(temporaryFolder.newFolder("in-flight").toPath());
        // 中文：entered确认首线程已经预留但尚未复制，resume控制真正复制时机，从而使封口竞态可重复。
        // English: entered confirms the first thread reserved but has not copied; resume controls copying so the sealing race is reproducible.
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            WalDataStruct delegate = new WalDataStruct(new byte[4000]);
            DataStruct blocked = new DataStruct() {
                /**
                 * 中文：保持被代理记录的真实预留长度，Latch只影响复制时序。
                 * English: Preserves the delegated reservation size; latches affect copying order only.
                 *
                 * @return 中文：对齐的WAL记录长度；English: aligned WAL record size
                 */
                @Override public long getSerializedSize() { return delegate.getSerializedSize(); }
                /**
                 * 中文：保持真实Value计数，避免测试改变封口条件。
                 * English: Preserves the actual value count so the test does not alter sealing conditions.
                 *
                 * @return 中文：Value字节数；English: value byte count
                 */
                @Override public int getDataLen() { return delegate.getDataLen(); }
                /**
                 * 中文：在真正复制前通知测试线程并等待放行，最多10秒；这是确定性故障时序控制，不是业务重试策略。
                 * English: Signals before the real copy and waits at most ten seconds for release; this is deterministic fault ordering, not a production retry policy.
                 *
                 * @param target 中文：已预留的目标切片；English: reserved target slice
                 */
                @Override public void writeTo(MemorySegment target) {
                    entered.countDown();
                    try {
                        if (!resume.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Test writer timed out");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    delegate.writeTo(target);
                }
            };
            Future<AppendMessageResult> first = executor.submit(() -> manager.appendData(blocked));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertTrue(manager.appendData(new WalDataStruct(new byte[100])).isOk());
            DefaultMappedFile file = manager.getActiveMappedFile().get();
            file.ackReadPosition();
            assertEquals("Reserved but unfinished bytes cannot be flushed as a sealed block", 0, file.readPosition);
            resume.countDown();
            assertTrue(first.get(5, TimeUnit.SECONDS).isOk());
            file.ackReadPosition();
            assertEquals(BLOCK_SIZE, file.readPosition);
        } finally {
            resume.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            manager.close();
        }
    }

    /**
     * 中文：删除空且满足清理条件的旧WAL后重建同目录管理器，断言sidecar使新编号增加；不模拟旧版本无sidecar历史。
     * English: Deletes an empty eligible WAL, recreates a manager in the same directory, and asserts the sidecar advances identity; does not simulate legacy history without a sidecar.
     *
     * @throws Exception 中文：临时文件生命周期操作失败；English: temporary-file lifecycle operations fail
     */
    @Test
    public void removingCompletedWalDoesNotReuseItsFileIdentity() throws Exception {
        Path directory = temporaryFolder.newFolder("sequence").toPath();
        MappedFileManager first = manager(directory);
        long oldOffset;
        try {
            DefaultMappedFile file = first.getActiveMappedFile().get();
            oldOffset = file.fileFromOffset;
            file.close();
            first.endChackMappedFile();
            assertTrue(first.mappedFileIsEmpty());
        } finally {
            first.close();
        }
        MappedFileManager second = manager(directory);
        try {
            assertTrue(second.getActiveMappedFile().get().fileFromOffset > oldOffset);
        } finally {
            second.close();
        }
    }

    /**
     * 中文：将临时编号文件截成3字节，断言构造拒绝继续而不是复用0；故障仅作用于测试目录。
     * English: Truncates the temporary sequence file to three bytes and asserts construction fails rather than reusing zero; the fault is isolated to the test directory.
     *
     * @throws Exception 中文：准备损坏文件失败；English: corrupt-file preparation fails
     */
    @Test
    public void corruptSequenceFailsClosedInsteadOfReusingZero() throws Exception {
        Path directory = temporaryFolder.newFolder("bad-sequence").toPath();
        MappedFileManager first = manager(directory);
        first.close();
        Files.write(directory.resolve("next-file-offset"), new byte[]{1, 2, 3});
        assertThrows(WalException.class, () -> manager(directory));
    }
}
