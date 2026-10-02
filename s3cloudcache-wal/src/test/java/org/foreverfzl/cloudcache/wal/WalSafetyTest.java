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
public class WalSafetyTest {
    private static final int BLOCK_SIZE = 4096;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private MappedFileManager manager(Path directory) {
        BucketConfig config = new BucketConfig().setS3KeyPrefix("wal-safety")
                .setBlockSize(BLOCK_SIZE).setWalFileSize(4L * BLOCK_SIZE)
                .setWarmWalFile(false).setLockMappedFilePageCache(false);
        config.chackMappedFileTime = 60000;
        MappedFileManager manager = new MappedFileManager(directory.toString(), "test", "bucket", config, 0);
        manager.stopAllThread();
        return manager;
    }

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

    @Test
    public void lastInFlightWalWriterMakesSealedBlockFlushable() throws Exception {
        MappedFileManager manager = manager(temporaryFolder.newFolder("in-flight").toPath());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            WalDataStruct delegate = new WalDataStruct(new byte[4000]);
            DataStruct blocked = new DataStruct() {
                @Override public long getSerializedSize() { return delegate.getSerializedSize(); }
                @Override public int getDataLen() { return delegate.getDataLen(); }
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

    @Test
    public void corruptSequenceFailsClosedInsteadOfReusingZero() throws Exception {
        Path directory = temporaryFolder.newFolder("bad-sequence").toPath();
        MappedFileManager first = manager(directory);
        first.close();
        Files.write(directory.resolve("next-file-offset"), new byte[]{1, 2, 3});
        assertThrows(WalException.class, () -> manager(directory));
    }
}
