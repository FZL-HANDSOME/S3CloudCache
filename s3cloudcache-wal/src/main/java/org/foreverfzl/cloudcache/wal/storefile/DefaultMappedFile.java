package org.foreverfzl.cloudcache.wal.storefile;

import org.foreverfzl.cloudchache.common.ProjectConstants;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
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

    protected  long fileFromOffset; //文件的起始位点
    protected volatile int wrotePosition; //数据写入位置
    protected volatile int committedPosition; //写入到操作系统文件缓冲区的位置
    protected volatile int flushedPosition; //文件刷盘位置
    protected volatile int upLoadPosition; //该文件上传到云服务器的位置

    protected int fileSize;
    protected FileChannel fileChannel;
    protected MemorySegment mappedMemorySegment; //本质是MMP内存映射

    //获取操作系统 锁内存 方法的 机器码
    private static final MethodHandle LOCK_HANDLE;
    //获取取消操作系统锁内存 方法的机器吗
    private static final MethodHandle UNLOCK_HANDLE;
    //看看是不是windows
    private static final boolean IS_WINDOWS;

    static {
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "committedPosition");
        FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "flushedPosition");
        UPLOAD_POSITION = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "upLoadPosition");

        MethodHandle lockHandle = null;
        MethodHandle unlockHandle = null;
        boolean isWindows = false;
        try {
            String osName = System.getProperty("os.name").toLowerCase();
            isWindows = osName.contains("win");
            Linker linker = Linker.nativeLinker();
            if (isWindows) {
                // Windows: VirtualLock / VirtualUnlock
                SymbolLookup kernel32 = SymbolLookup.libraryLookup("kernel32", Arena.global());
                lockHandle = linker.downcallHandle(
                    kernel32.find("VirtualLock").orElseThrow(() -> new RuntimeException("VirtualLock not found")),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
                );
                unlockHandle = linker.downcallHandle(
                    kernel32.find("VirtualUnlock").orElseThrow(() -> new RuntimeException("VirtualUnlock not found")),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
                );
            } else {
                // Linux/Unix: mlock / munlock
                SymbolLookup stdlib = linker.defaultLookup();
                lockHandle = linker.downcallHandle(
                    stdlib.find("mlock").orElseThrow(() -> new RuntimeException("mlock not found")),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
                );
                unlockHandle = linker.downcallHandle(
                    stdlib.find("munlock").orElseThrow(() -> new RuntimeException("munlock not found")),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
                );
            }
        } catch (Throwable t) {
            System.err.println("Failed to initialize native memory lock handler: " + t.getMessage());
        }
        LOCK_HANDLE = lockHandle;
        UNLOCK_HANDLE = unlockHandle;
        IS_WINDOWS = isWindows;
    }

    /**
     * 预热 PageCache 并且锁定映射内存，确保其不会被换出到虚拟内存中，也不会被操作系统移动。
     *
     * @param pages 预热多少页后就执行一次强制刷盘
     */
    public void warmAndLock(int pages) {
        if (mappedMemorySegment == null) {
            throw new IllegalStateException("mappedMemorySegment has not been mapped yet.");
        }
        if (pages <= 0) {
            throw new IllegalArgumentException("pages must be greater than 0");
        }

        long size = mappedMemorySegment.byteSize();
        // 动态获取操作系统页大小，若获取不到则默认使用 4096 字节
        int pageSize = ProjectConstants.OS_PAGE_SIZE;
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

        // 2. 锁定内存，防止操作系统将其换出到虚拟内存或发生移动
        if (LOCK_HANDLE != null) {
            try {
                int result = (int) LOCK_HANDLE.invokeExact(mappedMemorySegment, size);
                if (IS_WINDOWS) {
                    if (result == 0) {
                        System.err.println("Warning: VirtualLock failed. Memory might not be locked.");
                    }
                } else {
                    if (result != 0) {
                        System.err.println("Warning: mlock failed with code " + result + ". Memory might not be locked.");
                    }
                }
            } catch (Throwable t) {
                System.err.println("Warning: Failed to invoke native memory lock: " + t.getMessage());
            }
        } else {
            System.err.println("Warning: Native memory lock handler is not initialized. Memory is not locked.");
        }
    }

    /**
     * 解锁内存，允许操作系统将其换出
     */
    public void unlockMemory() {
        if (mappedMemorySegment == null) {
            return;
        }
        if (UNLOCK_HANDLE != null) {
            try {
                long size = mappedMemorySegment.byteSize();
                int result = (int) UNLOCK_HANDLE.invokeExact(mappedMemorySegment, size);
                if (IS_WINDOWS) {
                    if (result == 0) {
                        System.err.println("Warning: VirtualUnlock failed.");
                    }
                } else {
                    if (result != 0) {
                        System.err.println("Warning: munlock failed with code " + result);
                    }
                }
            } catch (Throwable t) {
                System.err.println("Warning: Failed to invoke native memory unlock: " + t.getMessage());
            }
        }
    }

    //todo 删除对应文件的方法
    @Override
    public boolean realClean() {
        unlockMemory();
        return true;
    }

}

