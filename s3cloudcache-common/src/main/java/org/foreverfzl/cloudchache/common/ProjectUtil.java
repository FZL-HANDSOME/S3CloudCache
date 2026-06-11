package org.foreverfzl.cloudchache.common;

import sun.misc.Unsafe;

import java.io.File;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;


/**
 * 该类存储一些静态变量，静态操作系统方法等
 */
public final class ProjectUtil {
    //用户目录
    public static final String USER_HOME = System.getProperty("user.home");
    //磁盘持久化地址
    public static final String DEFAULT_DISK_PERSISTENT_ADDRESS = USER_HOME + File.separator + ".cloudcache" + File.separator + "store"+File.separator;

    public static final Unsafe UNSAFE;
    //操作系统页大小
    public static final int OS_PAGE_SIZE ;

    //获取操作系统锁内存 方法的 机器码
    private static final MethodHandle LOCK_HANDLE;
    //获取取消操作系统锁内存 方法的机器吗
    private static final MethodHandle UNLOCK_HANDLE;
    //看看是不是windows
    private static final boolean IS_WINDOWS;


    static {
        //反射获取unsafe
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to obtain Unsafe instance", e);
        }
        //初始化OS_PAGE_SIZE
        OS_PAGE_SIZE=UNSAFE == null ? 1024 * 4 : UNSAFE.pageSize();

        //根据操作系统获取对应方法的机器码
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
            System.err.println("Failed to initialize native memory lock/file handlers: " + t.getMessage());
        }
        LOCK_HANDLE = lockHandle;
        UNLOCK_HANDLE = unlockHandle;
        IS_WINDOWS = isWindows;
    }

    public static boolean isWindows() {
        return IS_WINDOWS;
    }

    //锁定指定内存，防止转移到虚拟内存中
    //todo 目前锁PageCache不知道能不能成功，操作系统默认只让用户锁64MB
    public static void lockMappedPages(MemorySegment memorySegment) {
        long size = memorySegment.byteSize();
        // 2. 锁定内存，防止操作系统将其换出到虚拟内存或发生移动
        if (ProjectUtil.LOCK_HANDLE != null) {
            try {
                int result = (int) ProjectUtil.LOCK_HANDLE.invokeExact(memorySegment, size);
                if (ProjectUtil.IS_WINDOWS) {
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

    //取消锁定指定内存
    public static void unlockMappedPages(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return;
        }
        if (ProjectUtil.UNLOCK_HANDLE != null) {
            try {
                long size = memorySegment.byteSize();
                int result = (int) ProjectUtil.UNLOCK_HANDLE.invokeExact(memorySegment, size);
                if (ProjectUtil.IS_WINDOWS) {
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


}
