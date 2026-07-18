package org.foreverfzl.cloudchache.common;

import sun.misc.Unsafe;

import java.io.File;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 该类存储一些静态变量，静态操作系统方法等
 */
public final class ProjectUtil {
    //用户目录
    public static final String USER_HOME = System.getProperty("user.home");
    public static final String WAL_FILE_ADDRESS = File.separator + "CloudCache" + File.separator + "store";

    public static final Unsafe UNSAFE;
    //操作系统页大小
    public static final int OS_PAGE_SIZE;

    //获取操作系统锁内存 方法的 机器码
    private static final MethodHandle LOCK_HANDLE;
    //获取取消操作系统锁内存 方法的机器吗
    private static final MethodHandle UNLOCK_HANDLE;
    //看看是不是windows
    private static final boolean IS_WINDOWS;
    //机器标识
    private static final String MACHINE_ID;

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
        OS_PAGE_SIZE = UNSAFE == null ? 1024 * 4 : UNSAFE.pageSize();

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
        //保存机器标识
        MACHINE_ID = buildMachineId();
    }

    public static boolean isWindows() {
        return IS_WINDOWS;
    }

    //构建机器唯一标识
    private static String buildMachineId() {
        try {
            String hostName = InetAddress.getLocalHost().getHostName();
            String mac = getMacAddress();
            return hostName + "-" + mac;
        } catch (Exception e) {
            // 极端情况下，例如容器网络不可用
            return UUID.randomUUID().toString();
        }
    }

    //回去机器的MAC
    private static String getMacAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();
                byte[] mac = ni.getHardwareAddress();
                if (mac == null) {
                    continue;
                }
                StringBuilder sb = new StringBuilder();
                for (byte b : mac) {
                    sb.append(String.format("%02X", b));
                }
                return sb.toString();
            }
        } catch (Exception ignored) {

        }
        return "UNKNOWN_MAC";
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


    /**
     * 根据用户自定义前缀，生成绝对分布式唯一的 S3 Key
     *
     * @return 最终的 S3 物理对象键路径 ({用户自定义前缀}/)
     */
    public static String generateUniqueS3Key(final String userPrefix, final String instanceName, final String bucketName,
                                             final long fileFromOffset, int blockIndex) {
        String uniqueContent = MACHINE_ID
                + "_"
                + instanceName
                + "_"
                + bucketName
                + "_"
                + fileFromOffset
                + "_"
                + blockIndex;
        int hash = uniqueContent.hashCode() & 0xFFFFFF;
        return userPrefix
                + "/"
                + Integer.toUnsignedString(hash)
                + "_"
                + MACHINE_ID
                + "_"
                + instanceName
                + "_"
                + bucketName
                + "_"
                + fileFromOffset
                + "_"
                + blockIndex
                + ".block";
    }

    /**
     * 除法：num1 / num2，位运算实现
     *
     * @param num1 被除数 非负
     * @param num2 除数，必须是2的整数次幂
     * @return 整除结果
     */
    public static long divideByPower(long num1, int num2) {
        // 获取2的幂次，即右移位数
        int shiftBits = Integer.numberOfTrailingZeros(num2);
        return num1 >> shiftBits;
    }


    /**
     * blockIndex 使用 8 bit（0~255）
     */
    private static final int BLOCK_INDEX_BITS = 8;

    /**
     * blockIndex 掩码
     */
    private static final long BLOCK_INDEX_MASK = (1L << BLOCK_INDEX_BITS) - 1;

    /**
     * 生成唯一 BlockKey
     *
     * @param fileFromOffset WAL 文件起始 Offset（即 fileName）
     * @param blockIndex     Block 编号（0~255）
     * @return 唯一 BlockKey
     */
    public static long buildBlockKey(long fileFromOffset, int blockIndex) {
        return (fileFromOffset << BLOCK_INDEX_BITS) | (blockIndex & BLOCK_INDEX_MASK);
    }

    /**
     * 获取 fileFromOffset
     */
    public static long parseFileFromOffset(long blockKey) {
        return blockKey >>> BLOCK_INDEX_BITS;
    }

    /**
     * 获取 blockIndex
     */
    public static int parseBlockIndex(long blockKey) {
        return (int) (blockKey & BLOCK_INDEX_MASK);
    }


}
