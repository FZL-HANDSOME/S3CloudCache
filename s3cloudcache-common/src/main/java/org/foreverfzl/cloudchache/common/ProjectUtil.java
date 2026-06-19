package org.foreverfzl.cloudchache.common;

import sun.misc.Unsafe;

import java.io.File;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 该类存储一些静态变量，静态操作系统方法等
 */
public final class ProjectUtil {
    //用户目录
    public static final String USER_HOME = System.getProperty("user.home");
    public static final String WAL_FILE_ADDRESS=File.separator+"CloudCache"+File.separator+"store";

    public static final Unsafe UNSAFE;
    //操作系统页大小
    public static final int OS_PAGE_SIZE ;

    //获取操作系统锁内存 方法的 机器码
    private static final MethodHandle LOCK_HANDLE;
    //获取取消操作系统锁内存 方法的机器吗
    private static final MethodHandle UNLOCK_HANDLE;
    //看看是不是windows
    private static final boolean IS_WINDOWS;

    // 1. 静态缓存机器唯一标识，防止 K8s 环境下主机名重复或本地多进程冲突
    private static final String MACHINE_IDENTIFIER;

    // 2. 线程安全的时间格式化器
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd");

    // 3. 进程内单机防重原子序列（从一个随机正数开始递增）
    private static final AtomicLong SEQUENCE = new AtomicLong(ThreadLocalRandom.current().nextLong(10_000, 100_000));


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

        String hostName;
        try {
            // 获取当前机器的主机名
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            // 极端无网卡状态下，回退到随机生成的安全前缀
            hostName = "fallback-node-" + ThreadLocalRandom.current().nextInt(1000, 9999);
        }

        // 获取当前 JVM 的进程 PID
        String jvmName = ManagementFactory.getRuntimeMXBean().getName();
        String pid = jvmName.contains("@") ? jvmName.split("@")[0] : String.valueOf(Thread.currentThread().getId());

        // 强行追加一个 16 进制的运行时随机盐，彻底打碎“两台机器起了一模一样的主机名且 PID 相同”的因果链
        String runtimeSalt = Long.toHexString(ThreadLocalRandom.current().nextLong(0x100000L, 0xFFFFFFL));

        // 最终形态：主机名_进程PID_运行盐 (例如: order-pay-pod-01_12345_a4f2bc)
        MACHINE_IDENTIFIER = hostName + "_" + pid + "_" + runtimeSalt;
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


    /**
     * 根据用户自定义前缀，生成绝对分布式唯一的 S3 Key
     *
     * @param userPrefix 用户传进来的第一个字符串（如 "coupon-order" 或 "tenant-A"）
     * @return 最终的 S3 物理对象键路径 (如: coupon-order/2026/06/16/pod-01_1234_a4f2bc/1718534055001_00001024.block)
     */
    public static String generateUniqueS3Key(final String userPrefix) {
        if (userPrefix == null || userPrefix.isBlank()) {
            throw new IllegalArgumentException("User prefix for S3Key cannot be null or empty");
        }

        // 获取当天的日期路由分区
        String datePath = LocalDate.now().format(DATE_FORMATTER);

        // 获取当前的绝对毫秒时间戳
        long currentTimestamp = System.currentTimeMillis();

        // 获取进程内绝对递增的无符号序列值
        long seq = SEQUENCE.getAndIncrement() & Long.MAX_VALUE;

        // 预分配 160 字节的缓冲区，避免 StringBuilder 在高并发下频繁触发内部数组扩容（扩容会导致内存拷贝）
        StringBuilder sb = new StringBuilder(160);

        sb.append(userPrefix).append('/')          // 1. 用户自定义的第一顺位字符串
                .append(datePath).append('/')            // 2. 日期层级分区 (yyyy/MM/dd)
                .append(MACHINE_IDENTIFIER).append('/')  // 3. 强隔离机器物理标识（彻底阻断跨机替换）
                .append(currentTimestamp).append('_')    // 4. 时间戳
                .append(String.format("%08d", seq));      // 5. 8位左补0的单机防重序列
        return sb.toString();
    }


}
