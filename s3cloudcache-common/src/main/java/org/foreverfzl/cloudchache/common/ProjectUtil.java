package org.foreverfzl.cloudchache.common;

import sun.misc.Unsafe;

import java.io.File;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.UUID;


/**
 * 该类存储一些静态变量，静态操作系统方法等
 * 中文：集中提供平台页操作、对象键拼装与块标识位运算；类初始化会查询本机信息并尝试链接原生锁页函数。
 * English: Centralizes platform page operations, object-key assembly, and block-identity bit operations; initialization queries the host and attempts native page-lock linkage.
 * 中文：调用锁页方法不会取得 MemorySegment 的生命周期所有权，持有映射的组件仍负责解锁与关闭。
 * English: Page-lock methods do not take ownership of MemorySegment lifetimes; the mapping owner remains responsible for unlocking and closing.
 */
public final class ProjectUtil {
    //用户目录
    /**
     * 中文：类初始化时读取的 user.home，作为未显式设置 WAL 基础目录时的默认值。
     * English: user.home captured at class initialization, used when no WAL base directory is supplied.
     */
    public static final String USER_HOME = System.getProperty("user.home");
    /**
     * 中文：附加到基础目录的 CloudCache/store 相对层级，包含当前平台的前置分隔符。
     * English: CloudCache/store hierarchy appended to the base directory, including a leading platform separator.
     */
    public static final String WAL_FILE_ADDRESS = File.separator + "CloudCache" + File.separator + "store";

    /**
     * 中文：通过反射取得的 JVM Unsafe 单例，用于平台页大小等底层操作；获取失败会使类初始化失败。
     * English: JVM Unsafe singleton obtained reflectively for low-level operations such as page size; failure aborts class initialization.
     */
    public static final Unsafe UNSAFE;
    //操作系统页大小
    /**
     * 中文：Unsafe 报告的 OS 页字节数；代码在 Unsafe 为 null 时采用 4096 字节回退。
     * English: OS page bytes reported by Unsafe; the code falls back to 4096 when Unsafe is null.
     */
    public static final int OS_PAGE_SIZE;

    //获取操作系统锁内存 方法的 机器码
    /**
     * 中文：VirtualLock 或 mlock 的原生调用句柄；未链接成功时可以为 null。
     * English: Native call handle for VirtualLock or mlock; may be null when linkage fails.
     */
    private static final MethodHandle LOCK_HANDLE;
    //获取取消操作系统锁内存 方法的机器吗
    /**
     * 中文：VirtualUnlock 或 munlock 的原生调用句柄；资源清理由映射拥有者发起。
     * English: Native call handle for VirtualUnlock or munlock; cleanup is initiated by the mapping owner.
     */
    private static final MethodHandle UNLOCK_HANDLE;
    //看看是不是windows
    /**
     * 中文：平台探测时 os.name 是否包含 win；用于选择锁页函数返回码规则。
     * English: Whether the probed os.name contains win; selects the native lock function's return-code convention.
     */
    private static final boolean IS_WINDOWS;
    //机器标识
    /**
     * 中文：本次类加载缓存的主机名和首个可用 MAC 组合，探测异常时使用随机 UUID。
     * English: Hostname and first available MAC cached for this class loading, with a random UUID on lookup failure.
     */
    private static final String MACHINE_ID;

    // 中文：Unsafe 获取失败会中止初始化；原生锁页链接失败则仅告警，允许后续使用不锁页的路径。
    // English: Unsafe lookup failure aborts initialization; native page-lock linkage failure only warns, allowing unlocked operation.
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
            // 中文：Windows 句柄返回非零表示成功，Unix mlock/munlock 返回零表示成功。
            // English: Windows handles report success with nonzero results; Unix mlock/munlock report success with zero.
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

    /**
     * 中文：读取类初始化时缓存的平台判断，不重新查询系统属性或检查锁页句柄是否可用。
     * English: Reads the cached platform decision without rechecking system properties or native-handle availability.
     * @return 中文：初始化探测认为是 Windows 时为 true；English: true when initialization identified Windows
     */
    public static boolean isWindows() {
        return IS_WINDOWS;
    }

    //构建机器唯一标识
    /**
     * 中文：构造当前主机的 Key 身份分量；回退随机 UUID 只在本次初始化中保存，不持久化到 WAL。
     * English: Builds the host component of key identity; a fallback UUID is retained for this initialization but is not persisted to WAL.
     * @return 中文：主机名加 MAC 文本，或主机查询失败时的 UUID；English: hostname plus MAC text, or a UUID if host lookup fails
     */
    private static String buildMachineId() {
        try {
            String hostName = InetAddress.getLocalHost().getHostName();
            String mac = getMacAddress();
            return hostName + "-" + mac;
        } catch (Exception e) {
            // 极端情况下，例如容器网络不可用
            // 中文：没有可用主机信息时仍可生成键，但随机回退值不承诺跨进程重启稳定。
            // English: Keys remain constructible without host information, but the random fallback is not stable across restarts.
            return UUID.randomUUID().toString();
        }
    }

    //回去机器的MAC
    /**
     * 中文：按 NetworkInterface 的枚举顺序选择首个非 null 硬件地址，未找到或查询异常时返回占位文本。
     * English: Selects the first non-null hardware address in NetworkInterface order, returning a placeholder if unavailable or lookup fails.
     * @return 中文：无分隔符的大写十六进制 MAC，或 UNKNOWN_MAC；English: separator-free uppercase hexadecimal MAC, or UNKNOWN_MAC
     */
    private static String getMacAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();
                byte[] mac = ni.getHardwareAddress();
                // 中文：跳过没有硬件地址的接口；这里不按接口名称或是否活动做进一步选择。
                // English: Skip interfaces without hardware addresses; no additional name or active-state selection is performed.
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
    /**
     * 中文：尽力请求锁定整个内存段对应的 OS 页；调用失败或句柄缺失只打印警告，不保证页面已驻留。
     * English: Makes a best-effort request to lock the segment's OS pages; invocation failure or missing handles only warn and do not guarantee residency.
     * 中文：调用方应提供存活且当前线程可访问的原生段，并在关闭该段前负责配对解锁；锁页不是刷盘。
     * English: Supply a live native segment accessible to this thread and arrange matching unlock before closing it; page locking is not flushing.
     * @param memorySegment 中文：借用的待锁页原生内存段，不转移所有权；English: borrowed native segment to lock, with no ownership transfer
     * @throws NullPointerException 中文：memorySegment 为 null；English: memorySegment is null
     */
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
    /**
     * 中文：尽力解除整个段的页锁；null 直接返回，句柄缺失也不执行操作，原生调用失败仅告警。
     * English: Best-effort unlock for the whole segment; null or missing handles are no-ops, and native-call failures only warn.
     * 中文：不关闭 Arena、不卸载文件映射，也不把内存写入磁盘。
     * English: Does not close an Arena, unmap a file, or write memory to disk.
     * @param memorySegment 中文：仍存活的已借用原生段，允许 null；English: live borrowed native segment, or null
     */
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
     * 中文：按缓存的机器标识和传入逻辑身份确定性拼装键；24-bit 哈希仅为键的一部分，完整身份也保留在键中。
     * English: Deterministically assembles a key from cached machine identity and supplied logical identity; the 24-bit hash is only one part, with the full identity retained.
     * 中文：本方法不分配文件编号、不验证命名空间，也不检查远端冲突；调用方负责正确的实例和文件身份。
     * English: This method neither allocates file identities, validates namespaces, nor checks remote collisions; callers supply the proper instance and file identities.
     *
     * @param userPrefix 中文：直接拼接的用户前缀，不在此归一化；English: user prefix concatenated without normalization
     * @param instanceName 中文：实例身份名称；English: instance identity name
     * @param bucketName 中文：目标 Bucket 名称；English: target Bucket name
     * @param fileFromOffset 中文：WAL 文件的逻辑身份编号；English: logical identity of the WAL file
     * @param blockIndex 中文：文件内逻辑块索引；English: logical block index within the file
     * @return 最终的 S3 物理对象键路径 ({用户自定义前缀}/)
     * English: S3 object key ending in .block, assembled from the supplied identity components.
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
        // 中文：掩码保留哈希低 24 位；后续仍拼接原始身份分量，不能只用这个短哈希索引 Block。
        // English: The mask keeps 24 hash bits; original identity fields are still appended, so this short hash alone must not identify a Block.
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
     * 中文：仅在非负被除数和正的 2 次幂除数前提下等价于整数除法；本方法不校验这些前提。
     * English: Equivalent to integer division only for a nonnegative dividend and a positive power-of-two divisor; these preconditions are not validated here.
     *
     * @param num1 被除数 非负
     * English: Nonnegative dividend.
     * @param num2 除数，必须是2的整数次幂
     * English: Positive power-of-two divisor.
     * @return 整除结果
     * English: Quotient obtained by an arithmetic right shift.
     */
    public static long divideByPower(long num1, int num2) {
        // 获取2的幂次，即右移位数
        int shiftBits = Integer.numberOfTrailingZeros(num2);
        return num1 >> shiftBits;
    }


    /**
     * blockIndex 使用 10 bit（可表示 0~1023，共 1024 个 Block）
     * 中文：复合块键为文件身份预留高 54 位，为块索引固定预留低 10 位。
     * English: Composite block keys reserve the high 54 bits for file identity and the low 10 bits for block index.
     */
    private static final int BLOCK_INDEX_BITS = 10;

    /**
     * blockIndex 掩码 (0x3FF)
     * 中文：编码与解码都只保留块索引的低 10 位。
     * English: Both encoding and decoding retain only the block index's low 10 bits.
     */
    private static final long BLOCK_INDEX_MASK = (1L << BLOCK_INDEX_BITS) - 1;

    /**
     * 生成唯一 BlockKey
     * 中文：组合文件身份与块索引，不分配新身份；调用方须保证文件编号非负且最多占 54 位、块索引处于 0..1023。
     * English: Combines existing identities without allocating one; callers must provide a nonnegative file identity fitting 54 bits and an index in 0..1023.
     * 中文：超出范围不会抛异常，而会截断位；同一个键只在正确的实例/Bucket 管理范围内解释。
     * English: Out-of-range inputs truncate rather than throw; interpret the key only within its intended instance/Bucket scope.
     *
     * @param fileFromOffset WAL 文件起始 Offset（即 fileName）
     * English: WAL file identity, stored in the composite key's upper bits.
     * @param blockIndex     Block 编号（0~1023）
     * English: Logical block index in the range 0..1023.
     * @return 唯一 BlockKey
     * English: Bit-packed key for the supplied file identity and block index.
     */
    public static long buildBlockKey(long fileFromOffset, int blockIndex) {
        return (fileFromOffset << BLOCK_INDEX_BITS) | (blockIndex & BLOCK_INDEX_MASK);
    }

    /**
     * 获取 fileFromOffset
     * 中文：无符号右移移除索引位，恢复编码时保留的文件身份部分。
     * English: Removes index bits with an unsigned shift and recovers the file-identity portion retained during encoding.
     * @param blockKey 中文：buildBlockKey 格式的复合键；English: composite key in buildBlockKey format
     * @return 中文：无符号高 54 位表示的文件身份；English: file identity represented by the unsigned upper 54 bits
     */
    public static long parseFileFromOffset(long blockKey) {
        return blockKey >>> BLOCK_INDEX_BITS;
    }

    /**
     * 获取 blockIndex
     * 中文：提取低 10 位，不检查该块是否真实存在。
     * English: Extracts the low 10 bits without checking whether that block exists.
     * @param blockKey 中文：buildBlockKey 格式的复合键；English: composite key in buildBlockKey format
     * @return 中文：0..1023 范围的块索引；English: block index in the range 0..1023
     */
    public static int parseBlockIndex(long blockKey) {
        return (int) (blockKey & BLOCK_INDEX_MASK);
    }


}
