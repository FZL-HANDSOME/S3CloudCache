package org.foreverfzl.cloudchache.common;

import sun.misc.Unsafe;

import java.io.File;
import java.lang.foreign.MemorySegment;

import static sun.misc.Unsafe.getUnsafe;

/**
 * 该类存储一些静态变量
 */
public final class ProjectConstants {
    //用户目录
    public static final String USER_HOME=System.getProperty("user.home");
    //磁盘持久化地址
    public static final String DISK_PERSISTENT_ADDRESS= USER_HOME + File.separator + ".cloudcache" + File.separator + "store";;

    public static final Unsafe UNSAFE = getUnsafe();
    //操作系统页大小
    public static final int OS_PAGE_SIZE= UNSAFE==null?1024*4:UNSAFE.pageSize();


}
