package org.foreverfzl.cloudchache.common.cloudcahceEnum;

/**
 * 分为2个级别  写入到操作系统Page 和 落盘
 */
public enum AckLevel {
    /**
     * 写入到操作系统页缓存中就返回成功
     */
    WRITE_PAGE,
    /**
     * 数据真正落盘返回成功
     */
    WRITE_FILE;

}
