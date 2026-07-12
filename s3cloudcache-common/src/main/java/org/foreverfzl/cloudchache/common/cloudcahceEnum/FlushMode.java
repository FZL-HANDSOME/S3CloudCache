package org.foreverfzl.cloudchache.common.cloudcahceEnum;

public enum FlushMode {
    /**
     * 完全依赖 Linux Page Cache
     */
    ASYNC,

    /**
     * 每次 append 后 force()
     */
    SYNC,

    /**
     * 后台线程定时 force()
     */
    TIMED
}
