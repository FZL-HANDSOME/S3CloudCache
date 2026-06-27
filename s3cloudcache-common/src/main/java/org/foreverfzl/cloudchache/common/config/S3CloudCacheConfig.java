package org.foreverfzl.cloudchache.common.config;

import org.foreverfzl.cloudchache.common.cloudcahceEnum.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 该类存储真个项目的配置·
 */
public class S3CloudCacheConfig {


    public String instanceName;
    /**
     * 用户指定的持久化目录
     */
    public String walPath = null;
    /**
     * 默认配置文件
     */
    public BucketConfig defaultBucketConfig;
    /**
     * 特殊配置文件
     */
    public Map<String, BucketConfig> specialBuckets = new HashMap<>();


}

