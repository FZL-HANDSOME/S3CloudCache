package org.foreverfzl.cloudchache.common.cloudcahceEnum;

/**
 * 分为2个级别  写入磁盘返回成功 、写入缓冲区返回成功、成功上传到S3服务器返回成功
 */
public enum AckLevel {

    // 2. 写入缓冲区成功
    BUFFER_WRITE_SUCCESS,

    // 3. 成功上传到S3服务器返回成功
    S3_UPLOAD_SUCCESS;

}
