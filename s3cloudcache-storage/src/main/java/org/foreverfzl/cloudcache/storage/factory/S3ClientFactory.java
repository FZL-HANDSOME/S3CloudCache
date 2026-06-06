package org.foreverfzl.cloudcache.storage.factory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

import java.net.URI;

/**
 * factory class for creating and configuring S3Client instances.
 * Supports standard AWS S3 and S3-compatible object storage such as MinIO.
 */
public class S3ClientFactory {

    private static final String DEFAULT_REGION = "cn-east-1";

    /**
     * 根据 URL（Endpoint）、账号、密码和 BucketName 创建 S3 客户端，其他参数默认。
     * 该方法会自动确保指定的 bucketName 存在，如果不存在则自动创建。
     *
     * @param url        MinIO/S3 服务的 API 地址 (例如 http://127.0.0.1:9001)
     * @param accessKey  账号 / Access Key
     * @param secretKey  密码 / Secret Key
     * @param bucketName 存储桶名称
     * @return 配置好的 S3Client 实例
     */
    public static S3Client create(String url, String accessKey, String secretKey, String bucketName) {
        return create(url, accessKey, secretKey, DEFAULT_REGION, true, bucketName);
    }

    /**
     * 根据 URL（Endpoint）、账号、密码创建 S3 客户端，不限制特定的 Bucket，其他参数默认。
     * 默认开启 PathStyleAccess（适用于 MinIO），默认 Region 为 cn-east-1。
     *
     * @param url       MinIO/S3 服务的 API 地址
     * @param accessKey 账号 / Access Key
     * @param secretKey 密码 / Secret Key
     * @return 配置好的 S3Client 实例
     */
    public static S3Client create(String url, String accessKey, String secretKey) {
        return create(url, accessKey, secretKey, DEFAULT_REGION, true, null);
    }

    /**
     * 根据配置的 URL、账号、密码、区域和路径样式访问设置创建 S3 客户端。
     *
     * @param url              MinIO/S3 服务的 API 地址
     * @param accessKey        账号 / Access Key
     * @param secretKey        密码 / Secret Key
     * @param region           区域名称 (如 "us-east-1")
     * @param pathStyleEnabled 是否开启路径样式访问 (MinIO/私有部署 S3 推荐开启)
     * @return 配置好的 S3Client 实例
     */
    public static S3Client create(String url, String accessKey, String secretKey, String region, boolean pathStyleEnabled) {
        return create(url, accessKey, secretKey, region, pathStyleEnabled, null);
    }

    /**
     * 最全参数的创建方法。根据用户的需求定制化创建 S3Client 对象，并可选验证/创建 Bucket。
     *
     * @param url              MinIO/S3 服务的 API 地址
     * @param accessKey        账号 / Access Key
     * @param secretKey        密码 / Secret Key
     * @param region           区域名称
     * @param pathStyleEnabled 是否开启路径样式访问
     * @param bucketName       可选的 bucketName。如果传入非空值，则会检查该 bucket 并自动创建
     * @return 配置好的 S3Client 实例
     */

    public static S3Client create(String url, String accessKey, String secretKey, String region, boolean pathStyleEnabled, String bucketName) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("Endpoint URL cannot be null or empty.");
        }
        if (accessKey == null || accessKey.isBlank()) {
            throw new IllegalArgumentException("AccessKey cannot be null or empty.");
        }
        if (secretKey == null || secretKey.isBlank()) {
            throw new IllegalArgumentException("SecretKey cannot be null or empty.");
        }

        Region s3Region = region != null && !region.isBlank() ? Region.of(region) : Region.of(DEFAULT_REGION);

        S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create(url))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)
                ))
                .region(s3Region)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(pathStyleEnabled)
                        .build())
                .build();

        // 如果用户指定了 bucketName，则在初始化时进行检测和自动创建
        if (bucketName != null && !bucketName.isBlank()) {
            try {
                ensureBucketExists(s3Client, bucketName);
            } catch (Exception e) {
                // 如果检测/创建桶失败，关闭 client 并将异常抛出，防止使用未就绪的桶
                s3Client.close();
                throw new RuntimeException("Failed to verify or create bucket '" + bucketName + "'.", e);
            }
        }

        return s3Client;
    }

    /**
     * 创建连接标准 AWS S3 的客户端（不重写 Endpoint 且默认不启用路径样式访问）。
     *
     * @param accessKey 账号
     * @param secretKey 密码
     * @param region    区域名称
     * @return 配置好的 S3Client 实例
     */
    public static S3Client createForAws(String accessKey, String secretKey, String region) {
        if (accessKey == null || accessKey.isBlank() || secretKey == null || secretKey.isBlank()) {
            throw new IllegalArgumentException("Credentials cannot be null or empty.");
        }
        Region s3Region = region != null && !region.isBlank() ? Region.of(region) : Region.of(DEFAULT_REGION);
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)
                ))
                .region(s3Region)
                .build();
    }

    /**
     * 辅助方法：确保指定的 Bucket 存在，若不存在则自动创建
     */
    private static void ensureBucketExists(S3Client s3Client, String bucketName) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
        } catch (NoSuchBucketException e) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        }
    }
}
