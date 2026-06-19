package org.foreverfzl.cloudcache.storage.factory;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.time.Duration;

/**
 * Factory class for creating and configuring S3Client instances.
 * Supports standard AWS S3 and S3-compatible object storage such as MinIO.
 */
public class S3ClientFactory {

    // 静态属性保存默认值
    public static boolean pathStyleAccess = true;
    public static Duration connectionTimeout = Duration.ofSeconds(10);
    public static int retryCount = 3;

    // 固定属性
    private static final RetryMode RETRY_MODE = RetryMode.STANDARD;
    private static final boolean ACCELERATE_MODE = false;
    private static final boolean CHECKSUM_VALIDATION_ENABLED = false;

    /**
     * 创建 S3Client 实例，使用默认配置。
     *
     * @param endpoint  S3 端点 URL (必须)
     * @param region    S3 区域 (必须)
     * @param accessKey S3 访问密钥 (必须)
     * @param secretKey S3 私有密钥 (必须)
     * @return 配置好的 S3Client 实例
     */
    public static S3Client createS3Client(String endpoint, String region, String accessKey, String secretKey) {
        return createS3Client(endpoint, region, accessKey, secretKey, pathStyleAccess, connectionTimeout, retryCount);
    }

    /**
     * 创建 S3Client 实例，支持自定义配置。
     *
     * @param endpoint          S3 端点 URL (必须)
     * @param region            S3 区域 (必须)
     * @param accessKey         S3 访问密钥 (必须)
     * @param secretKey         S3 私有密钥 (必须)
     * @param pathStyleAccess   是否启用 Path-style 访问
     * @param connectionTimeout 连接超时时间
     * @param retryCount        重试次数
     * @return 配置好的 S3Client 实例
     */
    public static S3Client createS3Client(String endpoint, String region, String accessKey, String secretKey,
                                          boolean pathStyleAccess, Duration connectionTimeout,int retryCount) {
        if (endpoint == null || endpoint.isBlank()) {
            throw new IllegalArgumentException("endpoint cannot be null or empty");
        }
        if (region == null || region.isBlank()) {
            throw new IllegalArgumentException("region cannot be null or empty");
        }
        if (accessKey == null || accessKey.isBlank()) {
            throw new IllegalArgumentException("accessKey cannot be null or empty");
        }
        if (secretKey == null || secretKey.isBlank()) {
            throw new IllegalArgumentException("secretKey cannot be null or empty");
        }

        // 1. S3 专属配置 (关闭 checksum, 关闭 accelerate, 配置 pathStyle)
        S3Configuration s3Configuration = S3Configuration.builder()
                .pathStyleAccessEnabled(pathStyleAccess)
                .accelerateModeEnabled(ACCELERATE_MODE)
                .checksumValidationEnabled(CHECKSUM_VALIDATION_ENABLED)
                .build();

        // 2. 客户端通用重写配置 (配置固定 STANDARD 模式的重试策略与自定义重试次数)
        ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
                .retryPolicy(RetryPolicy.forRetryMode(RETRY_MODE).toBuilder()
                        .numRetries(retryCount)
                        .build())
                .build();

        // 3. 构建并返回 S3Client (使用 UrlConnectionHttpClient, 设定连接与读取超时时间)
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)
                ))
                .serviceConfiguration(s3Configuration)
                .overrideConfiguration(overrideConfiguration)
                .httpClient(UrlConnectionHttpClient.builder()
                        .connectionTimeout(connectionTimeout)
                        .build())
                .build();
    }
}
