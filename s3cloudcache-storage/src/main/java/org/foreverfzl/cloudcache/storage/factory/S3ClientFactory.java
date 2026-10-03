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
 * 中文：创建同步 S3Client 的配置工厂；不创建 Bucket、不验证凭证可用性，也不执行对象上传。
 * English: Configuration factory for synchronous S3 clients; it does not create buckets, validate remote credentials or upload objects.
 * Factory class for creating and configuring S3Client instances.
 * Supports standard AWS S3 and S3-compatible object storage such as MinIO.
 * 中文：返回客户端的关闭责任归调用方；传给 S3CloudCacheInstance 后该实例会关闭它，勿与独立生命周期的实例共享。
 * English: Callers own client shutdown; S3CloudCacheInstance closes a supplied client, so do not share it across independent instance lifecycles.
 */
public class S3ClientFactory {

    // 静态属性保存默认值
    /** 中文：后续默认构造使用路径式寻址，默认 true，常用于 MinIO；非同步可变全局配置，应在启动前设置。
     * English: Mutable global default for path-style addressing (true, useful for MinIO); configure before concurrent use. */
    public static boolean pathStyleAccess = true;
    /** 中文：建连超时默认 10 秒，不是整个 PUT 或 read/socket 的超时预算。
     * English: Default connection timeout of 10 seconds, not an end-to-end PUT or read/socket timeout. */
    public static Duration connectionTimeout = Duration.ofSeconds(10);
    /** 中文：SDK 重试次数默认 3；Core 还有独立上传重试，两层次数可能叠加。
     * English: SDK retry count defaults to 3; Core has its own upload retries, so attempts may compound. */
    public static int retryCount = 3;

    // 固定属性
    /** 中文：SDK STANDARD 重试模式，不代表 CloudCache 的业务提交策略；English: SDK STANDARD retry policy, independent of CloudCache commit semantics. */
    private static final RetryMode RETRY_MODE = RetryMode.STANDARD;
    /** 中文：不启用 AWS 专属传输加速，便于自定义兼容端点；English: disables AWS transfer acceleration for custom compatible endpoints. */
    private static final boolean ACCELERATE_MODE = false;
    /** 中文：禁用此 SDK 配置项的校验功能，不等于禁用 WAL CRC；English: disables this SDK checksum option, not WAL CRC verification. */
    private static final boolean CHECKSUM_VALIDATION_ENABLED = false;

    /**
     * 创建 S3Client 实例，使用默认配置。
     * English: Creates an S3 client from the mutable defaults; the caller owns closing the returned service client.
     *
     * @param endpoint  S3 端点 URL (必须)；English: required nonblank endpoint URI.
     * @param region    S3 区域 (必须)；English: required nonblank signing region.
     * @param accessKey S3 访问密钥 (必须)；English: required nonblank access identifier.
     * @param secretKey S3 私有密钥 (必须)；English: required nonblank secret.
     * @return 配置好的 S3Client 实例；English: configured service client owned by the caller.
     * English: endpoint is a nonblank URI, region/accessKey/secretKey are nonblank; returns a caller-owned client.
     * @throws IllegalArgumentException 中文：空参数或 URI 非法；SDK 也可拒绝非法配置；English: blank arguments or invalid URI; SDK builders may reject invalid settings.
     */
    public static S3Client createS3Client(String endpoint, String region, String accessKey, String secretKey) {
        return createS3Client(endpoint, region, accessKey, secretKey, pathStyleAccess, connectionTimeout, retryCount);
    }

    /**
     * 创建 S3Client 实例，支持自定义配置。
     * 中文：此方法只做本地构建，不保证网络可达；凭证由静态 provider 持有，没有自动刷新。
     * English: Builds locally without proving connectivity; a static credentials provider is used, without automatic credential refresh.
     * 中文：显式 httpClient 实例不会由 SDK 服务客户端代为关闭；当前 UrlConnectionHttpClient.close 是空操作，连接随响应流关闭。
     * English: An explicitly supplied httpClient is not closed by the SDK service client; this UrlConnectionHttpClient has a no-op close and releases connections through response streams.
     * 中文：将来更换为持有连接池的 HTTP 实现时须重新明确所有权，本方法目前没有暴露单独的 HTTP 客户端句柄。
     * English: Revisit ownership before switching to a pooled HTTP implementation; this method currently exposes no separate HTTP-client handle.
     *
     * @param endpoint          S3 端点 URL (必须)；English: required nonblank endpoint URI.
     * @param region            S3 区域 (必须)；English: required nonblank signing region.
     * @param accessKey         S3 访问密钥 (必须)；English: required nonblank access identifier.
     * @param secretKey         S3 私有密钥 (必须)；English: required nonblank secret.
     * @param pathStyleAccess   是否启用 Path-style 访问；English: whether to use path-style addressing.
     * @param connectionTimeout 连接超时时间；English: connect duration, with value validation delegated to the HTTP builder.
     * @param retryCount        重试次数；English: SDK retry count, independent of Core retries.
     * @return 配置好的 S3Client 实例；English: configured caller-owned service client.
     * English: endpoint is a nonblank URI; region and keys are nonblank; pathStyleAccess selects addressing, connectionTimeout sets connect duration,
     * retryCount configures SDK retries, and the returned client must be closed by its owner. Additional value validation is delegated to the SDK.
     * @throws IllegalArgumentException 中文：显式字符串校验或 URI/SDK 配置校验失败；English: string, URI or SDK configuration validation fails.
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
        // 中文：不在这里验证服务端是否支持这些选项，兼容性错误可能在首次请求时出现。
        // English: Server compatibility is not probed here; incompatibilities may surface on the first request.
        S3Configuration s3Configuration = S3Configuration.builder()
                .pathStyleAccessEnabled(pathStyleAccess)
                .accelerateModeEnabled(ACCELERATE_MODE)
                .checksumValidationEnabled(CHECKSUM_VALIDATION_ENABLED)
                .build();

        // 2. 客户端通用重写配置 (配置固定 STANDARD 模式的重试策略与自定义重试次数)
        // English: SDK retries are configured independently of the block uploader's outer retry loop.
        ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
                .retryPolicy(RetryPolicy.forRetryMode(RETRY_MODE).toBuilder()
                        .numRetries(retryCount)
                        .build())
                .build();

        // 3. 构建并返回 S3Client (使用 UrlConnectionHttpClient, 设定连接与读取超时时间)
        // 中文：更正：下面只显式设置连接超时，读取/socket 超时沿用 SDK 默认配置。
        // English: Clarification: only connect timeout is explicitly set below; read/socket timeout uses SDK defaults.
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
