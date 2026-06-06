package com.cloudCache;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * A simple demo class to verify S3 SDK operations (read/write/remove) against a local MinIO server.
 */
public class MinioDemo {

    private static final String ENDPOINT = "http://127.0.0.1:9001";
    private static final String ACCESS_KEY = "admin";
    private static final String SECRET_KEY = "12345678";
    private static final String BUCKET_NAME = "test-bucket";
    private static final String OBJECT_KEY = "test-object-key.txt";

    static void main(String[] args) {
        System.out.println("Starting MinIO connection test...");

        try {
//            // 1. 测试 add 方法
//            String testContent = "Hello, MinIO! This is test data stored using the add method.";
//            byte[] contentBytes = testContent.getBytes(StandardCharsets.UTF_8);
//            System.out.println("--- Testing add method ---");
//            add(contentBytes, BUCKET_NAME, "text");

            // 2. 测试 read 方法
            System.out.println("\n--- Testing read method ---");
            byte[] readBytes = read(BUCKET_NAME, "text");
            String readContent = new String(readBytes, StandardCharsets.UTF_8);
            System.out.println("Read content: " + readContent);
//
//            // 3. 测试 remove 方法
//            System.out.println("\n--- Testing remove method ---");
//            remove(BUCKET_NAME, OBJECT_KEY);
//
//            // 验证删除成功
//            System.out.println("\n--- Verifying removal ---");
//            try {
//                read(BUCKET_NAME, OBJECT_KEY);
//                System.err.println("Warning: Object still exists!");
//            } catch (Exception e) {
//                System.out.println("Verification success: Object was successfully deleted (failed to read it).");
//            }

        } catch (Exception e) {
            System.err.println("An error occurred during test execution:");
            e.printStackTrace();
        }
    }

    /**
     * 向指定的 bucketName 写入字节数组数据。
     *
     * @param data       要写入的字节数组数据
     * @param bucketName 存储桶名称
     * @param key        对象键 (Key)
     */
    public static void add(byte[] data, String bucketName, String key) {
        try (S3Client s3Client = createClient()) {
            ensureBucketExists(s3Client, bucketName);
            s3Client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build(),
                    RequestBody.fromBytes(data)
            );
            System.out.printf("Successfully added object '%s' to bucket '%s'.%n", key, bucketName);
        } catch (Exception e) {
            System.err.printf("Failed to add object '%s' to bucket '%s'.%n", key, bucketName);
            throw e;
        }
    }

    /**
     * 从指定的 bucketName 读取指定 key 的字节数据。
     *
     * @param bucketName 存储桶名称
     * @param key        对象键 (Key)
     * @return 读取到的字节数组数据
     */
    public static byte[] read(String bucketName, String key) {
        try (S3Client s3Client = createClient()) {
            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(
                    GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build()
            );
            return objectBytes.asByteArray();
        } catch (Exception e) {
            System.err.printf("Failed to read object '%s' from bucket '%s'.%n", key, bucketName);
            throw e;
        }
    }

    /**
     * 从指定的 bucketName 删除指定 key 的对象。
     *
     * @param bucketName 存储桶名称
     * @param key        要删除的对象键 (Key)
     */
    public static void remove(String bucketName, String key) {
        try (S3Client s3Client = createClient()) {
            s3Client.deleteObject(
                    DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .build()
            );
            System.out.printf("Successfully removed object '%s' from bucket '%s'.%n", key, bucketName);
        } catch (Exception e) {
            System.err.printf("Failed to remove object '%s' from bucket '%s'.%n", key, bucketName);
            throw e;
        }
    }

    /**
     * 创建 S3 客户端实例（使用配置的静态常量）
     */
    private static S3Client createClient() {
        return S3Client.builder()
                .endpointOverride(URI.create(ENDPOINT))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
                ))
                .region(Region.US_EAST_1) // MinIO 忽略 Region，但 SDK 要求必须设置
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
    }

    /**
     * 辅助方法：确保指定的 Bucket 存在，若不存在则自动创建
     */
    private static void ensureBucketExists(S3Client s3Client, String bucketName) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
        } catch (NoSuchBucketException e) {
            System.out.printf("Bucket '%s' does not exist. Creating bucket...%n", bucketName);
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
            System.out.printf("Bucket '%s' created successfully.%n", bucketName);
        }
    }


}
