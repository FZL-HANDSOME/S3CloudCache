# S3CloudCache

A high-performance **write aggregation and buffering layer** for object storage services such as **S3 / MinIO**.

S3CloudCache buffers massive amounts of small writes into **WAL (Write-Ahead Log)** and **off-heap Physical Blocks**, then aggregates data by Block and uploads each Block as a single large object to object storage.

This effectively transforms **a large number of small PUT requests into a small number of large PUT requests**, while providing crash recovery and data durability.

* **High throughput**: Off-heap memory (`MemorySegment`) + asynchronous batched uploads
* **Crash-safe**: Data is written to WAL first and can be recovered after a process crash
* **Black-box API**: Only the `s3cloudcache-instance` JAR is exposed; internal implementations are relocated and hidden

---

# Architecture

## Module Structure

| Module                                                            | Responsibility                                                              |
| ----------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `s3cloudcache-common`                                             | Common configuration, enums, `WriteResult`, exceptions, and utilities       |
| `s3cloudcache-core`                                               | Off-heap Physical Block pool and S3 upload engine                           |
| `s3cloudcache-wal`                                                | WAL persistence using memory-mapped files and crash recovery                |
| `s3cloudcache-metadata`                                           | Block metadata and the three major queues (upload / recovery / dead-letter) |
| `s3cloudcache-storage` (public artifact: `s3cloudcache-instance`) | Public APIs and S3 adapter                                                  |

Only `s3cloudcache-instance` is exposed to users.

The internal `core` / `wal` / `metadata` modules are shaded into the JAR and relocated to the `internal.*` package namespace, so users neither need nor are expected to access the internal implementation directly.

## Core Concepts

* **Logical Block**: A logical unit into which a WAL file is divided according to `blockSize`. It is the basic unit for sealing and uploading.
* **Physical Block**: An off-heap memory buffer bound 1:1 to a Logical Block. Data is aggregated in this buffer before being uploaded.
* **WAL (Write-Ahead Log)**: The write-ahead log. Data is first written to the memory-mapped WAL file (PageCache), and then copied into the off-heap Physical Block. After a crash, unuploaded data can be reconstructed from the WAL.
* **Seal**: A Block is marked as no longer writable when it becomes full or remains idle for too long. Once sealed, it can be uploaded to S3.

## Write Pipeline

```text
User write(data)
   │
   ├─ ① WAL persistence:
   │      Write data to the memory-mapped WAL file (PageCache)
   │      using the WAL protocol (Magic + Checksum + Length + Data)
   │      └─ The WAL file is divided into Logical Blocks according to blockSize
   │         and each Block tracks expected bytes / bytes already written to PageCache
   │
   ├─ ② Physical Block write:
   │      Write the data into the off-heap MemorySegment
   │      └─ Each Physical Block is bound 1:1 to a Logical Block
   │         and records the final S3 object key (s3Key)
   │         and the offset within the Block
   │
   └─ ③ Seal + Upload:
          A Block is sealed when it becomes full or idle for too long,
          then uploaded asynchronously to S3
          └─ After a successful upload, the upload position is advanced
             and the corresponding WAL file can be safely deleted
```

## When Is a Block Sealed?

A Block is sealed and scheduled for upload when any of the following conditions is met:

1. **Full**: The next write cannot fit into the remaining space of the current Logical Block. The current Block is sealed automatically and the write continues in the next Block.
2. **Idle timeout**: No new data is written within `blockMaxIdleTime`. This is useful for low-frequency writes and tail Blocks.
3. **Explicit shutdown**: `instance.close(...)` seals all remaining Blocks and waits for their uploads.

## Fault Tolerance

| Scenario                              | Handling                                                                                                                                       |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Process crash / machine failure       | On restart, `start()` scans WAL files, skips Blocks with durable upload acknowledgements, validates unacknowledged Blocks before replay, and retains corrupt WAL for repair |
| Physical Block write failure          | The affected Block is marked as broken. A background recovery thread re-reads the original data from WAL and rebuilds the Physical Block       |
| Upload failure after 3 retries        | The Block is placed into the dead-letter queue. The user can retrieve and repair it through `getUpLoadFailedBlockInfo()`                       |
| Single record larger than `blockSize` | The write is rejected and `WriteResult.isSuccess() == false`                                                                                   |

---

# Usage

## 1. Add Dependency（This dependency requires you to download the project and build it yourself.）

```xml
<dependency>
    <groupId>org.example</groupId>
    <artifactId>s3cloudcache-instance</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

> This dependency automatically brings in `s3cloudcache-common` (configuration / return types) and the AWS SDK `s3` + `url-connection-client`.
>
> The internal implementation modules are not exposed in the dependency tree.

## 2. Quick Start

```java
// 1. Create an S3 client (S3 / MinIO)
S3Client s3Client = S3ClientFactory.createS3Client(
        "http://127.0.0.1:9001",   // endpoint
        "cn-local",                // region
        "your-access-key",         // accessKey
        "your-secret-key"          // secretKey
);

// 2. Configure the bucket
BucketConfig bucketConfig = new BucketConfig()
        .setBlockSize(BlockSizeLevel.SMALL.getBytes())     // Block size: 4MB
        .setCacheSize(512 * 1024 * 1024L)                  // 512MB off-heap buffer
        .setBlockUpLoadCount(BlockUploadConcurrencyLevel.NORMAL.getConcurrency())
        .setS3KeyPrefix("order/phone")                     // S3 object key prefix
        .setWalFileSize(WalFileSize.SIZE_1G.getBytes());   // 1GB WAL file

// 3. Create the global configuration
S3CloudCacheConfig config = new S3CloudCacheConfig(
        "my-instance",   // instanceName: globally unique; do not change it
        null,            // walPath: null -> use default directory
        bucketConfig
);

// 4. Create and start the instance
//    start() performs crash recovery
S3CloudCacheInstance instance = new S3CloudCacheInstance(s3Client, config);
instance.start();

// 5. Get a write handle for a bucket
BucketWriterWriter writer = instance.getBucketWriterInstance("my-bucket");

// 6. Write data
byte[] data = "hello s3cloudcache".getBytes(StandardCharsets.UTF_8);
CompletableFuture<WriteResult> future = writer.writeHeapData(data);

future.whenComplete((result, throwable) -> {
    if (throwable == null && result != null && result.isSuccess()) {
        System.out.println("s3Key  = " + result.getS3Key());
        System.out.println("offset = " + result.getOffset());
        System.out.println("size   = " + result.getSize());
    }
});

// 7. Graceful shutdown
//    Wait for all Blocks to finish uploading
instance.close(15000, 15000, 15000);
```

## 3. Configuration

### 3.1 S3CloudCacheConfig (Global Configuration)

| Field                 | Type                      | Default  | Description                                                                                               |
| --------------------- | ------------------------- | -------- | --------------------------------------------------------------------------------------------------------- |
| `instanceName`        | String                    | Required | Globally unique instance name. **Do not change it**, as it participates in recovery and S3 key generation |
| `walPath`             | String                    | null     | WAL persistence directory. When null, `user.home/CloudCache/store` is used                                |
| `blockMaxIdleTime`    | Integer                   | 20000 ms | Maximum idle time before a Block is automatically sealed and uploaded                                     |
| `defaultBucketConfig` | BucketConfig              | Required | Default configuration for buckets                                                                         |
| `specialBuckets`      | Map<String, BucketConfig> | Empty    | Per-bucket override configuration; unspecified fields inherit from the default configuration              |

### 3.2 BucketConfig (Bucket-Level Configuration)

| Field                       | Type    | Default  | Description                                                                 |
| --------------------------- | ------- | -------- | --------------------------------------------------------------------------- |
| `s3KeyPrefix`               | String  | None     | Custom prefix used when generating S3 object keys, e.g. `order/phone`       |
| `walFileSize`               | Long    | 1GB      | Size of a single WAL file                                                   |
| `cacheSize`                 | Long    | 256MB    | Total off-heap buffer size (Physical Block pool) for the bucket             |
| `blockSize`                 | Integer | 4MB      | Size of a single Block; Logical Block and Physical Block have the same size |
| `blockUpLoadCount`          | Integer | 8        | Maximum number of concurrent Block uploads                                  |
| `isWarmWalFile`             | Boolean | true     | Whether to preheat the WAL file PageCache                                   |
| `isLockMappedFilePageCache` | Boolean | false    | Whether to lock the mapped memory to prevent paging                         |
| `enableHeadCheck`           | Boolean | false    | Whether to perform an additional `headObject` verification after upload     |
| `flushFileMetaInfoTime`     | Integer | 5000 ms  | Interval for flushing file metadata (`readPosition` / `upLoadPosition`)     |
| `chackMappedFileTime`       | Integer | 10000 ms | Interval for checking and deleting reclaimable WAL files                    |

### 3.3 Enum Values

**BlockSizeLevel**

| Enum   | Size |
| ------ | ---- |
| TINY   | 2MB  |
| SMALL  | 4MB  |
| MEDIUM | 8MB  |
| LARGE  | 16MB |
| ULTRA  | 32MB |

**WalFileSize**

| Enum       | Size  |
| ---------- | ----- |
| SIZE_256MB | 256MB |
| SIZE_512MB | 512MB |
| SIZE_1G    | 1GB   |
| SIZE_2G    | 2GB   |

**CacheSizeLevel**

| Enum               | Size  |
| ------------------ | ----- |
| TINE               | 256MB |
| LIGHTWEIGHT        | 512MB |
| STANDARD           | 1GB   |
| HIGH_THROUGHPUT_2G | 2GB   |
| HIGH_THROUGHPUT_4G | 4GB   |

**BlockUploadConcurrencyLevel**

| Enum   | Concurrency |
| ------ | ----------: |
| LOW    |           4 |
| NORMAL |           8 |
| HIGH   |          16 |
| ULTRA  |          32 |

> Recommendation: `cacheSize` should be large enough to hold at least one complete WAL file worth of Blocks (`cacheSize / blockSize` Blocks). This helps avoid write-side blocking when waiting for reusable Blocks.

---

# API Reference

## 4.1 S3ClientFactory

```java
// Default configuration:
// pathStyle = true
// connection timeout = 10s
// retry count = 3
S3Client c = S3ClientFactory.createS3Client(
        endpoint,
        region,
        accessKey,
        secretKey
);

// Custom configuration
S3Client c2 = S3ClientFactory.createS3Client(
        endpoint,
        region,
        accessKey,
        secretKey,
        true,                       // pathStyleAccess
        Duration.ofSeconds(20),     // connectionTimeout
        5                           // retryCount
);
```

## 4.2 S3CloudCacheInstance

| Method                                                                       | Description                                                                                                                      |
| ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `start()`                                                                    | Starts the instance and performs crash recovery                                                                                  |
| `getBucketWriterInstance(String bucketName)`                                 | Returns a write handle for the specified bucket; creates the bucket automatically if it does not exist                           |
| `close(long walWriteWaitTime, long blockWriteWaitTime, long upLoadWaitTime)` | Gracefully shuts down the instance: waits for WAL writes → waits for Block writes → seals remaining Blocks and waits for uploads |
| `s3RawPutObject(bucket, key, MemorySegment)`                                 | Uploads a MemorySegment directly using the underlying S3 client                                                                  |

## 4.3 BucketWriterWriter

| Method                                                          | Description                                                                             |
| --------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `writeHeapData(byte[] data)`                                    | Writes the entire heap array                                                            |
| `writeHeapData(byte[] data, long offset, long length)`          | Writes the `[offset, offset + length)` range without creating another array             |
| `writeOffHeapData(ByteBuffer buffer)`                           | Writes the `[position, limit)` range of a DirectByteBuffer without modifying `position` |
| `writeOffHeapData(ByteBuffer buffer, long offset, long length)` | Writes the `[position + offset, position + offset + length)` range of the buffer        |
| `getUpLoadFailedBlockInfo()`                                    | Blocks until a failed-upload entry is available, then returns a `MappedFileReader`      |

All `write*` methods return `CompletableFuture<WriteResult>`.

---

# 5. WriteResult

```java
public class WriteResult {
    String getS3Key();   // Final S3 object key for this record
    long   getOffset();  // Absolute byte offset of this record within the object
    int    getSize();    // Total length of this record in bytes
    boolean isSuccess(); // Whether the write succeeded
}
```

### Semantics

* `isSuccess() == true`: The Block has been uploaded to S3 and its local upload acknowledgement is durable. Only then is `s3Key + offset + size` a committed location.
* `isSuccess() == false` or an exceptional Future: The write was not acknowledged as successful. Unacknowledged WAL is retained after upload failure for restart or manual recovery; an object may nevertheless exist remotely.
* One `s3Key` corresponds to one Block. Multiple records in the same Block share the same `s3Key` and are distinguished by their `offset`.
* Upload is asynchronous, but successful Future completion waits for acknowledgement. A partial tail Block waits for idle sealing or `instance.close(...)`; avoid awaiting each individual write immediately in throughput tests.
* A Block is the minimum commit unit. Unacknowledged data may be reordered during recovery; acknowledged Blocks are not rebuilt and overwritten in WAL order.

---

# 6. Reading Data from S3

Data is located using:

```text
s3Key + offset + size
```

Use an HTTP Range GET to read exactly the required byte range:

```java
String s3Key  = result.getS3Key();
long   offset = result.getOffset();
int    size   = result.getSize();

// Exact byte range: bytes=offset-(offset+size-1)
String range = "bytes=" + offset + "-" + (offset + size - 1);

GetObjectRequest req = GetObjectRequest.builder()
        .bucket("my-bucket")   // The actual S3 bucket
        .key(s3Key)
        .range(range)
        .build();

try (ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(req)) {
    byte[] readBack = stream.readAllBytes();

    // readBack contains the original bytes
    // readBack.length == size

    // Perform byte-by-byte verification if needed
}
```

> A complete read-back integrity test can be found in
> `S3CloudCacheInstanceText`, including `dataIntegrityTest` and `highConcurrencyTest`.

---

# 7. Upload Failure Recovery

When a Block still fails after 3 upload retries, it is moved into the **dead-letter queue**.

The user can retrieve and manually repair the failed Block through `getUpLoadFailedBlockInfo()`:

```java
BucketWriterWriter writer = instance.getBucketWriterInstance("my-bucket");

// The reader owns a native snapshot and must be closed.
try (MappedFileReader reader = writer.getUpLoadFailedBlockInfo()) {
    // All Value Bytes in the entire Block; no protocol headers or padding.
    MemorySegment values = reader.readAll();
    var response = instance.s3RawPutObject(reader.getBucketName(), reader.getS3Key(), values);
    if (response.eTag() == null || response.eTag().isBlank()) {
        throw new IllegalStateException("Upload was not confirmed; retain WAL");
    }
    reader.ackUpLoadPosition(); // close() never acknowledges the WAL automatically.
}
```

> **Important:** `ackUpLoadPosition()` must only be called after the Block has been successfully uploaded.
>
> Otherwise, the upload position will remain stuck and the corresponding WAL file cannot be safely deleted.
> `hasNext()/next()` iterate Values. `readAll()` always represents the entire Block, regardless of the cursor. `getMemorySegment()` includes WAL headers and is not the Value-only upload body. Returned native views expire when the Reader closes; acknowledge before closing the Instance. Corrupt or incomplete Blocks fail instead of returning a valid prefix as a complete Block.

# 8. Integrity Regression Tests and Upgrade Boundaries

Run `mvn test` from the repository root with JDK 25 or newer (`mvn -o test` when dependencies are cached).
`IntegrityRegressionTest` runs isolated fault scenarios in `S3CloudCacheInstanceText`, alongside the WAL tests: concurrent heap/native writes, slices, shutdown tails, upload failure and restart, broken physical Block recovery, and stable acknowledged locations. Tests use temporary directories and an in-memory S3 client, not existing WAL or a real server, and compare record IDs, lengths, returned locations and every payload byte.

* The reserved WAL header now stores per-Block upload acknowledgements. A checksummed `next-file-offset` file in each Bucket directory prevents file number reuse; do not delete it independently.
* Legacy WAL has no per-Block acknowledgements beyond its contiguous upload position. An already emptied legacy directory without a sequence file also has no recoverable historical number. Resolve outstanding legacy tasks before upgrade and use a new instance name or Key prefix when historical numbers are unknown.
* Outstanding WAL must first be recovered using its original Block size, file size and Key prefix; configuration changes fail closed.
* Corrupt WAL is retained instead of uploading its valid prefix as a complete Block. Repair or manual handling is required. These offline tests do not replace real S3, process-kill or power-loss validation.
* Newly created `bucketMeta` files use V2 and a full-field CRC, including a 64-bit file size. Same-configuration legacy metadata remains readable without in-place migration. V2 cannot be handed back to older binaries; drain and back up before downgrading. Corrupt, truncated and unknown-version metadata now fail explicitly.

# 9. Maintenance Constraints

* `BlockMetaData` snapshots results under its lock and dispatches each Future on an independent virtual thread. Synchronous callbacks may close the Instance or await sibling Futures; never run them serially on the upload/recovery thread. Shutdown waits for storage work, not arbitrary user callbacks.
* Instance construction deep-copies and validates all configuration before starting threads, allocating native memory or creating files. Mutating the caller's configuration is not a live reconfiguration mechanism. Instance and Bucket names must be single path components.
* `.instance.lock` provides OS-level exclusivity for one local instance directory. Keep it held when shutdown must be retried; release only after resources stop, and never delete the lock file while running. This is not a distributed S3 namespace lock.
* V2 metadata keeps the 24-byte fixed header. Its first int is `0x424D0200 | dirty`; preserve the version bits when updating dirty. CRC covers all bytes of block size, file size, prefix length and prefix, excluding mutable dirty. Existing compatible files are remapped, not truncated; allowed configuration replacement is atomic after forcing a temporary file. Legacy CRC remains weak for compatibility.
* Manual recovery owns a read-only snapshot, uses the same whole-Block validation as startup recovery, and checks the accepted byte count. Reading/closing does not acknowledge a Block. Use `readAll()` for Value-only upload and acknowledge only after remote success.
* Recovered metadata may have missing indices because committed Blocks are skipped. File cleanup must cover all 1024 possible indices, not stop at the first missing entry.

Focused tests are `FutureNotificationRegressionTest`, `ConfigurationSafetyTest`, `InstanceDirectoryLockTest` (including a separate JVM), `BucketMetaInfoCompatibilityTest`, `ManualRecoveryReaderTest`, and `BlockMetadataCleanupTest`.


———————————————————————————————————————
## As an independent developer and open-source enthusiast, maintaining this project takes up a significant amount of my personal time. If you find this project helpful and would like to buy me a coffee, it would mean the world to me! Every contribution, no matter how small, is deeply appreciated and will be remembered. I'm committed to keeping this project alive and making it even better over time.
<img width="400" height="556" alt="image" src="https://github.com/user-attachments/assets/6b42a3ff-4923-4566-b26e-7dbd1a656dfe" />
