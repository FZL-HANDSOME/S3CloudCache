📖 README Languages
This README is currently available in Simplified Chinese.
For the English version, please refer to README.en.md.
# S3CloudCache

## 项目背景

S3CloudCache 是一个面向 S3 兼容对象存储的高性能缓存中间件，核心目标是在本地内存与云端对象存储之间构建一层高吞吐、低延迟、具备故障恢复能力的数据缓存层。

传统方式直接将大量小对象或高频写入请求同步写入 S3/MinIO，容易受到网络延迟、远端存储吞吐以及频繁小对象操作带来的开销影响。与此同时，如果仅依赖内存缓存，又会面临进程宕机导致数据丢失的问题。

因此，S3CloudCache 将数据写入过程拆分为本地高性能缓存与云端异步持久化两个阶段：数据首先通过 WAL 顺序写入本地磁盘，并进入基于内存映射与堆外内存的 Physical Block；当 Block 达到条件后，再异步批量上传至 S3/MinIO。即使运行过程中出现 Block 写入异常、进程重启或上传失败，也可以利用 WAL 对数据进行恢复，重新构建缓存 Block 并继续完成后续上传。

## 主要解决的问题

### 1. 高频小数据写入的性能问题

通过 WAL 顺序追加写、内存映射文件以及批量异步上传，减少频繁随机磁盘 I/O 与远程对象存储网络 I/O 带来的性能损耗，提高整体写入吞吐。

### 2. 本地缓存与云端存储之间的性能差异

采用“本地快速写入 + 后台异步上传”的方式，将业务写入路径与 S3 网络 I/O 解耦，避免业务线程直接受到远端存储延迟影响。

### 3. 进程异常导致的缓存数据丢失问题

引入 WAL 作为本地持久化兜底。当进程发生异常退出、物理缓存 Block 写入失败等情况时，可以根据 WAL 重新恢复数据，而不是完全依赖易失的内存缓存。

### 4. 高并发写入下的数据与位置一致性问题

通过原子写指针、Block 元数据以及 CAS 等并发控制机制，在多线程并发追加数据、Block 切换、上传位置推进等场景下保证数据状态和逻辑位置的正确性。

### 5. 物理缓存 Block 故障问题

将 WAL 与 Physical Block 解耦，使 Physical Block 成为可重建的数据副本。当 Physical Block 出现异常时，可以直接依据 WAL 重新构建，而不必让底层缓存故障直接导致持久化数据丢失。

### 6. 云端对象随机读取问题

上传后的 Block 以稳定的 `S3Key + offset + size` 描述数据位置，用户无需重新扫描对象即可通过 Range 请求读取指定数据片段。

### 7. 异步任务与优雅停机问题

通过异步上传、后台刷盘、元数据刷新以及优雅关闭机制，在系统停止时尽可能完成 WAL 刷盘与数据上传，并控制关闭超时时间，避免直接中断后台任务导致状态不一致。

---------------------------------------------------

# 项目架构

## 模块划分

| 模块 | 职责 |
|------|------|
| `s3cloudcache-common` | 公共配置、枚举、`WriteResult`、异常、工具类 |
| `s3cloudcache-core` | 堆外物理 Block 池 + S3 上传引擎 |
| `s3cloudcache-wal` | WAL 磁盘持久化（mmap 内存映射文件）与崩溃恢复 |
| `s3cloudcache-metadata` | Block 元数据与三大队列（上传 / 恢复 / 死信） |
| `s3cloudcache-storage`（对外 artifact：`s3cloudcache-instance`） | 对外 API 与 S3 适配 |

对外只依赖 `s3cloudcache-instance`，内部模块 `core`/`wal`/`metadata` 已通过 shade 打进该 jar 并重定位到 `internal.*` 包，使用者无法也不需要直接引用。

## 核心概念

- **逻辑 Block**：WAL 文件按 `blockSize` 切分出的逻辑单元，是封口 / 上传的基本单位。
- **物理 Block**：堆外内存中与某个逻辑 Block 1:1 绑定的缓冲块，数据在这里被聚合。
- **WAL（Write-Ahead Log）**：写前日志。宕机后由 WAL 恢复未上传的数据。
- **封口（Seal）**：一个 Block 写满、或超过空闲时间后，标记为「不再写入」，随后被上传到 S3。

## 数据写入链路

```
用户 write(data)
   │
   ├─ ① WAL 持久化：把数据按协议格式写入 文件（PageCache）
   │        └─ 按 blockSize 切分「逻辑 Block」，并为每个 Block 维护期望字节数 / 已落 PageCache 字节数
   │
   ├─ ② 写物理 Block：把数据零拷贝写入堆外 MemorySegment（与逻辑 Block 1:1 绑定）
   │        └─ 记录该数据最终归属的 S3 对象 key（s3Key）和块内偏移（offset）
   │
   └─ ③ 封口 + 上传：Block 写满或空闲超时后封口，异步上传到 S3
            └─ 上传成功后推进上传位点，WAL 文件可被安全删除
```

## 封口与上传时机

一个 Block 会在以下任一条件满足时封口并触发上传：

1. **写满**：下一条数据导致逻辑 Block 空间不足（跨块时自动封口上一个块）。
2. **空闲超时**：`blockMaxIdleTime` 内没有新数据写入（适合低频、尾块场景）。
3. **主动关闭**：调用 `instance.close(...)` 时把所有剩余 Block 封口上传。

## 容错机制

| 场景 | 处理 |
|------|------|
| 进程崩溃 / 宕机 | 重启时 `start()` 扫描 WAL，跳过已持久化上传确认的 Block；校验未确认 Block 的完整记录后恢复，损坏数据保留等待处理 |
| 物理 Block 写入失败 | 该 Block 标记 broken，后台线程从 WAL 重读该 Block 的原始数据并重写（恢复队列） |
| 上传失败（重试 3 次仍失败） | 进入死信队列，由用户通过 `getUpLoadFailedBlockInfo()` 读取并自愈 |
| 单条数据大于 blockSize | 直接拒绝并返回失败（`WriteResult.isSuccess() == false`） |

---

# 如何使用

## 1. 引入依赖(该依赖需要您自己下载项目并打包)

```xml
<dependency>
    <groupId>org.example</groupId>
    <artifactId>s3cloudcache-instance</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

> 该依赖会自动带入 `s3cloudcache-common`（配置/返回值）和 AWS SDK `s3` + `url-connection-client`。
> 内部实现模块不会出现在你的依赖树中。

## 2. 快速开始

```java
// 1. 创建 S3 客户端（连接你的 S3 / MinIO）
S3Client s3Client = S3ClientFactory.createS3Client(
        "http://127.0.0.1:9001",   // endpoint
        "cn-local",                // region
        "your-access-key",         // accessKey
        "your-secret-key"          // secretKey
);

// 2. 配置 bucket
BucketConfig bucketConfig = new BucketConfig()
        .setBlockSize(BlockSizeLevel.SMALL.getBytes())     // Block 大小 4MB
        .setCacheSize(512 * 1024 * 1024L)                  // 堆外缓冲区 512MB
        .setBlockUpLoadCount(BlockUploadConcurrencyLevel.NORMAL.getConcurrency())
        .setS3KeyPrefix("order/phone")                     // S3 对象 key 前缀
        .setWalFileSize(WalFileSize.SIZE_1G.getBytes());   // WAL 文件 1GB

// 3. 创建全局配置
S3CloudCacheConfig config = new S3CloudCacheConfig(
        "my-instance",   // instanceName：全局唯一，不要修改（影响恢复和 s3Key）
        null,            // walPath：null 则使用默认目录
        bucketConfig
);

// 4. 创建实例并启动（start 会做崩溃恢复）
S3CloudCacheInstance instance = new S3CloudCacheInstance(s3Client, config);
instance.start();

// 5. 获取某个 bucket 的写句柄
BucketWriterWriter writer = instance.getBucketWriterInstance("my-bucket");

// 6. 写数据
byte[] data = "hello s3cloudcache".getBytes(StandardCharsets.UTF_8);
CompletableFuture<WriteResult> future = writer.writeHeapData(data);
future.whenComplete((result, throwable) -> {
    if (throwable == null && result != null && result.isSuccess()) {
        System.out.println("s3Key  = " + result.getS3Key());
        System.out.println("offset = " + result.getOffset());
        System.out.println("size   = " + result.getSize());
    }
});

// 7. 优雅关闭（等待所有 Block 上传完成）
instance.close(15000, 15000, 15000);
```

## 3. 配置详解

### 3.1 S3CloudCacheConfig（全局配置）

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `instanceName` | String | 无（必填） | 实例名，全局唯一且**不要修改**，参与数据恢复和 s3Key 生成 |
| `walPath` | String | null | WAL 持久化目录；null 时用 `user.home/CloudCache/store` |
| `blockMaxIdleTime` | Integer | 20000（ms） | 一个 Block 空闲多久后自动封口上传（可理解为尾块的最大延迟） |
| `defaultBucketConfig` | BucketConfig | 无 | 默认 bucket 配置 |
| `specialBuckets` | Map\<String, BucketConfig\> | 空 | 特殊 bucket 的覆盖配置（未配置的字段继承默认） |

### 3.2 BucketConfig（Bucket 级配置）

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `s3KeyPrefix` | String | 无 | 生成 s3Key 时的用户自定义前缀（如 `order/phone`） |
| `walFileSize` | Long | 1GB | 单个 WAL 文件大小 |
| `cacheSize` | Long | 256MB | 该 bucket 的堆外缓冲区总大小（Block 池） |
| `blockSize` | Integer | 4MB | 单个 Block 大小（逻辑 Block 与物理 Block 一致） |
| `blockUpLoadCount` | Integer | 8 | 并发上传 Block 数量 |
| `isWarmWalFile` | Boolean | true | 是否预热 WAL 文件 PageCache |
| `isLockMappedFilePageCache` | Boolean | false | 是否锁定映射内存（防止换页） |
| `enableHeadCheck` | Boolean | false | 上传后是否额外做一次 headObject 校验 |
| `flushFileMetaInfoTime` | Integer | 5000（ms） | 文件元数据（read/upLoad 位点）刷新间隔 |
| `chackMappedFileTime` | Integer | 10000（ms） | 检查并删除可清理 WAL 文件的间隔 |

### 3.3 枚举取值

**BlockSizeLevel（Block 大小）**

| 枚举 | 大小 |
|------|------|
| TINY | 2MB |
| SMALL | 4MB |
| MEDIUM | 8MB |
| LARGE | 16MB |
| ULTRA | 32MB |

**WalFileSize（WAL 文件大小）**

| 枚举 | 大小 |
|------|------|
| SIZE_256MB | 256MB |
| SIZE_512MB | 512MB |
| SIZE_1G | 1GB |
| SIZE_2G | 2GB |

**CacheSizeLevel（堆外缓冲区大小）**

| 枚举 | 大小 |
|------|------|
| TINE | 256MB |
| LIGHTWEIGHT | 512MB |
| STANDARD | 1GB |
| HIGH_THROUGHPUT_2G | 2GB |
| HIGH_THROUGHPUT_4G | 4GB |

**BlockUploadConcurrencyLevel（上传并发）**

| 枚举 | 并发数 |
|------|--------|
| LOW | 4 |
| NORMAL | 8 |
| HIGH | 16 |
| ULTRA | 32 |

> 建议：`cacheSize` 要能容纳至少一个完整 WAL 文件的 Block 数（`cacheSize / blockSize` 个 Block），避免写入阻塞等待回收。

## 4. API 说明

### 4.1 S3ClientFactory（创建 S3 客户端）

```java
// 默认配置：pathStyle=true, 连接超时 10s, 重试 3 次
S3Client c = S3ClientFactory.createS3Client(endpoint, region, accessKey, secretKey);

// 自定义
S3Client c2 = S3ClientFactory.createS3Client(
        endpoint, region, accessKey, secretKey,
        true,                       // pathStyleAccess
        Duration.ofSeconds(20),     // connectionTimeout
        5                           // retryCount
);
```

### 4.2 S3CloudCacheInstance（实例入口）

| 方法 | 说明 |
|------|------|
| `start()` | 启动实例并做崩溃恢复 |
| `getBucketWriterInstance(String bucketName)` | 获取指定 bucket 的写句柄（不存在则自动创建） |
| `close(long walWriteWaitTime, long blockWriteWaitTime, long upLoadWaitTime)` | 优雅关闭：等 WAL 写完 → 等 Block 写完 → 封口并等全部上传 |
| `s3RawPutObject(bucket, key, MemorySegment)` | 用原始 S3Client 直接上传一个内存段 |

### 4.3 BucketWriterWriter（写句柄）

| 方法 | 说明 |
|------|------|
| `writeHeapData(byte[] data)` | 写入整个堆内数组 |
| `writeHeapData(byte[] data, long offset, long length)` | 写入数组 `[offset, offset+length)` 区间（避免二次裁剪） |
| `writeOffHeapData(ByteBuffer buffer)` | 写入 DirectByteBuffer 的 `[position, limit)`（零拷贝，不推进 position） |
| `writeOffHeapData(ByteBuffer buffer, long offset, long length)` | 写入 buffer 的 `[position+offset, position+offset+length)` |
| `getUpLoadFailedBlockInfo()` | 阻塞读取一条「上传失败」的死信数据，返回 `MappedFileReader` |

以上 `write*` 方法均返回 `CompletableFuture<WriteResult>`。

## 5. 返回对象 WriteResult

```java
public class WriteResult {
    String getS3Key();   // 这批数据最终归属的 S3 对象 key（如 order/phone/xxx.block）
    long   getOffset();  // 这条数据在该对象（Block）内部的绝对起始字节偏移量
    int    getSize();    // 这条数据的总长度（字节）
    boolean isSuccess(); // 是否写入成功
}
```

**语义说明**：

- `isSuccess() == true`：所在 Block 已成功上传到 S3，且本地上传确认已持久化；此时返回的 `s3Key + offset + size` 才是已提交位置。
- `isSuccess() == false` 或 Future 异常完成：写入未被确认成功。上传失败时保留未确认 WAL，供重启或人工恢复；这不表示远端一定不存在对象。
- 一个 `s3Key` 对应一个 Block；同一 Block 内的多条数据共享同一个 `s3Key`，用 `offset` 区分。
- 上传是异步的，但 Future 的成功完成会等待上传确认；尾块可能需要等空闲封口或 `instance.close(...)`，不要在提交每条记录后立即等待它完成来做吞吐测试。
- Block 是最小提交单位。未确认 Block 在恢复时可以重排，已确认 Block 不再按 WAL 顺序重建覆盖。
- 只要该s3Key有一个数据isSuccess==false则整个block中的数据作废，因为该项目是以block为基础单位的。

## 6. 如何用返回内容查询数据

数据是按 `s3Key + offset + size` 定位的。查询时用 **HTTP Range GET** 精确读回这段字节：

```java
String s3Key  = result.getS3Key();
long   offset = result.getOffset();
int    size   = result.getSize();

// 精确读取 bytes=offset-(offset+size-1)
String range = "bytes=" + offset + "-" + (offset + size - 1);

GetObjectRequest req = GetObjectRequest.builder()
        .bucket("my-bucket")   // 你写入时用的 bucket
        .key(s3Key)
        .range(range)
        .build();

try (ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(req)) {
    byte[] readBack = stream.readAllBytes();   // 就是你当初写入的原始字节，长度 == size
    // 逐字节校验即可
}
```

> 完整读回校验示例可参考项目内 `S3CloudCacheInstanceText` 中的 `dataIntegrityTest` / `highConcurrencyTest`。

## 7. 上传失败自愈（死信队列）

当一个 Block 上传重试 3 次仍失败时，会进入**死信队列**。用户通过 `getUpLoadFailedBlockInfo()` 读取并自行处理：

```java
BucketWriterWriter writer = instance.getBucketWriterInstance("my-bucket");

// 阻塞取得失败块的独立快照；必须关闭 Reader，释放它拥有的堆外内存。
try (MappedFileReader reader = writer.getUpLoadFailedBlockInfo()) {
    // readAll 是整块所有 Value Bytes，不含 WAL 协议头、对齐字节或尾部填充。
    MemorySegment values = reader.readAll();
    var response = instance.s3RawPutObject(reader.getBucketName(), reader.getS3Key(), values);
    if (response.eTag() == null || response.eTag().isBlank()) {
        throw new IllegalStateException("上传结果未通过确认，不推进 WAL 位点");
    }
    reader.ackUpLoadPosition(); // 仅上传成功后确认；Reader.close() 不会自动确认。
}
```
> 注意：`ackUpLoadPosition()` 必须在上传成功后调用，否则该文件的删除逻辑会一直等待。
> `hasNext()/next()` 可逐条读 Value；`readAll()` 不受当前游标影响，每次都表示整块。`getMemorySegment()` 是包含协议头的原始只读快照，不能把它当作纯 Value 对象上传。所有返回的堆外视图只在 Reader 关闭前有效；人工确认须在 Instance 关闭前完成。损坏或未完整写入的块会抛异常，不返回有效前缀冒充整块。

## 8. 数据完整性回归与升级边界

使用 JDK 25 或更高版本，在项目根目录运行 `mvn test`（依赖已缓存时可用 `mvn -o test`）。
`IntegrityRegressionTest` 调用 `S3CloudCacheInstanceText` 中的离线故障测试，配合 WAL 模块测试，验证并发写入、堆内/堆外切片、尾块关闭、上传失败与重启、失败物理块恢复以及已确认位置不被覆盖。测试使用独立临时目录和内存 S3 模拟器，不访问原有 WAL 或真实服务器；按记录 ID、长度、返回位置和逐字节内容核对。

- WAL 头部预留区增加逐块上传确认，Bucket 目录增加带校验的 `next-file-offset`；不要单独删除该编号文件，否则无法保证后续 Key 不复用。
- 旧版本没有逐块确认，无法识别旧连续上传位点之后已上传的块；旧目录如果 WAL 已清空且没有编号文件，也无法推断历史编号。升级前应先处理旧未完成任务，并为无法确定历史编号的目录使用新的实例名或 Key 前缀。
- 未完成 WAL 的 Block 大小、文件大小或 Key 前缀发生变化时，恢复拒绝继续；先用原配置恢复完成再调整配置。
- 损坏 WAL 不上传有效前缀冒充整块，也不静默删除；需要修复或人工处理。离线回归不等于真实 S3、进程强杀或机器断电验证，生产部署仍需补充这些测试。
- 新建 `bucketMeta` 使用 V2 标记及完整字段 CRC，支持 2 GiB 及更大文件尺寸。相同配置的旧元数据仍按旧格式读取且不原地升级；V2 不支持直接交给旧程序降级读取，降级前应先排空并备份。CRC 错误、短文件、未知版本都会拒绝启动，不再当作“没有数据”。

## 9. 维护约束与本轮修复定位

| 位置 | 原问题与现在的约束 | 回归测试 |
| --- | --- | --- |
| `BlockMetaData.notifyFutures` | `complete()` 会同步执行用户回调；串行通知会让 A 等 B 卡住，上传线程中的 close 也会等自身。现在锁内快照结果、锁外每个 Future 独立虚拟线程通知。 | `FutureNotificationRegressionTest` |
| `S3CloudCacheConfig` / `BucketConfig` | 原配置可变且浅共享。实例在任何目录/线程/内存分配之前深拷贝、校验默认及特殊配置。运行中修改调用方配置不再热更新现有实例。 | `ConfigurationSafetyTest` |
| `S3CloudCacheInstance.InstanceDirectoryLock` | 同目录多个实例会并发恢复/写相同 WAL。构造时取得 OS 独占锁，全部资源关闭后释放；关闭超时保留锁供重试，锁文件不删除。 | `InstanceDirectoryLockTest`，含独立 JVM 检查 |
| `BucketMetaInfo` / `BucketMetaInfoUtil` | 旧 CRC 只覆盖数值低 8 位，且 2 GiB 转 int 溢出。新建 V2 校验完整 4+8+4 字节字段及前缀；旧格式兼容。相同配置重开不截断，允许的配置替换通过临时文件 force 后原子替换。 | `BucketMetaInfoCompatibilityTest` |
| `MappedFileReader` / `WalBlockReader` | 原人工读取没有完整 CRC/长度验证，原始映射也可能在确认后被卸载。现在用只读独立快照和共用整块解析，校验实际字节数与 expectedBytes，显式释放内存。 | `ManualRecoveryReaderTest` |
| `BlockMetaDataManager.deleteFileAllBlockMetaData` | 恢复跳过已上传块后索引可能不连续。文件清理遍历完整 10-bit 索引范围，不能碰到第一个缺失索引就返回。 | `BlockMetadataCleanupTest` |

维护时尤其不要破坏以下约束：

1. `Future` 成功仍然意味着 S3 已确认；结果通知与用户回调执行是另一层。不要让上传/恢复线程等待用户回调，也不要依赖回调顺序或回调线程身份。`close()` 等存储任务结束，不等待用户回调全部结束。
2. `expectedBytes` 表示 WAL 已接受的数据量，不等同于“当前已经复制完的数据量”。人工恢复不能把尚未写完的全零尾部当成完整块；确认前块必须停止接收新数据。
3. `.instance.lock` 保护同一本地实例目录，不是分布式 S3 键空间锁。不同目录/机器仍应使用唯一实例名或 Key 命名空间；不要手工删除运行中的锁文件。
4. V2 首个 int 为 `0x424D0200 | dirty`。改变 dirty 时保留版本位；配置 CRC 不包含可变 dirty。旧文件的低 8 位 CRC 仅为兼容保留，不具备 V2 的完整字段保护。
5. `MappedFileReader.close()` 只释放快照；`ackUpLoadPosition()` 才确认 WAL。二者不能合并，上传失败、读取失败或用户取消时必须保留未确认数据。


———————————————————————————————————————
## 作为一名独立开发者/开源爱好者，维护这个项目会占用大量的休息时间。如果你觉得这个项目对你有帮助，并且愿意请我喝杯咖啡，那将是对我极大的鼓励！打赏无论多少，我都将铭记于心。我也承诺会持续维护下去，让项目变得更好。
<img width="400" height="556" alt="image" src="https://github.com/user-attachments/assets/6b42a3ff-4923-4566-b26e-7dbd1a656dfe" />
