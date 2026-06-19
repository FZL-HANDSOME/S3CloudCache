package org.foreverfzl.cloudcache.wal.storefile;

import org.foreverfzl.cloudcache.wal.storefile.WalDataStruct;
import org.foreverfzl.cloudchache.common.ProjectUtil;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;



public class DefaultMappedFileText {

    static void main() throws Exception {
        System.out.println(ProjectUtil.USER_HOME);
    }


//    /**
//     * 测试文件的append和get和clean
//     */
//    private static void textAppend() {
//        DefaultMappedFile defaultMappedFile = new DefaultMappedFile("000001", 128L * 1024 * 1024);
//        String key = "FZL";
//        String value = "Forever";
//        WalDataStruct walDataStruct = new WalDataStruct(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
//        AppendMessageResult appendMessageResult = defaultMappedFile.appendData(walDataStruct);
//        long wroteOffset = appendMessageResult.getWroteOffset();
//        long wroteBytes = appendMessageResult.getWroteBytes();
//        WalDataStruct data = defaultMappedFile.getData(wroteOffset, wroteBytes);
//        String readKey = new String(data.getKeyBytes(), StandardCharsets.UTF_8);
//        String valueS = new String(data.getValueBytes(), StandardCharsets.UTF_8);
//        System.out.println(readKey + "   " + valueS);
//        defaultMappedFile.clean();
//    }
//
//    /**
//     * 测试多个线程同时写，简单测试，后面需要大测试
//     */
//    private static void testConcurrentWriteThenRead()
//            throws Exception {
//
//
//        DefaultMappedFile mappedFile =
//                new DefaultMappedFile(
//                        "000001",
//                        128L * 1024 * 1024
//                );
//
//        int threadCount = 3;
//        ExecutorService executor =
//                Executors.newFixedThreadPool(threadCount);
//
//        CountDownLatch startLatch =
//                new CountDownLatch(1);
//
//        CountDownLatch finishLatch =
//                new CountDownLatch(threadCount);
//
//        List<AppendMessageResult> results =
//                Collections.synchronizedList(
//                        new ArrayList<>()
//                );
//
//        List<WalDataStruct> sourceData =
//                Collections.synchronizedList(
//                        new ArrayList<>()
//                );
//
//        System.out.println("准备启动线程...");
//        for (int i = 0; i < threadCount; i++) {
//            int threadId = i;
//            executor.submit(() -> {
//                try {
//                    //等待同时开始
//                    startLatch.await();
//                    String key =
//                            "KEY_" + threadId;
//                    String value =
//                            "VALUE_FOREVER_" + threadId;
//                    WalDataStruct data =
//                            new WalDataStruct(
//                                    key.getBytes(StandardCharsets.UTF_8),
//                                    value.getBytes(StandardCharsets.UTF_8)
//                            );
//
//                    AppendMessageResult result = mappedFile.appendData(data);
//
//                    System.out.println(
//                            Thread.currentThread().getName()
//                                    +
//                                    " write offset="
//                                    +
//                                    result.getWroteOffset()
//                    );
//
//
//                    results.add(result);
//                    sourceData.add(data);
//
//
//                } catch (Exception e) {
//
//                    e.printStackTrace();
//
//                } finally {
//
//                    finishLatch.countDown();
//                }
//
//
//            });
//        }
//
//
//        // 发令
//        startLatch.countDown();
//
//
//        //等待全部写完
//        finishLatch.await();
//
//
//        System.out.println();
//        System.out.println("========== 写完成 ==========");
//
//
//        executor.shutdown();
//
//
//        // ============================
//        // 检查 offset 是否冲突
//        // ============================
//
//
//        Set<Long> offsetSet =
//                new HashSet<>();
//
//
//        for (AppendMessageResult r : results) {
//
//
//            if (!offsetSet.add(
//                    r.getWroteOffset()
//            )) {
//
//                throw new RuntimeException(
//                        "发现offset冲突:"
//                                +
//                                r.getWroteOffset()
//                );
//            }
//
//        }
//        System.out.println(
//                "offset检查通过"
//        );
//
//
//        // ============================
//        // 主线程统一读取
//        // ============================
//
//
//        System.out.println();
//        System.out.println("========== 开始读取 ==========");
//
//
//        for (int i = 0; i < results.size(); i++) {
//
//
//            AppendMessageResult result =
//                    results.get(i);
//
//
//            WalDataStruct expect =
//                    sourceData.get(i);
//
//
//            WalDataStruct actual =
//                    mappedFile.getData(
//                            result.getWroteOffset(),
//                            result.getWroteBytes()
//                    );
//            if (actual == null) {
//
//                throw new RuntimeException(
//                        "读取null offset="
//                                +
//                                result.getWroteOffset()
//                );
//            }
//            String expectKey =
//                    new String(
//                            expect.getKeyBytes(),
//                            StandardCharsets.UTF_8
//                    );
//            String expectValue =
//                    new String(
//                            expect.getValueBytes(),
//                            StandardCharsets.UTF_8
//                    );
//            String actualKey =
//                    new String(
//                            actual.getKeyBytes(),
//                            StandardCharsets.UTF_8
//                    );
//            String actualValue =
//                    new String(
//                            actual.getValueBytes(),
//                            StandardCharsets.UTF_8
//                    );
//
//
//            boolean ok =
//                    expectKey.equals(actualKey)
//                            &&
//                            expectValue.equals(actualValue);
//
//
//            System.out.println(
//                    "offset="
//                            +
//                            result.getWroteOffset()
//                            +
//                            " "
//                            +
//                            (ok ? "PASS" : "FAIL")
//                            +
//                            " expect="
//                            +
//                            expectKey
//                            +
//                            "="
//                            +
//                            expectValue
//                            +
//                            " actual="
//                            +
//                            actualKey
//                            +
//                            "="
//                            +
//                            actualValue
//            );
//            if (!ok) {
//
//                throw new RuntimeException(
//                        "数据不一致"
//                );
//            }
//        }
//        System.out.println();
//        System.out.println(
//                "======== 测试全部通过 ========"
//        );
//        mappedFile.clean();
//    }
}

