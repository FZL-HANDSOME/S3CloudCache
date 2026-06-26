package org.foreverfzl.cloudcache.wal.storefile;

import org.foreverfzl.cloudcache.wal.manager.MappedFileManager;
import org.foreverfzl.cloudcache.wal.storefile.WalDataStruct;
import org.foreverfzl.cloudchache.common.ProjectUtil;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;



public class DefaultMappedFileText {

    static void main() throws Exception {
//        textAppend();
    }


//    /**
//     * 测试文件的append和get和clean
//     */
//    private static void textAppend() {
////        MappedFileManager mappedFileManager = new MappedFileManager("order","instance1","11","textBucket",1);
////        (final String dirPath, final String fileName, final long fileFromOffset,
////                             final long fileSize, final int blockSize, MappedFileManager manager)
//        String dirPath=ProjectUtil.USER_HOME+ProjectUtil.WAL_FILE_ADDRESS+ File.separator+"instance1"+File.separator+"textBucket";
//        DefaultMappedFile defaultMappedFile = new DefaultMappedFile(dirPath,"0001",0,128L * 1024 * 1024,
//                8 * 1024 * 1024,false,false,mappedFileManager);
//        String value = "Forever";
//        WalDataStruct walDataStruct = new WalDataStruct(value.getBytes(StandardCharsets.UTF_8));
//        AppendMessageResult appendMessageResult = defaultMappedFile.appendData(walDataStruct);
//        long wroteOffset = appendMessageResult.getWroteOffset();
//        long wroteBytes = appendMessageResult.getWroteBytes();
//        WalDataStruct data = defaultMappedFile.getData(wroteOffset, wroteBytes);
//        String valueS = new String(data.getValueBytes(), StandardCharsets.UTF_8);
//        System.out.println(valueS);
//        System.out.println("111");
//    }

}

