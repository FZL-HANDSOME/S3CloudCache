package org.foreverfzl.cloudcache.wal.storefile;


public class DefaultMappedFileText {


    static void main() throws Exception {



    }


//    /**
//     * 测试文件的append和get和clean
//     */
//    private static void textAppend() {
////          public MappedFileManager(String prefix, String dirPath, String instanceName, String bucketName, long pageFlushTime,
////                             long fileSize, int blockSize) {
//        MappedFileManager mappedFileManager = new MappedFileManager("order",
//                ProjectUtil.USER_HOME+File.separator+"ClouCache"+File.separator+"store"+File.separator+"instance1"+File.separator+"textBucket",
//                "instance1","textBucket",10,1024*2024,1024*1024);
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

//    private static void textPrefix(){
//        MappedFileManager mappedFileManager = new MappedFileManager("order",
//                ProjectUtil.USER_HOME+File.separator+"CloudCache"+File.separator+"store"+File.separator+"instance1"+File.separator+"textBucket",
//                "instance1","textBucket",10,1024*2024,1024*1024);
//        System.out.println("完成");
//    }

}

