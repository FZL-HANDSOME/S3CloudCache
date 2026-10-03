package org.foreverfzl.cloudchache.common;

/**
 * 中文：手动打印对象键的演示入口，不是 JUnit 用例，不写 WAL 或访问 S3。
 * English: Manual object-key printing demo, not a JUnit test; it neither writes WAL nor accesses S3.
 */
public class ProjectUtilText {

    /**
     * 中文：运行固定输入的 Key 格式示例；首次访问 ProjectUtil 会触发其平台初始化。
     * English: Runs a fixed-input key-format demo; first access to ProjectUtil triggers its platform initialization.
     */
    static void main() {
        textName();
    }

    /**
     * 中文：打印包含本机标识、实例、Bucket、文件编号和块索引的示例键，不做唯一性断言。
     * English: Prints a sample key containing machine, instance, Bucket, file identity, and block index without asserting uniqueness.
     */
    private static void textName(){
        String aaa = ProjectUtil.generateUniqueS3Key("order","instanceA","bucketA",20,1);
        System.out.println(aaa);
    }

}
