package org.foreverfzl.cloudchache.common;

public class ProjectUtilText {

    static void main() {
        textName();
    }

    private static void textName(){
        String aaa = ProjectUtil.generateUniqueS3Key("order","instanceA","bucketA",20,1);
        System.out.println(aaa);
    }

}
