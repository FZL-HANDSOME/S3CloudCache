package org.foreverfzl.cloudchache.common;

import org.foreverfzl.cloudchache.common.cloudcahceEnum.WalFileSize;

public class ProjectUtilText {

    static void main() {
        textName();
    }

    private static void textName(){
        String aaa = ProjectUtil.generateUniqueS3Key("order","instanceA","bucketA","00000002",1);
        System.out.println(aaa);
    }

}
