package org.foreverfzl.cloudchache.common;

import org.foreverfzl.cloudchache.common.cloudcahceEnum.WalFileSize;

public class ProjectUtilText {

    static void main() {

    }

    private static void textName(String name){
        String aaa = ProjectUtil.generateUniqueS3Key(name);
        System.out.println(aaa);
    }

}
