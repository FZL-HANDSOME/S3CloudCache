package org.foreverfzl.cloudchache.common;

import org.foreverfzl.cloudchache.common.cloudcahceEnum.WalFileSize;

public class ProjectUtilText {

    static void main() {
        textName("fzl");
    }

    private static void textName(String name){
        String aaa = ProjectUtil.generateUniqueS3Key(name);
        System.out.println(aaa);
    }
}
