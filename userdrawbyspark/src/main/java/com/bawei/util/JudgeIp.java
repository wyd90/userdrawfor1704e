package com.bawei.util;

import org.apache.commons.lang.StringUtils;

public class JudgeIp {
    public static boolean ipCheck(String text) {
        if(!StringUtils.isEmpty(text)) {

            String regex = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
                    + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                    + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
                    + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";

            if(text.matches(regex)) {
                return true;
            } else {
                return false;
            }

        } else {
            return false;
        }
    }


//    public static void main(String[] args) {
//        System.out.println(JudgeIp.ipCheck(""));
//    }
}
