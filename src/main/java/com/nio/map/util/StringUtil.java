package com.nio.map.util;

import java.util.Map;

/**
 * Created by rain.chen on 2023/3/27
 **/
public class StringUtil {

    static public String templateStr(String str, Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            str = str.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return str;
    }

    static public String trimChar(String str, char c) {
        int len = str.length();
        int st = 0;
        char[] val = str.toCharArray();

        while ((st < len) && (val[st] == c)) {
            st++;
        }
        while ((st < len) && (val[len - 1] == c)) {
            len--;
        }
        return ((st > 0) || (len < str.length())) ? str.substring(st, len) : str;
    }
}
