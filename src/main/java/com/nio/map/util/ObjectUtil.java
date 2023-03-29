package com.nio.map.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rain.chen on 2023/3/27
 **/
public class ObjectUtil {
    static public String obj2Str(Object obj, String def) {
        try {
            return obj != null ? String.valueOf(obj) : def;
        } catch (Exception e) {
            return def;
        }

    }

    static public Long obj2Long(Object obj, Long def) {
        try {
            if (obj == null) {
                return def;
            } else if (obj instanceof Long) { //linkid = 9160000044288
                return (Long) obj;
            } else if (obj instanceof Integer) { //linkid = 49320313
                return ((Integer) obj).longValue();
            } else if (obj instanceof String) {
                return Long.valueOf((String) obj);
            } else {
                return def;
            }
        } catch (Exception e) {
            return def;
        }
    }

    static public Integer obj2Int(Object obj, Integer def) {
        try {
            return obj != null ? (Integer) obj : def;
        } catch (Exception e) {
            return def;
        }
    }

    static public List<Object> obj2List(Object obj) {
        try {
            if (obj != null) {
                List<Object> list = new ArrayList<>();
                for (Object oo : (List<?>) obj) {
                    list.add(oo);
                }
                return list;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }
}
