package io.openmessaging.util;

import java.util.Map;

public class MapUtil {
    /**
     * 并发安全的判空之后put方法*/
    public static   <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue){
        V presentV = map.get(key);
        if (presentV == null) {
               V put = map.putIfAbsent(key, defaultValue);
               if (put != null) {
                   presentV = put;
               } else {
                   presentV = defaultValue;
               }
        }
        return presentV;
    }
}
