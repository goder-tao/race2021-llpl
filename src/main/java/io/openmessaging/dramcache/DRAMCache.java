package io.openmessaging.dramcache;

import io.openmessaging.constant.StorageSize;
import io.openmessaging.util.SystemMemory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DRAMCache {
    // 定时监视内存情况
    private Thread updateAvailableMemory;
    // key为topic+qid组合
    private final ConcurrentHashMap<String, Map<Long, ByteBuffer>> cacheMap = new ConcurrentHashMap<>();
    // 限定一个内存的下限值，应对未知的情况
    private final long memoryDownThreshold = StorageSize.MB * 500;
    // 当前内存
    private long currentMemory = 0L;
    // 内存监视频率
    private final int freq = 1000;

    public DRAMCache() {
        updateAvailableMemory = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    currentMemory = SystemMemory.getSystemAvailableMemory();
                    try {
                        Thread.sleep(freq);
                    } catch (Exception e) {
                        System.out.println(e.toString());
                    }
                }
            }
        });
        updateAvailableMemory.start();
    }

    public void put(String topicAndQId, long off, ByteBuffer v) {
        v.rewind();
        Map<Long, ByteBuffer> map = getOrPutDefault(cacheMap, topicAndQId, new ConcurrentHashMap<>());
        map.put(off, v);
    }
    public ByteBuffer getAndRemove(String topicAndQId, long off) {
        Map<Long, ByteBuffer> map = getOrPutDefault(cacheMap, topicAndQId, new ConcurrentHashMap<>());
        ByteBuffer data = map.get(off);
        map.remove(off);
        return data;
    }
    public void remove(String topicAndQId, long off) {
        Map<Long, ByteBuffer> map = getOrPutDefault(cacheMap, topicAndQId, new ConcurrentHashMap<>());
        map.remove(off);
    }

    public boolean isCacheAvailable() {
        return currentMemory > memoryDownThreshold;
    }

    private <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue){
        V retObj = map.get(key);
        if(retObj != null){
            return retObj;
        }
        map.put(key, defaultValue);
        return defaultValue;
    }
}
