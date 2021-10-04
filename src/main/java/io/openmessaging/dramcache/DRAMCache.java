package io.openmessaging.dramcache;

import io.openmessaging.constant.StorageSize;
import io.openmessaging.util.MapUtil;
import io.openmessaging.util.SystemMemory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import sun.misc.Lock;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


public class DRAMCache {
    private final Logger logger;
    // 定时监视内存情况
    private Thread updateAvailableMemory;
    // key为topic+qid组合
    private ConcurrentHashMap<String, Map<Long, ByteBuffer>> cacheMap = new ConcurrentHashMap<>();
    // 限定一个内存的下限值，应对未知的情况
    private final long memoryDownThreshold = StorageSize.MB * 500;
    // 当前内存
    private long currentMemory = 0L;
    // 内存监视频率(100ms)
    private final int freq = 100;
    // 单次
    private AtomicBoolean startDetect = new AtomicBoolean(false);

    public DRAMCache() {
        currentMemory = SystemMemory.getSystemAvailableMemory();
        logger = LogManager.getLogger(DRAMCache.class.getName());
    }

    public void startDetect() {
        if (startDetect.compareAndSet(false, true)) {
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
    }

    public void put(String topicAndQId, long off, ByteBuffer v) {
        v.rewind();
        Map<Long, ByteBuffer> map = MapUtil.getOrPutDefault(cacheMap, topicAndQId, new HashMap<>());
        map.put(off, v);
    }

    public ByteBuffer getAndRemove(String topicAndQId, long off) {
        Map<Long, ByteBuffer> map = MapUtil.getOrPutDefault(cacheMap, topicAndQId, new HashMap<>());
        ByteBuffer data = map.get(off);
        map.remove(off);
        return data;
    }

    public void remove(String topicAndQId, long off) {
        Map<Long, ByteBuffer> map = MapUtil.getOrPutDefault(cacheMap, topicAndQId, new HashMap<>());
        map.remove(off);
    }

    public boolean isCacheAvailable() {
        return currentMemory > memoryDownThreshold;
    }

}
