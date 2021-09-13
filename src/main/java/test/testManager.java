package test;

import io.openmessaging.aep.util.PMemSpace;
import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StorageSize;
import io.openmessaging.dramcache.DRAMCache;
import io.openmessaging.manager.Manager;
import io.openmessaging.util.SystemMemory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class testManager {
    public static void main(String[] args) {

        testBlock();
        Manager manager = new Manager();
//        testParallelWrite(manager, "test", 0, 0, 40);
//        testSequentRead(manager, "test", 0, 0, 40);

        testSequentWrite(manager, "test", 0, 0, 40);
        testSequentWrite(manager, "test", 1, 0, 40);
        testParallelRead(manager, "test", 0, 0, 40, "test", 1, 20, 40);


    }

    /**
     * 测试PMemBlock(yes)*/
    static void testBlock() {
        PMemSpace block = new PMemSpace(MntPath.AEP_PATH+"test", StorageSize.COLD_SPACE_SIZE);
        Map<Integer, Long> mapData = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            ByteBuffer data = ByteBuffer.allocate(4);
            data.putInt(i);
            block.write(data.array());
        }
    }

    /**
     * 测试DRAMCache, 监视频率以及监视频率间隔期间能写入的内存多少
     * 通过(yes) -> 一次写入1MB的数据，100ms写入33MB*/
    static void testDRAMCache() {
        long memoryBefore = SystemMemory.getSystemAvailableMemory();
        long currentTime = System.currentTimeMillis();
        DRAMCache cache = DRAMCache.createOrGetCache();
        for(int i = 0; i < 10000; i++) {
           ByteBuffer data = ByteBuffer.allocate((int) StorageSize.MB);
           if (cache.isCacheAvailable()) {
               System.out.print(i+" ");
               cache.put("", i, data);
           }
           if (System.currentTimeMillis() - currentTime > 100) break;
        }
        System.out.println();

        System.out.println("Spend memory:"+(memoryBefore-SystemMemory.getSystemAvailableMemory())/StorageSize.MB+"MB");

    }

    static void testSequent(Manager manager, String topic, int qid, int s, int e) {
        testSequentWrite(manager, topic, qid, s, e);
        testSequentRead(manager, topic, qid, s, e);
    }

    /**
     * 串行读数据
     * 冷队列读(yes), range(0, 20)
     * 热队列读(yes), range(10, 20)*/
    static void testSequentRead(Manager manager, String topic, int qid, int s, int e) {
        Map<Integer, ByteBuffer> data = manager.getRange(topic, qid, s, e-s);
        System.out.println("Read data: {");
        for(int key : data.keySet()) {
            ByteBuffer b = data.get(key);
            System.out.println(Thread.currentThread().getName()+"  "+key+":"+b.getInt());
        }
        System.out.println("}");

    }


    /**
     * 串行写数据(yes)*/
    static void testSequentWrite(Manager manager, String topic, int qid, int s, int e) {
        for (int i = s; i < e; i++) {
            ByteBuffer data = ByteBuffer.allocate(4);
            data.putInt(i);
            manager.append(topic, qid, data);
        }
    }

    /**
     * 测试并行写(yes)*/
    static void testParallelWrite(Manager manager, String topic, int qid, int s, int e) {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                long offset;
                for (int i = s; i < s+(e-s)/2; i++) {
                    ByteBuffer data = ByteBuffer.allocate(4);
                    data.putInt(i);
                    offset = manager.append(topic, qid, data);
                    System.out.println(Thread.currentThread().getName()+" write data: "+i+", offset: "+offset);
                }
            }
        }), thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                long offset;
                for (int i = s+(e-s)/2; i < e; i++) {
                    ByteBuffer data = ByteBuffer.allocate(4);
                    data.putInt(i);
                    offset = manager.append(topic, qid, data);
                    System.out.println(Thread.currentThread().getName()+" write data: "+i+", offset: "+offset);
                }
            }
        });

        thread1.start();
        thread2.start();
        try {
            thread1.join();
            thread2.join();
        } catch (Exception ec) {
            System.out.println(ec.toString());
        }
    }

    /**
     * 测试并行读*/
    static void testParallelRead(Manager manager, String topic1, int qid1, int s1, int e1, String topic2, int qid2, int s2, int e2) {
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                testSequentRead(manager, topic1, qid1, s1, e1);
            }
        }), thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                testSequentRead(manager, topic2, qid2, s2, e2);
            }
        });
        thread1.start();
        thread2.start();
        try {
            thread1.join();
            thread2.join();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

}
