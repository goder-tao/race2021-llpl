package test;

import io.openmessaging.aep.util.PmemBlock;
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
        Manager manager = new Manager();
        testLatency(manager);

        System.out.println();
    }

    /**
     * 测试PMemBlock(yes)*/
    static void testBlock() {
        PmemBlock block = new PmemBlock(MntPath.AEP_PATH+"test", 16* StorageSize.MB);
        Map<Integer, Long> mapData = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            ByteBuffer data = ByteBuffer.allocate(4);
            data.putInt(i);
            long handle = block.writeData(data);
            mapData.put(i, handle);
        }
        for (int key:mapData.keySet()) {
            ByteBuffer data = block.readData(mapData.get(key));
            System.out.println(key+" : "+data.getInt());
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

    /**
     * 测试append各部分时延*/
    static void testLatency(Manager manager) {
        for (int i = 0; i < 100; i++) {
            ByteBuffer b = ByteBuffer.allocate((int) (StorageSize.KB*8));
            manager.append("test", 0, b);
        }
        System.out.printf("Spend time summary - map time: %f%%, pmem io: %f%%, hdd io: %f%%\n",
                (double)manager.sumMapTime.get()/ manager.sumAppendTime.get(), (double)manager.sumPMemIO.get()/manager.sumAppendTime.get(),
                (double)manager.sumDiskIO.get()/manager.sumAppendTime.get());
    }

    /**
     * 测试并行读写延时开两个不同的线程一个进行append工作一个进行getRange*/
    static void testParallelWriteAndRead(Manager manager, String topic, int qid, int s, int e) {

    }

    /**
     * 测试调度群scheduler*/
    static void testScheduler() {

    }

}
