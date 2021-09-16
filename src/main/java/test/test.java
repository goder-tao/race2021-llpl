package test;

import io.openmessaging.dramcache.DRAMCache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentHashMap;

class Node {
    int i;

    Node(int i) {
        this.i = i;
    }
}

class NodeTurn {
    private DRAMCache cache = null;
    int flag = 0;

    void initCache() {
        if (flag == 0) {
            flag = 1;

        }
        if (cache == null) {
            System.out.println("Cache is null");
        }
    }
}


public class test {
    public static void main(String[] args) throws IOException {
//        RandomAccessFile file =  new RandomAccessFile("", "rw");
//        MappedByteBuffer mmap = file.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 1024);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName().split("-")[1]);
            }
        });
        thread.start();
    }
    static void initCache(NodeTurn node){
        node.initCache();
        for (int i = 0; i < 10000; i++) {
            test();
        }
    }

    public static void test() {
        ConcurrentHashMap<Integer, Integer> map = new ConcurrentHashMap<>();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                if (map.get(1) == null) {
                    if (map.putIfAbsent(1, 1) != null) {
                        System.out.println("!!!");
                    }
                }
            }
        }), thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                if (map.get(1) == null) {
                    if (map.putIfAbsent(1, 2) != null) {
                        System.out.println("!!!");
                    }
                }
            }
        });
        thread.start();
        thread1.start();
        try {
            thread.join();
            thread1.join();
        } catch (Exception e) {

        }
//        System.out.println(map.get(1));
    }
    static void parallelDir() {
        for(int i = 0; i < 100; i++) {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    File f = new File("/home/tao/Data/sim_SSD/test2");
                    if (!f.exists()) {
                        boolean b = f.mkdirs();
                        if (!b) {
                            System.out.println("Create dir fail!!");
                        }
                    }
                }
            });
            thread.start();
        }
    }
}
