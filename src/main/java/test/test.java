package test;

import io.openmessaging.dramcache.DRAMCache;

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
            cache = DRAMCache.createOrGetCache();
        }
        if (cache == null) {
            System.out.println("Cache is null");
        }
    }
}


public class test {
    public static void main(String[] args) {
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

}
