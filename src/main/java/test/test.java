package test;

import io.openmessaging.dramcache.DRAMCache;

import java.io.File;

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
       parallelDir();
    }
    static void initCache(NodeTurn node){
        node.initCache();
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
