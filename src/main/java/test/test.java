package test;

import io.openmessaging.dramcache.DRAMCache;

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
        final NodeTurn nodeTurn = new NodeTurn();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                initCache(nodeTurn);
            }
        }), thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                initCache(nodeTurn);
            }
        });
        try {
            thread.start();
            thread.join();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
        System.out.println("waiting");
    }
    static void initCache(NodeTurn node){
        node.initCache();
    }

}
