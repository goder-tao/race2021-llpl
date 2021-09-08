package test;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;

class Node {
    int i;
    Node(int i) {
        this.i = i;
    }
}

class NodeTurn {
    NodeTurn(Node node) {
        node.i = 5;
    }
}

public class test {
    public static void main(String[] args) {
        Map<Integer, Integer> map = new HashMap<>();
        map.put(1,1);
        Integer i = map.get(5);
        System.out.println(i);

    }
}
