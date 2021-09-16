package io.openmessaging.aep.util;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.aep.mmu.MemoryListNode;
import io.openmessaging.constant.StorageSize;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 创建一个较大的空间的Heap，建立当前线程和对应PmemThreadSpace的映射，将具体的操作下放到
 * 线程对应的PmemThreadSpace中去
 * @author tao */

public class PMemSpace implements Space {
    private Heap heap;
    // 线程及其空间的映射
    private ConcurrentHashMap<String, PmemThreadSpace> threadSpaceMap = new ConcurrentHashMap<>();
    private final long size;
    private byte stage = 1;

    public PMemSpace(String path, long size) {
        boolean initialized = Heap.exists(path);
        heap = initialized ? Heap.openHeap(path) : Heap.createHeap(path, size);
        this.size = heap.size();

    }

    @Override
    public void free(MemoryListNode listNode) {
        threadSpaceMap.get(listNode.tName).free(listNode);
    }

    /**
     * get Thread.name对应的PmemThreadSpace或者新建，写入下放*/
    @Override
    public MemoryListNode write(byte[] data) {
        MemoryListNode listNode = null;
        return listNode;
    }

    /**
     * Scheduler调度aep时使用的方法*/
    public MemoryListNode write(byte[] data, String tName) {
        MemoryListNode listNode;
        PmemThreadSpace threadSpace = threadSpaceMap.get(tName);
        if (threadSpace == null) {
            threadSpace = new PmemThreadSpace(heap, tName);
            threadSpace.setStage(this.stage);
            threadSpaceMap.put(tName, threadSpace);
        }
        listNode = threadSpace.write(data);
        if (listNode != null) {
            listNode.tName = tName;
        }
        return listNode;
    }

    @Override
    public byte[] read(MemoryListNode listNode) {
        return threadSpaceMap.get(listNode.tName).read(listNode);
    }

    public ByteBuffer readDataAndFree(MemoryListNode memoryListNode) {
        return threadSpaceMap.get(memoryListNode.tName).readDataAndFree(memoryListNode);
    }

    public long getSize() {
        return size;
    }
    
    public void changeStage() {
        stage = 2;
        for (String key:threadSpaceMap.keySet()) {
            threadSpaceMap.get(key).changeStage();
        }
    }
}
