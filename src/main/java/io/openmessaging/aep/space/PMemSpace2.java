package io.openmessaging.aep.space;

import com.intel.pmem.llpl.Heap;
import io.openmessaging.aep.mmu.MemoryListNode;
import io.openmessaging.aep.mmu.MemoryNode;
import io.openmessaging.aep.pool.PMemUnitPool;
import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StorageSize;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * pmem存储管理使用了无锁内存池
 * @author tao */
public class PMemSpace2 implements Space2 {
    // append写入线程隔离map
    private ConcurrentHashMap<String, PMemThreadSpace2> threadSpaceMap = new ConcurrentHashMap<>();
    private Heap heap;
    private PMemUnitPool pool;
    private final long size;
    private byte stage = 1;

    public PMemSpace2(String path, long size) {
        this.size = size;
        boolean initialized = Heap.exists(path);
        heap = initialized ? Heap.openHeap(path) : Heap.createHeap(path, size);
        pool = new PMemUnitPool(heap);
    }

    @Override
    public void free(MemoryNode listNode) {
        threadSpaceMap.get(listNode.tName).free(listNode);
    }

    /**
     * 写入对应线程的ThreadSpace, 首次先进行创建，保证并发安全*/
    public MemoryNode write(ByteBuffer data, String tName) {
        PMemThreadSpace2 space = threadSpaceMap.get(tName);
        // 还未创建
        if (space == null) {
            space = new PMemThreadSpace2(pool);
            PMemThreadSpace2 tag = threadSpaceMap.putIfAbsent(tName, space);
            if (tag != null) {
                space = tag;
            }
        }
        MemoryNode node = space.write(data);
        if (node != null) {
            node.tName = tName;
        }
        return node;
    }

    @Override
    public MemoryNode write(ByteBuffer data) {
        return null;
    }

    @Override
    public byte[] read(MemoryNode listNode) {
        return threadSpaceMap.get(listNode.tName).read(listNode);
    }

    public ByteBuffer readDataAndFree(MemoryNode node) {
        byte[] data = read(node);
        threadSpaceMap.get(node.tName).free(node);
        ByteBuffer buffer = ByteBuffer.allocate(data.length);
        buffer.put(data);
        buffer.rewind();
        return buffer;
    }

    public void changeStage() {
        stage = 2;
    }

    public long getSize() {
        return size;
    }

}
