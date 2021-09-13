package io.openmessaging.aep.util;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.aep.mmu.MemoryListNode;
import io.openmessaging.aep.mmu.PMemMMU2;
import io.openmessaging.constant.StorageSize;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 创建一个较大的空间的Heap，均分成固定大小相等的MemoryBlock作为partition，
 * 利用负载均衡算法（轮询）指定本次分配空间的partition，再由对应partition的
 * PMemPartitionSpace具体处理数据。PMemSpace只负责partition的创建以及请求
 * 的负载均衡
 * @author tao */
public class PMemSpace implements Space {
    private Heap heap;
    // 分区对应的PMemPartitionSpace
    private final PMemPartitionSpace[] partSpace;
    // 默认partition大小
    // 负载均衡的轮询指针, 保证并发
    private AtomicLong lbp = new AtomicLong(0);
    private final long size;

    public PMemSpace(String path, long size) {
        boolean initialized = Heap.exists(path);
        heap = initialized ? Heap.openHeap(path) : Heap.createHeap(path, size);
<<<<<<< HEAD
        mainBlock = heap.allocateMemoryBlock(size-10*StorageSize.MB);
        mmu = new PMemMMU2(mainBlock.size());
        readerWriter = new PMemReaderWriter(mainBlock);
        this.size = mainBlock.size();
=======
        this.size = heap.size();
        partSpace = new PMemPartitionSpace[(int) (size/StorageSize.DEFAULT_PARTITION_SIZE)];
        for (int i = 0; i < partSpace.length; i++) {
            partSpace[i] = new PMemPartitionSpace(heap.allocateMemoryBlock(StorageSize.DEFAULT_PARTITION_SIZE), (byte) i);
        }
>>>>>>> test
    }

    @Override
    public void free(MemoryListNode listNode) {
        partSpace[listNode.partiotion].free(listNode);
    }

    /**
     * 负载均衡，选择一个partition进行写入*/
    @Override
    public MemoryListNode write(byte[] data) {
        int p = (int) (lbp.getAndIncrement() % partSpace.length);
        return partSpace[p].write(data);
    }

    @Override
    public byte[] read(MemoryListNode listNode) {
        return partSpace[listNode.partiotion].read(listNode);
    }

    public ByteBuffer readDataAndFree(MemoryListNode memoryListNode) {
        return partSpace[memoryListNode.partiotion].readDataAndFree(memoryListNode);
    }

    public long getSize() {
        return size;
    }
}
