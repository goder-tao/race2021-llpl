package io.openmessaging.aep.mmu;

import com.intel.pmem.llpl.MemoryBlock;
import com.intel.pmem.llpl.Heap;

public class PMemMMU  implements MMU{
    private Heap heap;

    public PMemMMU(String path, long size) {
        boolean initialized = Heap.exists(path);
        heap = initialized ? Heap.openHeap(path) : Heap.createHeap(path, size);
    }
    @Override
    /**
     * get MemoryBlock by its handle*/
    public MemoryBlock getBlock(long handle) {
        return heap.memoryBlockFromHandle(handle);
    }

    @Override
    public void free(long handle) {
        MemoryBlock block = heap.memoryBlockFromHandle(handle);
        block.free(false);
    }

    @Override
    public MemoryBlock allocate(long size) {
        return heap.allocateMemoryBlock(size);
    }

}
