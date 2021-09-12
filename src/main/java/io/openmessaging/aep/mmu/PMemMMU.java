package io.openmessaging.aep.mmu;

import com.intel.pmem.llpl.MemoryBlock;
import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.Range;

public class PMemMMU  implements MMU{
    private final Heap heap;

    public PMemMMU(String path, long size) {
        boolean initialized = Heap.exists(path);
        heap = initialized ? Heap.openHeap(path) : Heap.createHeap(path, size);
    }

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

    public MemoryBlock allocateAndWrite(byte[] data) {
        return heap.allocateMemoryBlock(data.length, (Range range) -> {
            range.copyFromArray(data,
                    0,
                    0,
                    data.length);
        });
    }

}
