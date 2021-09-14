package io.openmessaging.aep.mmu;

import com.intel.pmem.llpl.MemoryBlock;

public interface MMU {

    void free(long handle);

    MemoryBlock allocate(long size);

    MemoryBlock getBlock(long handle);
}
