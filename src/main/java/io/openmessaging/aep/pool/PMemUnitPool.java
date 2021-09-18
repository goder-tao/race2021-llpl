package io.openmessaging.aep.pool;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.constant.StorageSize;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 提前将Heap分成大小相同的MemoryBlock池化，通过池来完成
 * pmem空间的分配
 * @author tao */
public class PMemUnitPool implements Pool {
    Heap heap;
    // 使用一个并发队列保存空余unit
    private ConcurrentLinkedQueue<PMemUnit> unitQueue = new ConcurrentLinkedQueue<>();

    public PMemUnitPool(Heap heap) {
        this.heap = heap;
        while (true) {
            try {
                MemoryBlock block = heap.allocateMemoryBlock(StorageSize.DEFAULT_UNIT_SIZE);
                PMemUnit unit = new PMemUnit(block);
                unitQueue.add(unit);
            } catch (Exception e) {
                break;
            }
        }
    }

    @Override
    public PMemUnit allocate() {
        return unitQueue.poll();
    }

    @Override
    public void deAllocate(PMemUnit pMemUnit) {
        unitQueue.add(pMemUnit);
    }
}
