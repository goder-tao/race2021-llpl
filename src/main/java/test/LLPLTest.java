package test;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StorageSize;


public class LLPLTest {
    public static void main(String[] args) {
        testLLPLLatency();

    }


    /**
     * 使用llpl库各部分的耗时测时*/
    static void testLLPLLatency() {
        Heap heap;
        String path = MntPath.AEP_PATH+"test";

        boolean initialized = Heap.exists(path);
        heap = initialized ? Heap.openHeap(path) : Heap.createHeap(path, StorageSize.MB*500);
        MemoryBlock block;

        for (int i = 0; i < 25; i++){
            long st = System.nanoTime();
            block = heap.allocateMemoryBlock((long) Math.pow(2, i));
            long t1 = System.nanoTime();
            System.out.println("allocate block spend: "+(t1-st)+"ns");
            block.copyFromArray(new byte[(int) Math.pow(2, i)], 0,0, (int) Math.pow(2, i));
            System.out.println("write block spend: "+(System.nanoTime()-t1)+"ns");
        }
    }
}