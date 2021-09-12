package test;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.constant.StorageSize;
import io.openmessaging.manager.Manager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class LatencyTest {
    public static void main(String[] args) {
        Manager manager = new Manager();
        testLatency(manager);
//        try {
//            testLLPLAndFS();
//        } catch (Exception e) {
//            System.out.println(e.toString());
//        }

        System.out.println();
    }

    /**
     * 测试以llpl memoryblock和file的方式去操作aep的性能差异*/
    static void testLLPLAndFS() throws IOException {
        String aepPath = "/mnt/sim_pmem/";
        String space = "test";
        String path = aepPath+space;
        int T = 3000;
        long t = 0;
        long sumT = 0;
        int bSize = (int) (8* StorageSize.KB);
        byte[] b = new byte[bSize];

        boolean isExisted = Heap.exists(path);
        Heap heap = isExisted ? Heap.openHeap(path) : Heap.createHeap(path, 500*StorageSize.MB);
        MemoryBlock memoryBlock = heap.allocateMemoryBlock(496*StorageSize.MB);
        RandomAccessFile file = new RandomAccessFile(aepPath+"test1", "rw");

        for (int i = 0; i < T; i++) {
            t = System.nanoTime();
            file.seek(i*bSize);
            file.write(b);
            sumT += System.nanoTime()-t;
            //System.out.println("file write time: "+(System.nanoTime()-t));
        }
        System.out.println("file write average time: "+(sumT/T));
        System.out.printf("\n\n\n\n");
        sumT = 0;

        for (int i = 0; i < T; i++) {
            t = System.nanoTime();
            memoryBlock.copyFromArray(b, 0, i*bSize, b.length);
            sumT += System.nanoTime()-t;
            // System.out.println("llpl write time: "+(System.nanoTime()-t));
        }
        System.out.println("llpl write average time: "+(sumT/T));
    }

    /**
     * 测试append各部分时延*/
    static void testLatency(Manager manager) {
        for (int i = 0; i < 100; i++) {
            ByteBuffer b = ByteBuffer.allocate((int) (StorageSize.KB*8));
            manager.append("test", 0, b);
        }
        System.out.printf("Spend time summary - map time: %f%%, pmem io: %f%%, hdd io: %f%%\n",
                (double)manager.sumMapTime.get()/ manager.sumAppendTime.get(), (double)manager.sumPMemIO.get()/manager.sumAppendTime.get(),
                (double)manager.sumDiskIO.get()/manager.sumAppendTime.get());
    }

}
