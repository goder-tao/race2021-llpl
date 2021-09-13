package io.openmessaging.aep.util;

import com.intel.pmem.llpl.Heap;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.aep.mmu.MemoryListNode;
import io.openmessaging.aep.mmu.PMemMMU2;
import io.openmessaging.constant.StorageSize;

import java.nio.ByteBuffer;

/**
 * 代表aep中的一块空间，创建一个size大小的heap，利用heap分配一个尽可能大的MemoryBlock
 * 称为mainBlock，以offset和size的方式使用llpl库读写mainBlock，实测能有将近于FS方式
 * 的两倍性能。包含一个MMU手动管理mainBlock的存储使用情况，分配写入的offset，再调用
 * PMemeReaderWriter根据offset操作mainBlock
 * @author tao */
public class PMemPartitionSpace implements Space{
    private final MemoryBlock mainBlock;
    private final long size;
    private final PMemMMU2 mmu;
    private final PMemReaderWriter readerWriter;
    private final byte partition;

    public PMemPartitionSpace(MemoryBlock memoryBlock, byte partition) {
       this.mainBlock = memoryBlock;
        mmu = new PMemMMU2(mainBlock.size());
        readerWriter = new PMemReaderWriter(mainBlock);
        this.size = mainBlock.size();
        this.partition = partition;
    }

    @Override
    public void free(MemoryListNode listNode) {
        mmu.free(listNode);
    }

    /**
     * MMU先分配一个handle，再调用readerWriter写入*/
    @Override
    public MemoryListNode write(byte[] data) {
        MemoryListNode listNode = mmu.allocate(data.length);
        if (listNode != null) {
            readerWriter.write(listNode.blockOffset, data);
            listNode.partiotion = partition;
        } else {

        }
        return listNode;
    }

    @Override
    public byte[] read(MemoryListNode listNode) {
        return readerWriter.read(listNode.blockOffset, (int) listNode.blockSize);
    }

    public ByteBuffer readDataAndFree(MemoryListNode memoryListNode) {
        ByteBuffer buffer;
        byte[] b = readerWriter.read(memoryListNode.blockOffset, (int) memoryListNode.blockSize);
        free(memoryListNode);
        buffer = ByteBuffer.allocate(b.length);
        buffer.put(b);
        buffer.rewind();
        return buffer;
    }

    public long getSize() {
        return size;
    }
}
