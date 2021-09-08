package io.openmessaging.aep.util;

import io.openmessaging.aep.mmu.PMemMMU;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StorageSize;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;

/**
 * 一块指定大小可以直接操作的aep block*/
public class PmemBlock implements PmemWriter, PmemReader {
    private final PMemMMU pMemMMU;
    private Logger logger = LogManager.getLogger(PmemBlock.class.getName());
    private long size;
    public PmemBlock(String path, long size) {
        pMemMMU = new PMemMMU(path, size);
    }

    @Override
    public byte[] read(long handle) {
        MemoryBlock block = pMemMMU.getBlock(handle);
        if (block == null) return null;
        byte[] b = new byte[(int)block.size()];
        for (int i = 0; i < b.length; i++) {
            b[i] = block.getByte(i);
        }
        return b;
    }

    public byte[] readAndFree(long handle) {
        byte[] b = read(handle);
        free(handle);
        return b;
    }

    public ByteBuffer readData(long handle) {
        MemoryBlock block = pMemMMU.getBlock(handle);
        if (block == null) return null;
        ByteBuffer buffer = ByteBuffer.allocate((int)block.size());
        for (int i = 0; i < buffer.capacity(); i++) {
            buffer.put(block.getByte(i));
        }
        buffer.rewind();
        return buffer;
    }

    public ByteBuffer readDataAndFree(long handle) {
        ByteBuffer data = readData(handle);
        free(handle);
        return data;
    }

    @Override
    public long write(byte[] data) {
        MemoryBlock block;
        try {
            block = pMemMMU.allocate(data.length);
        } catch (Exception e) {
            logger.info("No enough pmem storage");
            return -1L;
        }
        for (int i = 0; i < data.length; i++) {
            block.setByte(i, data[i]);
        }
        block.flush();
        return block.handle();
    }

    public long writeData(ByteBuffer data) {
        data.rewind();
        MemoryBlock block;
        try {
            block = pMemMMU.allocate(data.capacity());
        } catch (Exception e) {
            logger.info("No enough pmem storage");
            return -1L;
        }
        for (int i = 0; i < data.capacity(); i++) {
            block.setByte(i, data.get());
        }
        block.flush();
        return block.handle();
    }

    public void free(long handle) {
        pMemMMU.free(handle);
    }

    public long getSize() {
        return size;
    }

    public static void main(String[] args) {
        PmemBlock block = new PmemBlock(MntPath.AEP_PATH+"test", StorageSize.MB*8);
        byte[] b = new byte[(int) (StorageSize.MB*8+1)];
        long handle;
        try {
            handle = block.write(b);
        } catch (Exception e) {
            System.out.println("allocate fail");
            return;
        }
        System.out.println(handle);
    }
}
