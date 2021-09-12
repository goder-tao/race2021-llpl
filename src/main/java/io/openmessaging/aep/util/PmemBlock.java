package io.openmessaging.aep.util;

import io.openmessaging.aep.mmu.PMemMMU;
import com.intel.pmem.llpl.MemoryBlock;
import io.openmessaging.constant.MntPath;
import io.openmessaging.constant.StorageSize;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 一块指定大小可以直接操作的aep block*/
public class PmemBlock implements PmemWriter, PmemReader {
    private final PMemMMU pMemMMU;
    private final Logger logger = LogManager.getLogger(PmemBlock.class.getName());
    private final long size;
    public PmemBlock(String path, long size) {
        pMemMMU = new PMemMMU(path, size);
        this.size = size;
    }

    @Override
    public byte[] read(long handle) {
        MemoryBlock block = pMemMMU.getBlock(handle);
        if (block == null) return null;
        byte[] b = new byte[(int)block.size()];

        block.copyToArray(0, b, 0, b.length);

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
        byte[] b = new byte[(int) block.size()];
        block.copyToArray(0, b, 0, b.length);
        buffer.put(b);
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

        block.copyFromArray(data, 0, 0, data.length);

        block.flush();
        return block.handle();
    }

    public long writeData(ByteBuffer data) {
        data.rewind();
        MemoryBlock block;
        long sTime, allTime, copyTime, flushTime;
        sTime = System.nanoTime();
        try {
            block = pMemMMU.allocate(data.capacity());
            allTime = System.nanoTime();
        } catch (Exception e) {
            logger.info("No enough pmem storage");
            return -1L;
        }

        block.copyFromArray(data.array(), 0, 0, data.capacity());
        copyTime = System.nanoTime();

        block.flush();
        flushTime = System.nanoTime();

        System.out.printf("allocating time: %d, copy time: %d, flush time: %d\n",
                (allTime-sTime), (copyTime-allTime), (flushTime-copyTime));
        return block.handle();
    }

    public void free(long handle) {
        pMemMMU.free(handle);
    }

    public long getSize() {
        return size;
    }

    public static void main(String[] args) throws IOException {
        PmemBlock block = new PmemBlock(MntPath.AEP_PATH+"test", StorageSize.MB*80);
        int T = 2000;
        long sumTime = 0;
        // 使用 byte[]写
        byte b[] = new byte[(int) (StorageSize.KB*8)];
        ByteBuffer buffer = ByteBuffer.allocate((int) (StorageSize.KB));

        for (int i = 0; i < T; i++) {
            long t = System.nanoTime();
            block.writeData(buffer);
            sumTime += System.nanoTime()-t;
            System.out.println("Write time: "+(System.nanoTime()-t)+"ns");
        }

        // 使用ByteBuffer写
//        for (int i = 0; i < T; i++) {
//            long t = System.nanoTime();
//            block.writeData(buffer);
//            sumTime += System.nanoTime()-t;
//            System.out.println("Write time: "+(System.nanoTime()-t)+"ns");
//        }

        System.out.println("Average write time: "+(sumTime/T)+"ns");
    }
}
