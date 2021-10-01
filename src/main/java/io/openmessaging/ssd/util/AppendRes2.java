package io.openmessaging.ssd.util;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

public class AppendRes2 {
    private MappedByteBuffer mmap;
    private long writeStartOffset;
    public  AppendRes2(MappedByteBuffer mmap, long writeStartOffset) {
        this.mmap = mmap;
        this.writeStartOffset = writeStartOffset;
    }

    public long getWriteStartOffset() {
        return writeStartOffset;
    }

    public MappedByteBuffer getMmap() {
        return mmap;
    }
}
