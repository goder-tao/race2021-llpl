package io.openmessaging.ssd.util;

import java.io.RandomAccessFile;

/**
 * 保存append中创建的raf和插入点off
 * @author tao */
public class AppendRes {
    private RandomAccessFile raf;
    private long writeStartOffset;
    public  AppendRes(RandomAccessFile raf, long writeStartOffset) {
        this.raf = raf;
        this.writeStartOffset = writeStartOffset;
    }

    public long getWriteStartOffset() {
        return writeStartOffset;
    }

    public RandomAccessFile getRaf() {
        return raf;
    }
}
