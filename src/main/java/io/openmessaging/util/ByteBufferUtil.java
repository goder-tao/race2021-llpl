package io.openmessaging.util;

import java.nio.ByteBuffer;

public class ByteBufferUtil {
    public static ByteBuffer copyFrom(ByteBuffer src) {
        src.rewind();
        ByteBuffer dst = ByteBuffer.allocate(src.remaining());
        src.rewind();
        dst.put(src);
        src.rewind();
        dst.rewind();
        return dst;
    }

    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.put("1234567890".getBytes());
        buffer.limit(5);
        ByteBuffer buffer1 = copyFrom(buffer);
        byte[] b = new byte[buffer1.capacity()];
        buffer1.get(b);
        System.out.println(new java.lang.String(b));
    }

}
