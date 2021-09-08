package io.openmessaging.util;

import java.nio.ByteBuffer;

public class ByteBufferUtil {
    public static ByteBuffer copyFrom(ByteBuffer src) {
        ByteBuffer dst = ByteBuffer.allocate(src.capacity());
        src.rewind();
        dst.put(src);
        src.rewind();
        dst.rewind();
        return dst;
    }

    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(4);
        ByteBuffer b = copyFrom(buffer);
        System.out.println(b.getInt());
    }
}
