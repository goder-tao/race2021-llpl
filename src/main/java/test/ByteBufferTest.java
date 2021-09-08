package test;

import java.nio.ByteBuffer;
class ByteBufferTester{
    ByteBuffer buffer;

    ByteBufferTester(){
        buffer = ByteBuffer.allocate(1024);
        buffer.putInt(0, 4);
        System.out.println("----------------buffer info-------------");
        System.out.println("buffer capacity:" + buffer.capacity());
        System.out.println("buffer limit" + buffer.limit());
        System.out.println("buffer position:"+buffer.position());
        byte[] data = new byte[1024];
        System.out.println("data:" + buffer.getInt());
    }
}

