package io.openmessaging.ssd.aggregator;

import java.nio.ByteBuffer;

/** 包装一条要刷盘的消息的所有必要内容, directbuf复用版本
 * @author tao
 * @version 1.1
 * @date 2021-10-20*/
public class Message4Flush {
    // 消息中的数据
    private ByteBuffer data;
    // topic+queue+offset构成String的hash code
    private int hashKey;

//    public Message4Flush(byte[] data, int hashKey) {
    public Message4Flush(ByteBuffer data, int hashKey) {
        this.data = data;
        this.hashKey = hashKey;
    }

    public ByteBuffer getData(){
        data.rewind();
        return data;
    }

    public int getHashKey() {
        return hashKey;
    }
}
