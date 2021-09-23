package io.openmessaging.ssd.aggregator;

/** 包装一条要刷盘的消息的所有必要内容
 * @author tao
 * @date 2021-09-21*/
public class Message4Flush {
    // 消息中的数据
    private byte[] data;
    // topic+queue+offset构成String的hash code
    private int hashKey;

    public Message4Flush(byte[] data, int hashKey) {
        this.data = data;
        this.hashKey = hashKey;
    }

    public byte[] getData() {
        return data;
    }

    public int getHashKey() {
        return hashKey;
    }
}
