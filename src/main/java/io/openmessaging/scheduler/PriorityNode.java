package io.openmessaging.scheduler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author tao
 * @date 2021-09-23*/
public class PriorityNode {
    // 当前队列在冷aep空间中保存的总消息的大小
    public AtomicLong queueDataSize;
    // 下一个不在aep中的offset
    public AtomicLong tailOffset;

    public AtomicBoolean isVerified = new AtomicBoolean(false);

    public PriorityNode() {
        queueDataSize = new AtomicLong(0);
        tailOffset = new AtomicLong(0);
    }
}
