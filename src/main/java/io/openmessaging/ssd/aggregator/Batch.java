package io.openmessaging.ssd.aggregator;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 表示一批数据*/
public class Batch extends ConcurrentLinkedQueue<MessagePutRequest> {
    // 记录当前batch的大小
    private AtomicInteger batchSize = new AtomicInteger(0);
    // 标记当前队列是否可以flush
    private AtomicBoolean canFlush = new AtomicBoolean(false);
    public Batch() {

    }

    /**
     * 尝试向batch的尾部添加一个req，如果添加后的大小超过batch的默认大小返回false，
     * 否则将req加入queue并返回true*/
    public boolean tryToAdd(MessagePutRequest req) {
        if (req.getMessage().getData().length+batchSize.get() <= 8*1024) {
            super.add(req);
            batchSize.addAndGet(req.getMessage().getData().length);
            if (batchSize.get() == 8*1024) {
                canFlush.compareAndSet(false, true);
            }
            return true;
        }
        canFlush.compareAndSet(false, true);
        return false;
    }

    public int getBatchSize() {
        return batchSize.get();
    }

    public boolean isCanFlush() {
        return canFlush.get();
    }
}
