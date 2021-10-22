package io.openmessaging.ssd.aggregator;

import io.openmessaging.constant.StorageSize;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 表示一批数据*/
public class Batch extends ConcurrentLinkedQueue<MessagePutRequest> {
    // 记录当前batch的大小
    private AtomicInteger batchSize = new AtomicInteger(0);

    // batch聚合上限， 根据第一个加入的req决定
    private int batchSizeLimit = StorageSize.SMALL_BATCH_SIZE;

    public Batch() {

    }

    /**
     * 尝试向batch的尾部添加一个req，如果添加后的大小超过batch的默认大小返回false，
     * 否则将req加入queue并返回true*/
    public boolean tryToAdd(MessagePutRequest req) {
        // 首个加入batch的req作为head，这个head决定这个batch的聚合上限, 8kb, 16kb or 24kb
//        if (this.isEmpty()) {
//            if (req.getMessage().getData().remaining() <= StorageSize.SMALL_BATCH_SIZE) {
//                batchSizeLimit = StorageSize.SMALL_BATCH_SIZE;
//            } else if (req.getMessage().getData().remaining() <= StorageSize.MIDDLE_BATCH_SIZE) {
//                batchSizeLimit = StorageSize.MIDDLE_BATCH_SIZE;
//            } else if (req.getMessage().getData().remaining() <= StorageSize.LARGE_BATCH_SIZE) {
//                batchSizeLimit = StorageSize.LARGE_BATCH_SIZE;
//            } else {
//                batchSizeLimit = (int) (StorageSize.MB);
//                System.out.println("exceed max batch limit size");
//            }
//        }

        // 需要聚合
        if (req.getMessage().getData().remaining()+batchSize.get() <= batchSizeLimit) {
            super.add(req);
            batchSize.addAndGet(req.getMessage().getData().remaining());
            return true;
        }

        // 如果batch为空且超过8kb直接刷盘
        if (batchSize.get() == 0) {
            super.add(req);
            batchSize.addAndGet(req.getMessage().getData().remaining());
        }
        return false;
    }

    public int getBatchSize() {
        return batchSize.get();
    }

    public boolean isCanFlush() {
        return batchSize.get() >= batchSizeLimit;
    }
}

/**
 * 多个Batch进行一次force，减少线程数量*/
class SuperBatch extends ArrayList<Batch> {
    private int superBatchSize = 0;

    public SuperBatch() {

    }

    public void AddBatch(Batch batch) {
        this.superBatchSize += batch.getBatchSize();
        super.add(batch);
    }

    public int getSuperBatchSize() {
        return superBatchSize;
    }
}
