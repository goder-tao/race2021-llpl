package io.openmessaging.ssd.aggregator;

import io.openmessaging.ssd.util.SSDWriterReader3;
import io.openmessaging.ssd.util.IndexHandle;
import io.openmessaging.ssd.util.SSDWriterReader4;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * 向上聚合多个thread的数据，默认8kb聚合，向下调用底层
 * disk写接口写入一批数据，更新一批index并且通知thread
 * 刷盘已完成
 * @data 2021-09-21
 * @author tao
 * */
public class Aggregator implements Runnable {
    private volatile Batch batch = new Batch();
    // 等待落盘的batch队列
    private final ConcurrentLinkedQueue<Batch> flushBatchQueue = new ConcurrentLinkedQueue<>();
    private volatile CountDownLatch waitPoint = new CountDownLatch(1);
    private final IndexHandle indexHandle;
    private final Logger logger = LogManager.getLogger(Aggregator.class.getName());
    // 并发不同点位点位写的线程池
    private final ExecutorService writerPool = Executors.newFixedThreadPool(15);

    public Aggregator(IndexHandle indexHandle) {
        this.indexHandle = indexHandle;
        flushBatchQueue.offer(batch);
    }

    /**
     * 添加一个request，尝试向当前batch添加一个request, 如果添加后的结果
     * 超过8kb，则将原batch加入flush queue，新建一个batch添加当前request,
     * 并且通知刷盘程序刷盘*/
    public synchronized void putMessageRequest(MessagePutRequest req) {
        // 当前batch已经满了，需要进行刷盘
        if (!this.batch.tryToAdd(req)) {
            flushBatchQueue.offer(batch);
            batch = new Batch();
            batch.tryToAdd(req);
        } else if (batch.isCanFlush()) {
            flushBatchQueue.offer(batch);
            batch = new Batch();
        }
    }

    /**
     * 一直检查flush queue的队列头，有两种情况，一个是队列头batch已经可以
     * flush(检查标记)，取出头来flush，二个是flush标记还没激活，线程记录
     * head batch的大小并睡眠一段时间等待，睡眠过后对比head batch的新大小，
     * 如果没有变化说明已经没有再多的线程能够增加batch的大小了，先将当前batch
     * 进行持久化处理*/
    @Override
    public void run() {
        Batch head = null;
        while (true) {
            head = flushBatchQueue.peek();
            if (head != null) {
                // 启动一个task去刷盘
                writerPool.execute(this::doFlush);
            } else { //队列为空，但是batch不空，一种是没有再多的队列让这个batch再增大， 另一种是还在增长
                int batchSize = batch.getBatchSize();
                try {
                    Thread.sleep(1);
                } catch (Exception e) {
                    logger.error(""+e.toString());
                }
                // batch的大小没有变化, 已经不再能聚合线程，加入queue准备force
                if (batchSize == batch.getBatchSize()) {
                    flushBatchQueue.offer(batch);
                    // 新建另一个batch接收新的req
                    batch = new Batch();
                }
            }
        }
    }

    /**
     * 按照字段组合batch中的数据，调用disk接口进行append写入，生成
     * index更新，最终调用indexfile的mmap进行刷盘，接着通知所有request
     * 刷盘已经完成，异步任务完成*/
    private void doFlush() {
        long t = System.nanoTime();

        Batch flushBatch = flushBatchQueue.poll();
        if (flushBatch == null || flushBatch.isEmpty())  return;
        int off = 0, size;

        // 遍历batch中的每一个request
        ByteBuffer buffer = ByteBuffer.allocate(flushBatch.getBatchSize());
        for (MessagePutRequest msg : flushBatch) {
            size = msg.getMessage().getData().length;
            // put data
            buffer.put(msg.getMessage().getData(), 0, size);
            off += size;
        }

        // 刷盘 ->
        long sOff = SSDWriterReader4.getInstance().write(buffer.array());
        // 更新index
        for (MessagePutRequest msg : flushBatch) {
            indexHandle.newIndex(msg.getMessage().getHashKey(), sOff, (short) msg.getMessage().getData().length);
            sOff += msg.getMessage().getData().length;
        }
        // force index
        indexHandle.force();
        // 通知写入线程落盘完成
        for (MessagePutRequest msg:flushBatch) {
            msg.countDown();
        }
        System.out.println("flush time: "+(System.nanoTime()-t));
    }

    /**
     * 手动唤醒刷盘进程*/
    private void wakeUp() {
        waitPoint.countDown();
    }

    /**
     * 设置一个刷盘等待时间，以防多个线程的data batch无法达到设定的batch最大值而一直阻塞,
     * 或者另一个就是被wakeup给唤醒*/
    private void waitForFlush(int waitMillSecond) {
        try {
            waitPoint.await(waitMillSecond, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 减少创建新对象
            if (waitPoint.getCount() == 0) {
                waitPoint = new CountDownLatch(1);
            }
        }
    }
}
