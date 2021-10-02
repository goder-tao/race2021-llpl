package io.openmessaging.ssd.aggregator;

import io.openmessaging.ssd.index.IndexHandle;
import io.openmessaging.ssd.util.AppendRes2;
import io.openmessaging.ssd.util.SSDWriterReader5MMAP;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.*;

/**
 * 向上聚合多个thread的数据，默认8kb聚合，向下调用底层disk写接口写入一批数据
 * 使用线程池异步force，在task中并行进行index以及write data的force，并在
 * 结束之后通知put线程任务完成
 * @version 1.2
 * @data 2021-09-30
 * @author tao
 * */
public class Aggregator implements Runnable {
    private volatile Batch batch = new Batch();
    // 等待落盘的batch队列
    private final ConcurrentLinkedQueue<Batch> flushBatchQueue = new ConcurrentLinkedQueue<>();

    private final IndexHandle indexHandle;

    private final Logger logger = LogManager.getLogger(Aggregator.class.getName());
    // 线程池、异步force
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    // 并行index force线程池
    private final ExecutorService forceExecutor = Executors.newFixedThreadPool(10);
    // 使用一个信号量进行唤醒
    private final Semaphore waitPoint = new Semaphore(0);

    public Aggregator(IndexHandle indexHandle) {
        this.indexHandle = indexHandle;
    }

    /**
     * 添加一个request，尝试向当前batch添加一个request, batch的最大size根据第一个加入
     * batch的req决定，size都为8kb的整数倍. 如果加入当前batch失败则将当前batch加入flush
     * queue，并新建一个batch添加当前req
     * */
    public synchronized void putMessageRequest(MessagePutRequest req) {
        // 当前batch已经满了，需要进行刷盘
        if (!this.batch.tryToAdd(req)) {
            flushBatchQueue.offer(batch);
            batch = new Batch();
            waitPoint.release();
            batch.tryToAdd(req);
        } else if (batch.isCanFlush()) {
            flushBatchQueue.offer(batch);
            batch = new Batch();
            waitPoint.release();
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
        while (true) {
            try {
                // 尝试获取信号量并等待一个比较长的时间，用来处理最后一条消息
                if (!waitPoint.tryAcquire(200, TimeUnit.MILLISECONDS)) {
                    flushBatchQueue.offer(batch);
                    batch = new Batch();
                }
                doFlush();
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("aggregator waitPoint, "+e.toString());
            }
        }
    }

    /**
     * 异步force任务，datafile和index并行force， */
    private class ForceTask implements Runnable{
//        private RandomAccessFile raf;
        private MappedByteBuffer mmap;
        private Batch flushBatch;
        private volatile CountDownLatch forceCountDown = new CountDownLatch(1);

        ForceTask(MappedByteBuffer mmap, Batch flushBatch) {
            this.mmap = mmap;
            this.flushBatch = flushBatch;
        }

        @Override
        public void run() {
            long t = System.nanoTime();
            // 并行 force index mmap
            forceExecutor.execute(() -> {
                IndexHandle.getInstance().force();
                forceCountDown.countDown();
            });

            // force datafile
            mmap.force();
//            try {
//                raf.getChannel().force(true);
//                raf.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }

            try {
                forceCountDown.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("4.force time: "+(System.nanoTime()-t));
            t = System.nanoTime();

            // 通知put线程持久化完成
            for (MessagePutRequest req:flushBatch) {
                req.countDown(System.nanoTime());
            }
            System.out.println("5.countDown time: "+(System.nanoTime()-t));
        }
    }

    /**
     * 按照字段组合batch中的数据，调用disk接口进行append写入，更新index，
     * 接着开启异步任务进行datafile和index的并行force，在异步任务中通知
     * put线程任务完成*/
    private void doFlush() {
        long t = System.nanoTime();
        Batch flushBatch = flushBatchQueue.poll();
        if (flushBatch == null || flushBatch.isEmpty()) return;

        ByteBuffer dataBuffer = ByteBuffer.allocate(flushBatch.getBatchSize());
        for (MessagePutRequest req:flushBatch) {
            dataBuffer.put(req.getMessage().getData());
        }

        // 刷盘
        AppendRes2 appendRes = SSDWriterReader5MMAP.getInstance().append(dataBuffer.array());
        long sOff = appendRes.getWriteStartOffset();
        MappedByteBuffer mmap = appendRes.getMmap();

//        System.out.println("1.append time: "+(System.nanoTime()-t));
        t = System.nanoTime();

        // 更新index
        for (MessagePutRequest req:flushBatch) {
            indexHandle.newIndex(flushBatch.peek().getMessage().getHashKey(), sOff, (short) req.getMessage().getData().length);
            sOff += req.getMessage().getData().length;
        }

//        System.out.println("2.new index time: "+(System.nanoTime()-t));
        t = System.nanoTime();

        executor.submit(new ForceTask(mmap, flushBatch));

//        System.out.println("3.execute task time: "+(System.nanoTime()-t));
    }
}
