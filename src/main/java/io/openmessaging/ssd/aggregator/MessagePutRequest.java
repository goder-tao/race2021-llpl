package io.openmessaging.ssd.aggregator;

import java.util.concurrent.CountDownLatch;

/**
 * 传入Aggregator，使用CountDown等待异步刷盘结束，接收返回的结果
 * @author tao */
public class MessagePutRequest {
    private CountDownLatch wait;
    private Message4Flush message;

    private long downTime;

    public MessagePutRequest(Message4Flush message) {
        this.message = message;
        wait = new CountDownLatch(1);
    }

    /**
     * 获取异步任务的返回，在需要等待异步任务完成的
     * 位置调用*/
    public void getResponse() {
        try {
            long t = System.nanoTime();
            wait.await();
//            System.out.println("6.receive countDown: "+(System.nanoTime()-downTime));
//            System.out.println("7.await time: "+(System.nanoTime()-t));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void countDown(long downTime) {
        wait.countDown();
        this.downTime = downTime;
    }

    public Message4Flush getMessage() {
        return message;
    }
}
