package io.openmessaging.ssd.aggregator;

import io.openmessaging.util.TimeCounter;

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
            TimeCounter.getAggregatorInstance().addTime("receive count down time", (int) (System.nanoTime()-downTime));
            TimeCounter.getAggregatorInstance().addTime("await time", (int) (System.nanoTime()-t));
            TimeCounter.getAggregatorInstance().increaseTimes();
            TimeCounter.getAggregatorInstance().analyze();
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
