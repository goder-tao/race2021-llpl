package io.openmessaging.ssd.aggregator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

/**
 * 传入Aggregator，向外暴露future task，接收返回的结果
 *
 * @author tao */
public class MessagePutRequest {
    private CompletableFuture<Integer> future;
    private CountDownLatch wait;
    private Message4Flush message;

    public MessagePutRequest(Message4Flush message) {
        this.message = message;
        wait = new CountDownLatch(1);
    }

    /**
     * 获取异步任务的返回，在需要等待异步任务完成的
     * 位置调用*/
    public void getResponse() {
        try {
            wait.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void countDown() {
        wait.countDown();
    }

    public Message4Flush getMessage() {
        return message;
    }
}
