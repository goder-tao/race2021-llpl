package io.openmessaging;

import io.openmessaging.manager.Manager;

import java.nio.ByteBuffer;
import java.util.Map;


/**
 * Our solution
 * */
public class DefaultMessageQueueImpl extends MessageQueue {
    private Manager manager;
    public DefaultMessageQueueImpl() {
        manager = new Manager();
    }


    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        return manager.append(topic, queueId, data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return manager.getRange(topic, queueId, offset, fetchNum);
    }


}



