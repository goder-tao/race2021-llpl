package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;


/**
 * Our solution
 * */
public class DefaultMessageQueueImpl extends MessageQueue {

    private <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue){
        V retObj = map.get(key);
        if(retObj != null){
            return retObj;
        }
        map.put(key, defaultValue);
        return defaultValue;
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data){

        return 0L;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        return null;
    }
}



