package io.openmessaging.aep.pool;

public interface Pool {
    PMemUnit allocate();

    void deAllocate(PMemUnit pMemUnit);
}
