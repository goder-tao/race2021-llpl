package io.openmessaging.aep.util;

public interface Reader {
    byte[] read(long offset, int size);
}
