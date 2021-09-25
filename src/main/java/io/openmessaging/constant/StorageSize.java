package io.openmessaging.constant;

/**
 * 存储单位
 */
public class StorageSize {
    public static final long KB = 1024L;
    public static final long MB = 1024 * KB;
    public static final long GB = 1024 * MB;

    public static final long COLD_SPACE_SIZE = 45*GB;
    public static final long HOT_SPACE_SIZE = 15*GB;
//    public static final long COLD_SPACE_SIZE = 400*MB;
//    public static final long HOT_SPACE_SIZE = 200*MB;
    public static final long DEFAULT_UNIT_SIZE = 50*MB;

    // 批大小
    public static final int DEFAULT_BATCH_SIZE = (int) (8*KB);
    public static final int SMALL_BATCH_SIZE = (int) (8*KB);
    public static final int MIDDLE_BATCH_SIZE = (int) (16*KB);
    public static final int LARGE_BATCH_SIZE = (int) (24*KB);

    public static final long DEFAULT_PARTITION_SIZE = 50*MB;
}
