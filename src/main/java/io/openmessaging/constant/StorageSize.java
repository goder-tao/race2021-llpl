package io.openmessaging.constant;

/**
 * 存储单位*/
public class StorageSize {
    public static final long KB = 1024L;
    public static final long MB = 1024*KB;
    public static final long GB = 1024*MB;

    public static final long COLD_SPACE_SIZE = 45*GB;
    public static final long HOT_SPACE_SIZE = 15*GB;
<<<<<<< HEAD
//    public static final long COLD_SPACE_SIZE = 80*GB;
//    public static final long HOT_SPACE_SIZE = 40*MB;
=======
    public static final long DEFAULT_PARTITION_SIZE = StorageSize.GB;
//    private final long defaultPartitionSize = StorageSize.MB*10;
//    public static final long COLD_SPACE_SIZE = 89*MB;
//    public static final long HOT_SPACE_SIZE = 49*MB;
>>>>>>> test
}
