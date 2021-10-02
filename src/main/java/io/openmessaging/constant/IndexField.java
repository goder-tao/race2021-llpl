package io.openmessaging.constant;

/**
 * indexfile的字段长度: 12B head+ 1250W*4B slot+ 5000W*20B linked index
 * @author tao
 * @date 2021-09-21**/
public class IndexField {
    // 头部位置的大小
    public static int HEAD_SIZE = 12;
    // 一个slot的大小
    public static int SLOT_SIZE = 4;
    // slot数量
    public static int SLOT_SUM = 22500000;
    // 一个linked index的大小
    public static int INDEX_SIZE = 22;
    // index数量
    public static int INDEX_SUM = 90000000;
    // 整个indexfile的大小
    public static int INDEX_FILE_SIZE = HEAD_SIZE+SLOT_SUM*SLOT_SIZE+INDEX_SUM*INDEX_SIZE;

    private IndexField() {}
}
