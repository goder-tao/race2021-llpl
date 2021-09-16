package io.openmessaging.util;

import io.openmessaging.constant.DataFileBasicInfo;

public class PartitionMaker {
    /**
     * 生成分区的文件名, 1->00100000...,作为data file和index file的名字
     *
     * @param partition - 第几分区，从0开始
     * @param len       - 分区文件名的长度
     * @param base      - 一个分区的offset基数
     */
    public static String makePartitionPath(int partition, int len, int base) {
        StringBuilder s = new StringBuilder(partition * base + "");
        for (int i = s.length(); i < len; i++) {
            s.insert(0, "0");
        }
        return s.toString();
    }

    public static void main(String[] args) {
        System.out.println(makePartitionPath(1, DataFileBasicInfo.FILE_NAME_LENGTH, DataFileBasicInfo.ITEM_NUM));
    }
}
