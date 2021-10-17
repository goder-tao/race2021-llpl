package io.openmessaging.util;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 计时器，提供可注册的字段方式对运行中的各个步骤进行百分比和平均的计时
 * @author tao
 * @date 2021-10-03*/
public class TimeCounter {
    private final String name;
    // 开关
    private  boolean isEnable = true;
    // 计时次数(计算平均用)
    private AtomicLong countTimes;
    // 总时间
    private AtomicLong sumTime;
    // 其他各个部分的时间
    private ConcurrentHashMap<String, AtomicLong> partTime;
    // 百分比小数格式
    private DecimalFormat decimalFormat=new DecimalFormat("0.000000");
    // 获取系统参数
    private static OperatingSystemMXBean operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    // 单例
    public static TimeCounter managerTimeCounter = new TimeCounter("Manager counter");

    public static TimeCounter aggregatorTimeCounter = new TimeCounter("Aggregator counter");

    private TimeCounter(String name) {
        this.name = name;
        countTimes = new AtomicLong(0);
        sumTime = new AtomicLong(0);
        partTime = new ConcurrentHashMap<>();

    }

    /**
     * 添加某个部分的时间*/
    public void addTime(String part, int increment) {
        if (isEnable) {
            sumTime.getAndAdd(increment);
            if (partTime.get(part) == null) {
                AtomicLong atomicLong = new AtomicLong(0);
                AtomicLong flag = partTime.putIfAbsent(part, atomicLong);
                if (flag != null) {
                    atomicLong = flag;
                }
                atomicLong.addAndGet(increment);
            } else {
                partTime.get(part).addAndGet(increment);
            }
        }
    }

    /**
     * 完成一轮计时的时候用，增加完成一次完整步骤计时的次数*/
    public void increaseTimes() {
        if (isEnable) {
            countTimes.incrementAndGet();
        }
    }

    /**
     * 输出统计的时间*/
    public void analyze() {
        if (isEnable) {
            StringBuilder stringBuilder1 = new StringBuilder(), stringBuilder2 = new StringBuilder();
            stringBuilder1.append(this.name+" average time --- total: ").append((sumTime.get()) / countTimes.get());
            stringBuilder2.append(this.name+" time percent --- ");

            for (String key:partTime.keySet()) {
                stringBuilder1.append(", "+key+": "+partTime.get(key).get()/(countTimes.get()));
                stringBuilder2.append(", "+key+": "+decimalFormat.format((double) partTime.get(key).get()/sumTime.get()*100)+"%");
            }

            System.out.println(stringBuilder1.toString());
            System.out.println(stringBuilder2.toString());
            System.out.println();
        }
    }

    public static TimeCounter getManagerInstance() {
        return managerTimeCounter;
    }

    public static TimeCounter getAggregatorInstance() {
        return aggregatorTimeCounter;
    }

    // 关闭使用
    public static void disableCounter() {
        managerTimeCounter.isEnable = false;
        aggregatorTimeCounter.isEnable = false;
    }
}
