package io.openmessaging.util;

import java.lang.management.ManagementFactory;

import com.sun.management.OperatingSystemMXBean;

public class OSUtil {
    public static double getAverageCpuLoad() {
        OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        int cpuCount = osmxb.getAvailableProcessors();
        return osmxb.getSystemCpuLoad()/cpuCount;
    }

    public static long getAvailableSystemMemory() {
        OperatingSystemMXBean osmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        return osmxb.getFreePhysicalMemorySize();
//        return  osmxb.getTotalPhysicalMemorySize();
    }

    public static void main(String[] args) {
        System.out.println("cpu: "+getAverageCpuLoad());
        System.out.println("memory: "+getAvailableSystemMemory());
    }
}
