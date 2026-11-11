package io.kafkacore.lab.bench.metrics;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.LinkedHashMap;
import java.util.Map;

/** Samples the benchmark client JVM itself: process CPU, heap usage and GC time. */
public final class JvmResourceSampler implements ResourceSampler {

    private final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
    private final java.lang.management.OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    private long lastGcMillis = totalGcMillis();

    @Override
    public synchronized Map<String, Double> sample() {
        Map<String, Double> out = new LinkedHashMap<>();
        if (os instanceof com.sun.management.OperatingSystemMXBean sun) {
            double load = sun.getProcessCpuLoad();
            if (load >= 0) {
                out.put("client.cpu.pct", round(load * 100.0));
            }
        }
        out.put("client.heap.mb", round(memory.getHeapMemoryUsage().getUsed() / (1024.0 * 1024.0)));
        long gc = totalGcMillis();
        out.put("client.gc.ms", (double) (gc - lastGcMillis));
        lastGcMillis = gc;
        return out;
    }

    private static long totalGcMillis() {
        long total = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            total += Math.max(0, gc.getCollectionTime());
        }
        return total;
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
