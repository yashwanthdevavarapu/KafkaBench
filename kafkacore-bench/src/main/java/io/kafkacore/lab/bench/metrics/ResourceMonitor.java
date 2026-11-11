package io.kafkacore.lab.bench.metrics;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically polls a set of {@link ResourceSampler}s on daemon threads and aggregates each gauge
 * into avg / max over the monitored window.
 *
 * <p>Each sampler runs on its own schedule so a slow one ({@code docker stats} can take several
 * seconds on a busy host) never delays the cheap in-JVM sampler. {@link #stop()} lets an in-flight
 * sample complete instead of interrupting it, so short windows still get at least one data point.
 */
public final class ResourceMonitor implements AutoCloseable {

    private static final long STOP_GRACE_MILLIS = 20_000;

    private final List<ResourceSampler> samplers;
    private final long periodMillis;
    private final Map<String, double[]> agg = new TreeMap<>(); // name -> {sum, max, count}; guarded by agg
    private final Object lifecycle = new Object();
    private ScheduledExecutorService scheduler; // guarded by lifecycle

    public ResourceMonitor(List<ResourceSampler> samplers, long periodMillis) {
        this.samplers = List.copyOf(samplers);
        this.periodMillis = periodMillis;
    }

    public static ResourceMonitor none() {
        return new ResourceMonitor(List.of(), 1000);
    }

    public void start() {
        synchronized (lifecycle) {
            if (samplers.isEmpty() || scheduler != null) return;
            scheduler = Executors.newScheduledThreadPool(samplers.size(), r -> {
                Thread t = new Thread(r, "kafkacore-resource-monitor");
                t.setDaemon(true);
                return t;
            });
            for (ResourceSampler s : samplers) {
                scheduler.scheduleWithFixedDelay(() -> sample(s), 0, periodMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    /** Takes one sample synchronously from every sampler. Exposed for tests. */
    public void sampleOnce() {
        samplers.forEach(this::sample);
    }

    private void sample(ResourceSampler s) {
        Map<String, Double> sample;
        try {
            sample = s.sample();
        } catch (RuntimeException e) {
            return;
        }
        synchronized (agg) {
            sample.forEach((k, v) -> {
                double[] a = agg.computeIfAbsent(k, x -> new double[]{0, Double.NEGATIVE_INFINITY, 0});
                a[0] += v;
                a[1] = Math.max(a[1], v);
                a[2]++;
            });
        }
    }

    public void reset() {
        synchronized (agg) {
            agg.clear();
        }
    }

    public ResourceSummary summary() {
        Map<String, ResourceSummary.Gauge> out = new LinkedHashMap<>();
        synchronized (agg) {
            agg.forEach((k, a) -> out.put(k, new ResourceSummary.Gauge(
                    Math.round(a[0] / a[2] * 100.0) / 100.0, Math.round(a[1] * 100.0) / 100.0, (int) a[2])));
        }
        return new ResourceSummary(out);
    }

    /** Stops scheduling new samples and waits (bounded) for in-flight ones; {@link #summary()} stays available. */
    public void stop() {
        ScheduledExecutorService s;
        synchronized (lifecycle) {
            s = scheduler;
            scheduler = null;
        }
        if (s == null) return;
        s.shutdown();
        try {
            if (!s.awaitTermination(STOP_GRACE_MILLIS, TimeUnit.MILLISECONDS)) s.shutdownNow();
        } catch (InterruptedException e) {
            s.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        stop();
    }
}
