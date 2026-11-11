package io.kafkacore.lab.bench.metrics;

import org.HdrHistogram.Histogram;

/** Immutable latency summary in milliseconds. */
public record LatencyStats(long count, double minMs, double meanMs, double p50Ms, double p95Ms, double p99Ms,
                           double p999Ms, double maxMs) {

    public static final LatencyStats EMPTY = new LatencyStats(0, 0, 0, 0, 0, 0, 0, 0);

    static LatencyStats of(Histogram h) {
        if (h.getTotalCount() == 0) {
            return EMPTY;
        }
        return new LatencyStats(
                h.getTotalCount(),
                ms(h.getMinValue()),
                round(h.getMean() / 1_000.0),
                ms(h.getValueAtPercentile(50)),
                ms(h.getValueAtPercentile(95)),
                ms(h.getValueAtPercentile(99)),
                ms(h.getValueAtPercentile(99.9)),
                ms(h.getMaxValue()));
    }

    private static double ms(long micros) {
        return round(micros / 1_000.0);
    }

    private static double round(double v) {
        return Math.round(v * 1_000.0) / 1_000.0;
    }
}
