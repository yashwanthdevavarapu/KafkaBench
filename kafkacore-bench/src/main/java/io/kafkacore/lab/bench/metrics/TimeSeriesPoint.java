package io.kafkacore.lab.bench.metrics;

/** One-second sample of a running benchmark, used for timeline charts. */
public record TimeSeriesPoint(int second, long producedMsgs, long consumedMsgs, double publishP99Ms,
                              double endToEndP99Ms, long errors) {
}
