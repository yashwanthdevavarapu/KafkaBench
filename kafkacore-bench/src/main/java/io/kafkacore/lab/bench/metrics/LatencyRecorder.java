package io.kafkacore.lab.bench.metrics;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;

/**
 * Thread-safe latency recorder backed by HdrHistogram (see ADR-0004).
 *
 * <p>Writers call {@link #recordNanos(long)} from any thread without locking. A single reader
 * thread calls {@link #tick()} periodically to obtain the interval histogram, which is also folded
 * into a cumulative histogram used for the final summary.
 */
public final class LatencyRecorder {

    /** Values are stored in microseconds; anything above 5 minutes is clamped. */
    private static final long MAX_TRACKABLE_MICROS = TimeUnit.MINUTES.toMicros(5);

    private final Recorder recorder = new Recorder(MAX_TRACKABLE_MICROS, 3);
    private final Histogram cumulative = new Histogram(MAX_TRACKABLE_MICROS, 3);
    private Histogram recycled;

    public void recordNanos(long nanos) {
        long micros = Math.max(1, Math.min(MAX_TRACKABLE_MICROS, nanos / 1_000));
        recorder.recordValue(micros);
    }

    /** Drains values recorded since the previous tick, merges them into the total and returns the interval stats. */
    public synchronized LatencyStats tick() {
        recycled = recorder.getIntervalHistogram(recycled);
        cumulative.add(recycled);
        return LatencyStats.of(recycled);
    }

    /** Discards everything recorded so far (used to drop the warm-up window). */
    public synchronized void reset() {
        recycled = recorder.getIntervalHistogram(recycled);
        cumulative.reset();
    }

    /** Statistics over everything recorded since the last {@link #reset()}, including un-ticked values. */
    public synchronized LatencyStats summary() {
        tick();
        return LatencyStats.of(cumulative);
    }
}
