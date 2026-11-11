package io.kafkacore.lab.bench.metrics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class LatencyRecorderTest {

    @Test
    void computesPercentilesOverUniformDistribution() {
        LatencyRecorder r = new LatencyRecorder();
        for (int ms = 1; ms <= 1000; ms++) {
            r.recordNanos(ms * 1_000_000L);
        }
        LatencyStats s = r.summary();
        assertThat(s.count()).isEqualTo(1000);
        assertThat(s.p50Ms()).isCloseTo(500, within(1.0));
        assertThat(s.p95Ms()).isCloseTo(950, within(1.0));
        assertThat(s.p99Ms()).isCloseTo(990, within(1.0));
        assertThat(s.maxMs()).isCloseTo(1000, within(1.0));
        assertThat(s.minMs()).isCloseTo(1, within(0.01));
    }

    @Test
    void tickReturnsIntervalButSummaryIsCumulative() {
        LatencyRecorder r = new LatencyRecorder();
        r.recordNanos(1_000_000);
        assertThat(r.tick().count()).isEqualTo(1);
        r.recordNanos(100_000_000);
        r.recordNanos(100_000_000);
        LatencyStats interval = r.tick();
        assertThat(interval.count()).isEqualTo(2);
        assertThat(interval.minMs()).isCloseTo(100, within(0.1));
        assertThat(r.summary().count()).isEqualTo(3);
    }

    @Test
    void resetDropsWarmupValues() {
        LatencyRecorder r = new LatencyRecorder();
        r.recordNanos(5_000_000_000L); // a 5 s warm-up outlier
        r.tick();
        r.reset();
        r.recordNanos(2_000_000);
        LatencyStats s = r.summary();
        assertThat(s.count()).isEqualTo(1);
        assertThat(s.maxMs()).isCloseTo(2, within(0.01));
    }

    @Test
    void clampsNonPositiveAndHugeValues() {
        LatencyRecorder r = new LatencyRecorder();
        r.recordNanos(-5);
        r.recordNanos(Long.MAX_VALUE);
        LatencyStats s = r.summary();
        assertThat(s.count()).isEqualTo(2);
        assertThat(s.minMs()).isGreaterThan(0);
        assertThat(s.maxMs()).isCloseTo(300_000, within(300.0));
    }

    @Test
    void emptySummaryIsZeroes() {
        assertThat(new LatencyRecorder().summary()).isEqualTo(LatencyStats.EMPTY);
    }
}
