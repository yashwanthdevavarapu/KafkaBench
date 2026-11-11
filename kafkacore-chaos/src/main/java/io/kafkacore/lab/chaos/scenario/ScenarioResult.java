package io.kafkacore.lab.chaos.scenario;

import io.kafkacore.lab.bench.integrity.IntegrityReport;
import io.kafkacore.lab.bench.metrics.LatencyStats;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Outcome of one chaos scenario: pass/fail checks, recovery timings, integrity and the event timeline. */
public record ScenarioResult(
        String id,
        String title,
        String description,
        String fault,
        Instant startedAt,
        long durationMs,
        boolean passed,
        List<Check> checks,
        Map<String, Object> metrics,
        IntegrityReport integrity,
        LatencyStats publishLatency,
        long producedAcked,
        long producerErrors,
        List<Timeline.Event> timeline,
        List<Sample> throughput,
        String error) {

    /** A named assertion evaluated at the end of the scenario. */
    public record Check(String name, boolean passed, String detail) {
    }

    /** Per-second client view during the scenario, used to draw the availability dip. */
    public record Sample(long atMs, long producedPerSec, long consumedPerSec, double publishP99Ms, long errors) {
    }
}
