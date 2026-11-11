package io.kafkacore.lab.bench.metrics;

import java.util.Map;

/** Aggregated resource gauges for a run: name to {avg, max, samples}. */
public record ResourceSummary(Map<String, Gauge> gauges) {

    public static final ResourceSummary EMPTY = new ResourceSummary(Map.of());

    public record Gauge(double avg, double max, int samples) {
    }
}
