package io.kafkacore.lab.bench.metrics;

import java.util.Map;

/** Produces one point-in-time sample of named resource gauges (e.g. {@code broker.kafka-1.cpu.pct}). */
@FunctionalInterface
public interface ResourceSampler {
    Map<String, Double> sample();
}
