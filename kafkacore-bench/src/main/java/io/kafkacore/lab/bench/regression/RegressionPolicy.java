package io.kafkacore.lab.bench.regression;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

/**
 * Relative tolerances for regression detection (see ADR-0008). A metric FAILs when it moves in the
 * bad direction by more than its threshold, and WARNs past {@link #warnFraction} of the threshold.
 *
 * <p>Latency thresholds are deliberately looser than throughput ones: tail latency on shared
 * hardware is noisy, and a gate that cries wolf gets disabled.
 */
public record RegressionPolicy(
        double maxThroughputDropPct,
        double maxPublishP95IncreasePct,
        double maxPublishP99IncreasePct,
        double maxEndToEndP99IncreasePct,
        double warnFraction,
        double minAbsoluteLatencyDeltaMs,
        boolean requireIntegrity) {

    public static RegressionPolicy defaults() {
        return new RegressionPolicy(10, 20, 25, 30, 0.5, 1.0, true);
    }

    public static RegressionPolicy load(Path yaml) {
        try {
            return new ObjectMapper(new YAMLFactory())
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
                    .readValue(yaml.toFile(), RegressionPolicy.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read regression policy " + yaml, e);
        }
    }
}
