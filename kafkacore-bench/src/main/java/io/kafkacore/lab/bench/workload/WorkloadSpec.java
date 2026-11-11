package io.kafkacore.lab.bench.workload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.kafkacore.lab.bench.payload.MessageCodec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Declarative description of a benchmark workload, usually loaded from YAML (see {@code workloads/}).
 *
 * <p>Every field is optional in YAML; {@link #withDefaults()} fills gaps so the rest of the engine
 * can treat a spec as fully populated. A spec with a {@link Sweep} is a <em>template</em> that
 * {@link #expand()} turns into one concrete spec per combination of swept values.
 *
 * @param targetRatePerSec total produce rate across all producers; {@code 0} means unthrottled
 */
@JsonIgnoreProperties(ignoreUnknown = false)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record WorkloadSpec(
        String name,
        String description,
        String topic,
        Integer partitions,
        Integer replicationFactor,
        Integer messageSizeBytes,
        Integer producers,
        Integer consumers,
        Integer targetRatePerSec,
        Integer durationSec,
        Integer warmupSec,
        KeyStrategy keyStrategy,
        Map<String, String> producerConfig,
        Map<String, String> consumerConfig,
        Map<String, String> topicConfig,
        Sweep sweep) {

    /** Values swept by {@link #expand()}; each list replaces the scalar field of the same name. */
    @JsonIgnoreProperties(ignoreUnknown = false)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Sweep(List<Integer> messageSizeBytes, List<Integer> targetRatePerSec, List<Integer> producers) {
        boolean isEmpty() {
            return isNullOrEmpty(messageSizeBytes) && isNullOrEmpty(targetRatePerSec) && isNullOrEmpty(producers);
        }
    }

    /** How record keys are chosen; keys drive partition assignment. */
    public enum KeyStrategy {
        /** Null keys: the producer's sticky partitioner fills one batch at a time (best throughput). */
        NONE,
        /** Random 8-byte keys: uniform hash spread across partitions. */
        RANDOM
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    /** Returns a copy with every unset field populated by its documented default. */
    public WorkloadSpec withDefaults() {
        return new WorkloadSpec(
                name == null ? "unnamed" : name,
                description == null ? "" : description,
                topic == null ? "kafkacore-bench" : topic,
                partitions == null ? 6 : partitions,
                replicationFactor == null ? 3 : replicationFactor,
                messageSizeBytes == null ? 1024 : messageSizeBytes,
                producers == null ? 1 : producers,
                consumers == null ? 1 : consumers,
                targetRatePerSec == null ? 0 : targetRatePerSec,
                durationSec == null ? 30 : durationSec,
                warmupSec == null ? 5 : warmupSec,
                keyStrategy == null ? KeyStrategy.NONE : keyStrategy,
                producerConfig == null ? Map.of() : Map.copyOf(producerConfig),
                consumerConfig == null ? Map.of() : Map.copyOf(consumerConfig),
                topicConfig == null ? Map.of() : Map.copyOf(topicConfig),
                sweep);
    }

    /** Validates a defaulted spec, returning human-readable problems (empty when valid). */
    public List<String> validate() {
        WorkloadSpec s = withDefaults();
        List<String> errors = new ArrayList<>();
        if (!s.name().matches("[A-Za-z0-9._\\-\\[\\]=, ]+")) {
            errors.add("name contains unsupported characters: " + s.name());
        }
        if (!s.topic().matches("[A-Za-z0-9._\\-]+")) {
            errors.add("topic must match [A-Za-z0-9._-]+: " + s.topic());
        }
        if (s.partitions() < 1) errors.add("partitions must be >= 1");
        if (s.replicationFactor() < 1) errors.add("replicationFactor must be >= 1");
        if (s.messageSizeBytes() < MessageCodec.HEADER_SIZE) {
            errors.add("messageSizeBytes must be >= " + MessageCodec.HEADER_SIZE + " (integrity header size)");
        }
        if (s.messageSizeBytes() > 8 * 1024 * 1024) errors.add("messageSizeBytes must be <= 8 MiB");
        if (s.producers() < 1) errors.add("producers must be >= 1");
        if (s.consumers() < 1) errors.add("consumers must be >= 1");
        if (s.targetRatePerSec() < 0) errors.add("targetRatePerSec must be >= 0 (0 = unthrottled)");
        if (s.durationSec() < 1) errors.add("durationSec must be >= 1");
        if (s.warmupSec() < 0) errors.add("warmupSec must be >= 0");
        if (s.sweep() != null) {
            checkPositive(errors, "sweep.messageSizeBytes", s.sweep().messageSizeBytes(), MessageCodec.HEADER_SIZE);
            checkPositive(errors, "sweep.targetRatePerSec", s.sweep().targetRatePerSec(), 0);
            checkPositive(errors, "sweep.producers", s.sweep().producers(), 1);
        }
        return errors;
    }

    /**
     * Expands sweep combinations into concrete specs (cartesian product, in declaration order).
     * A spec without a sweep expands to a single defaulted copy of itself.
     */
    public List<WorkloadSpec> expand() {
        WorkloadSpec base = withDefaults();
        if (base.sweep() == null || base.sweep().isEmpty()) {
            return List.of(base.withSweep(null));
        }
        List<Integer> sizes = orSingleton(base.sweep().messageSizeBytes(), base.messageSizeBytes());
        List<Integer> rates = orSingleton(base.sweep().targetRatePerSec(), base.targetRatePerSec());
        List<Integer> prods = orSingleton(base.sweep().producers(), base.producers());
        List<WorkloadSpec> out = new ArrayList<>();
        for (int size : sizes) {
            for (int rate : rates) {
                for (int p : prods) {
                    Map<String, String> label = new LinkedHashMap<>();
                    if (!isNullOrEmpty(base.sweep().messageSizeBytes())) label.put("size", Integer.toString(size));
                    if (!isNullOrEmpty(base.sweep().targetRatePerSec())) label.put("rate", Integer.toString(rate));
                    if (!isNullOrEmpty(base.sweep().producers())) label.put("producers", Integer.toString(p));
                    StringBuilder n = new StringBuilder(base.name()).append('[');
                    label.forEach((k, v) -> n.append(k).append('=').append(v).append(','));
                    n.setLength(n.length() - 1);
                    n.append(']');
                    out.add(new WorkloadSpec(n.toString(), base.description(), base.topic(), base.partitions(),
                            base.replicationFactor(), size, p, base.consumers(), rate, base.durationSec(),
                            base.warmupSec(), base.keyStrategy(), base.producerConfig(), base.consumerConfig(),
                            base.topicConfig(), null));
                }
            }
        }
        return out;
    }

    public WorkloadSpec withSweep(Sweep newSweep) {
        return new WorkloadSpec(name, description, topic, partitions, replicationFactor, messageSizeBytes, producers,
                consumers, targetRatePerSec, durationSec, warmupSec, keyStrategy, producerConfig, consumerConfig,
                topicConfig, newSweep);
    }

    public WorkloadSpec withDuration(int warmup, int duration) {
        return new WorkloadSpec(name, description, topic, partitions, replicationFactor, messageSizeBytes, producers,
                consumers, targetRatePerSec, duration, warmup, keyStrategy, producerConfig, consumerConfig,
                topicConfig, sweep);
    }

    private static void checkPositive(List<String> errors, String field, List<Integer> values, int min) {
        if (values == null) return;
        for (Integer v : values) {
            if (v == null || v < min) errors.add(field + " values must be >= " + min + " but found " + v);
        }
    }

    private static boolean isNullOrEmpty(List<?> l) {
        return l == null || l.isEmpty();
    }

    private static List<Integer> orSingleton(List<Integer> values, int fallback) {
        return isNullOrEmpty(values) ? List.of(fallback) : values;
    }

    /** Fluent builder used by tests, chaos scenarios and programmatic callers. */
    public static final class Builder {
        private final String name;
        private String description = "";
        private String topic = "kafkacore-bench";
        private int partitions = 6;
        private int replicationFactor = 3;
        private int messageSizeBytes = 1024;
        private int producers = 1;
        private int consumers = 1;
        private int targetRatePerSec = 0;
        private int durationSec = 30;
        private int warmupSec = 5;
        private KeyStrategy keyStrategy = KeyStrategy.NONE;
        private final Map<String, String> producerConfig = new LinkedHashMap<>();
        private final Map<String, String> consumerConfig = new LinkedHashMap<>();
        private final Map<String, String> topicConfig = new LinkedHashMap<>();

        private Builder(String name) {
            this.name = name;
        }

        public Builder description(String v) { this.description = v; return this; }
        public Builder topic(String v) { this.topic = v; return this; }
        public Builder partitions(int v) { this.partitions = v; return this; }
        public Builder replicationFactor(int v) { this.replicationFactor = v; return this; }
        public Builder messageSizeBytes(int v) { this.messageSizeBytes = v; return this; }
        public Builder producers(int v) { this.producers = v; return this; }
        public Builder consumers(int v) { this.consumers = v; return this; }
        public Builder targetRatePerSec(int v) { this.targetRatePerSec = v; return this; }
        public Builder durationSec(int v) { this.durationSec = v; return this; }
        public Builder warmupSec(int v) { this.warmupSec = v; return this; }
        public Builder keyStrategy(KeyStrategy v) { this.keyStrategy = v; return this; }
        public Builder producerConfig(String k, String v) { producerConfig.put(k, v); return this; }
        public Builder consumerConfig(String k, String v) { consumerConfig.put(k, v); return this; }
        public Builder topicConfig(String k, String v) { topicConfig.put(k, v); return this; }

        public WorkloadSpec build() {
            return new WorkloadSpec(name, description, topic, partitions, replicationFactor, messageSizeBytes,
                    producers, consumers, targetRatePerSec, durationSec, warmupSec, keyStrategy,
                    Map.copyOf(producerConfig), Map.copyOf(consumerConfig), Map.copyOf(topicConfig), null);
        }
    }
}
