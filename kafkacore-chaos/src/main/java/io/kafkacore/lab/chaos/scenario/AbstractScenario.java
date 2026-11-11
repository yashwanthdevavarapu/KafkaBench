package io.kafkacore.lab.chaos.scenario;

import io.kafkacore.lab.bench.driver.WorkloadSession;
import io.kafkacore.lab.bench.integrity.IntegrityReport;
import io.kafkacore.lab.bench.metrics.LatencyStats;
import io.kafkacore.lab.bench.workload.WorkloadSpec;
import io.kafkacore.lab.chaos.cluster.ClusterInspector;
import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import io.kafkacore.lab.chaos.fault.FaultType;
import io.kafkacore.lab.chaos.scenario.Timeline.Kind;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Template for every scenario (see docs/fault-injection.md):
 *
 * <ol>
 *   <li>pre-check: cluster healthy (quorum leader, 3 live brokers, 0 URP)</li>
 *   <li>start a durable integrity workload (acks=all, idempotent, RF=3, min.insync=2)</li>
 *   <li>steady state, then {@link #execute(Run)}: inject + observe + heal (scenario-specific)</li>
 *   <li>wait for full recovery, steady state again, drain, reconcile every acked message</li>
 * </ol>
 *
 * Any fault registered via {@link Run#inject} is healed in a {@code finally} block, so an aborted
 * scenario never leaves the cluster broken for the next one.
 */
public abstract class AbstractScenario implements ChaosScenario {

    @Override
    public final ScenarioResult run(ScenarioContext ctx) {
        Instant startedAt = Instant.now();
        Timeline timeline = new Timeline(ctx.log());
        Run run = new Run(ctx, timeline);
        String error = null;
        IntegrityReport integrity = IntegrityReport.EMPTY;
        LatencyStats latency = LatencyStats.EMPTY;
        long acked = 0;
        long errors = 0;
        ctx.log().accept("▶ " + id() + " — " + title());
        String runId = id().replaceAll("[^a-z0-9]", "") + "-" + UUID.randomUUID().toString().substring(0, 6);
        WorkloadSpec spec = WorkloadSpec.builder("chaos-" + id())
                .topic("kafkacore-chaos")
                .partitions(6)
                .replicationFactor(3)
                .messageSizeBytes(512)
                .targetRatePerSec(ctx.ratePerSec())
                .producers(1)
                .consumers(2)
                .topicConfig("min.insync.replicas", "2")
                .producerConfig("acks", "all")
                .producerConfig("enable.idempotence", "true")
                .producerConfig("delivery.timeout.ms", "180000")
                .producerConfig("request.timeout.ms", "15000")
                .producerConfig("max.block.ms", "60000")
                .consumerConfig("session.timeout.ms", "10000")
                .consumerConfig("heartbeat.interval.ms", "2000")
                .build();
        try (WorkloadSession session = new WorkloadSession(ctx.topology().bootstrapServers(), spec, runId)) {
            run.session = session;
            timeline.add(Kind.INFO, "pre-check: waiting for a healthy cluster");
            OptionalLong healthy = Await.until(ctx.recoveryTimeout(), Duration.ofSeconds(1),
                    () -> ctx.inspector().healthy(ctx.topology().size()));
            if (healthy.isEmpty()) {
                throw new IllegalStateException("cluster not healthy before fault: " + ctx.inspector().snapshot());
            }
            session.createTopic();
            session.startConsumers(Duration.ofSeconds(60));
            session.startProducers();
            run.startSampler();
            timeline.add(Kind.INFO, String.format("integrity workload running: %d msg/s, topic %s (RF=3, min.insync=2, acks=all)",
                    ctx.ratePerSec(), session.topic()));
            Thread.sleep(ctx.steadyState().toMillis());

            try {
                execute(run);
            } finally {
                run.healAll();
            }

            OptionalLong recovered = Await.until(ctx.recoveryTimeout(), Duration.ofMillis(500),
                    () -> ctx.inspector().healthy(ctx.topology().size()));
            run.check("cluster fully recovered (quorum + 3 brokers + 0 URP)", recovered.isPresent(),
                    recovered.isPresent() ? "healthy " + recovered.getAsLong() + " ms after heal" : "not healthy within " + ctx.recoveryTimeout());
            recovered.ifPresent(ms -> run.metric("fullRecoveryAfterHealMs", ms));
            if (recovered.isPresent()) timeline.add(Kind.RECOVERED, "cluster healthy again");

            long before = session.producedAcked();
            Thread.sleep(ctx.steadyState().toMillis());
            long after = session.producedAcked();
            run.check("producers make progress after recovery", after > before,
                    (after - before) + " msgs acked in final " + ctx.steadyState().toSeconds() + " s");

            session.stopProducers(Duration.ofSeconds(200));
            run.stopSampler();
            boolean drained = session.drain(Duration.ofSeconds(60));
            session.stopConsumers(Duration.ofSeconds(30));
            integrity = session.tracker().report();
            latency = session.publishLatency().summary();
            acked = session.producedAcked();
            errors = session.producerErrors();
            timeline.add(Kind.INFO, String.format("workload stopped: %,d acked, %d send errors, drained=%s", acked, errors, drained));
            run.check("no acknowledged message lost", integrity.lost() == 0, integrity.lost() + " lost of " + integrity.acked() + " acked");
            run.check("no corrupted payloads", integrity.corrupt() == 0, integrity.corrupt() + " corrupt");
            run.check("no producer-side duplicates or re-ordering (idempotence)", integrity.outOfOrder() == 0,
                    integrity.outOfOrder() + " out-of-order; " + integrity.duplicates() + " consumer re-deliveries (allowed, at-least-once)");
        } catch (Exception e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            error = e.getClass().getSimpleName() + ": " + e.getMessage();
            timeline.add(Kind.ERROR, error);
            run.healAll();
            run.check("scenario completed without error", false, error);
        } finally {
            run.stopSampler();
        }
        boolean passed = error == null && run.checks.stream().allMatch(ScenarioResult.Check::passed);
        long durationMs = Duration.between(startedAt, Instant.now()).toMillis();
        ctx.log().accept((passed ? "✔ PASSED " : "✘ FAILED ") + id() + " in " + durationMs / 1000 + " s");
        return new ScenarioResult(id(), title(), description(), fault().description(), startedAt, durationMs, passed,
                List.copyOf(run.checks), run.metrics, integrity, latency, acked, errors, timeline.events(),
                run.samples(), error);
    }

    /** Scenario-specific inject → observe → heal. Faults must be injected through {@link Run#inject}. */
    protected abstract void execute(Run run) throws Exception;

    /** Mutable per-execution state handed to {@link #execute(Run)}. */
    protected static final class Run {
        private final ScenarioContext ctx;
        private final Timeline timeline;
        private final List<ScenarioResult.Check> checks = new ArrayList<>();
        private final Map<String, Object> metrics = new LinkedHashMap<>();
        private final List<ScenarioResult.Sample> samples = new ArrayList<>();
        private final Map<ClusterTopology.Node, FaultType> active = new LinkedHashMap<>();
        private WorkloadSession session;
        private Thread sampler;
        private volatile boolean sampling;

        Run(ScenarioContext ctx, Timeline timeline) {
            this.ctx = ctx;
            this.timeline = timeline;
        }

        public ScenarioContext ctx() { return ctx; }
        public ClusterInspector inspector() { return ctx.inspector(); }
        public ClusterTopology topology() { return ctx.topology(); }
        public WorkloadSession session() { return session; }
        public Timeline timeline() { return timeline; }

        public void log(String message) {
            timeline.add(Kind.INFO, message);
        }

        public void observe(String message) {
            timeline.add(Kind.OBSERVE, message);
        }

        /**
         * Injects a fault. The FAULT event is stamped when the fault has taken effect (the docker
         * command returned), so recovery timings and chart markers start from the real failure.
         */
        public void inject(ClusterTopology.Node node, FaultType fault) {
            active.put(node, fault);
            long t0 = System.nanoTime();
            ctx.injector().inject(node, fault);
            long dockerMs = (System.nanoTime() - t0) / 1_000_000;
            // Recovery timers start when the docker command returns. On a slow host the fault may take
            // effect well before that, so the command latency is kept as a metric for interpretation.
            if (!metrics.containsKey("injectCommandMs")) metric("injectCommandMs", dockerMs);
            timeline.add(Kind.FAULT, fault.description() + " → node " + node.id() + " (" + node.container()
                    + ", docker " + dockerMs + " ms)");
        }

        public void heal(ClusterTopology.Node node) {
            FaultType f = active.remove(node);
            if (f == null) return;
            long t0 = System.nanoTime();
            ctx.injector().heal(node, f);
            timeline.add(Kind.HEAL, "heal " + f.name().toLowerCase().replace('_', ' ') + " → node " + node.id()
                    + " (docker " + (System.nanoTime() - t0) / 1_000_000 + " ms)");
        }

        void healAll() {
            for (ClusterTopology.Node n : List.copyOf(active.keySet())) {
                try {
                    heal(n);
                } catch (RuntimeException e) {
                    timeline.add(Kind.ERROR, "heal failed for node " + n.id() + ": " + e.getMessage());
                }
            }
        }

        public void metric(String name, Object value) {
            metrics.put(name, value);
        }

        public void check(String name, boolean passed, String detail) {
            checks.add(new ScenarioResult.Check(name, passed, detail));
            timeline.add(Kind.CHECK, (passed ? "✔ " : "✘ ") + name + " — " + detail);
        }

        /**
         * Waits for a condition, records its duration as a metric, adds a check and an OBSERVE event.
         *
         * @return elapsed ms, or empty on timeout
         */
        public OptionalLong awaitAndMeasure(String metric, String description, Duration timeout, Callable<Boolean> condition)
                throws InterruptedException {
            OptionalLong ms = Await.until(timeout, Duration.ofMillis(200), condition);
            if (ms.isPresent()) {
                metric(metric, ms.getAsLong());
                observe(description + " after " + ms.getAsLong() + " ms");
            }
            check(description, ms.isPresent(), ms.isPresent() ? ms.getAsLong() + " ms" : "timed out after " + timeout.toSeconds() + " s");
            return ms;
        }

        public void hold(Duration d) throws InterruptedException {
            Thread.sleep(d.toMillis());
        }

        void startSampler() {
            sampling = true;
            sampler = new Thread(() -> {
                long prevAcked = session.producedAcked();
                long prevConsumed = session.consumed();
                long prevErr = session.producerErrors();
                long next = System.nanoTime();
                while (sampling) {
                    next += 1_000_000_000L;
                    long sleep = (next - System.nanoTime()) / 1_000_000;
                    if (sleep > 0) {
                        try {
                            Thread.sleep(sleep);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                    long a = session.producedAcked(), c = session.consumed(), er = session.producerErrors();
                    double p99 = session.publishLatency().tick().p99Ms();
                    synchronized (samples) {
                        samples.add(new ScenarioResult.Sample(timeline.elapsedMs(), a - prevAcked, c - prevConsumed, p99, er - prevErr));
                    }
                    prevAcked = a;
                    prevConsumed = c;
                    prevErr = er;
                }
            }, "kafkacore-chaos-sampler");
            sampler.setDaemon(true);
            sampler.start();
        }

        void stopSampler() {
            sampling = false;
            if (sampler != null) {
                sampler.interrupt();
                try {
                    sampler.join(2_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                sampler = null;
            }
        }

        List<ScenarioResult.Sample> samples() {
            synchronized (samples) {
                return List.copyOf(samples);
            }
        }
    }
}
