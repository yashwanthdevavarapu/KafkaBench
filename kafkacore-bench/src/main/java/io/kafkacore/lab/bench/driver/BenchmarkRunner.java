package io.kafkacore.lab.bench.driver;

import io.kafkacore.lab.bench.metrics.LatencyStats;
import io.kafkacore.lab.bench.metrics.ResourceMonitor;
import io.kafkacore.lab.bench.metrics.TimeSeriesPoint;
import io.kafkacore.lab.bench.result.BenchmarkResult;
import io.kafkacore.lab.bench.result.RunMetadata;
import io.kafkacore.lab.bench.workload.WorkloadSpec;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Runs a {@link WorkloadSpec} for a fixed duration and produces a {@link BenchmarkResult}.
 *
 * <pre>
 *  create topic -> start consumers (wait for assignment) -> start producers
 *    -> warm-up (discarded) -> measurement window (1s ticks) -> stop producers + flush
 *    -> drain consumers until every ack is consumed -> reconcile integrity -> delete topic
 * </pre>
 */
public final class BenchmarkRunner {

    private final String bootstrap;
    private final Supplier<ResourceMonitor> monitorFactory;
    private final Consumer<String> progress;

    public BenchmarkRunner(String bootstrap, Supplier<ResourceMonitor> monitorFactory, Consumer<String> progress) {
        this.bootstrap = bootstrap;
        this.monitorFactory = monitorFactory;
        this.progress = progress;
    }

    public BenchmarkRunner(String bootstrap) {
        this(bootstrap, ResourceMonitor::none, s -> { });
    }

    /** Runs every concrete spec produced by {@link WorkloadSpec#expand()}. */
    public List<BenchmarkResult> runAll(WorkloadSpec template) throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();
        for (WorkloadSpec spec : template.expand()) {
            results.add(run(spec));
        }
        return results;
    }

    public BenchmarkResult run(WorkloadSpec rawSpec) throws Exception {
        WorkloadSpec spec = rawSpec.withDefaults();
        String runId = UUID.randomUUID().toString().substring(0, 8);
        RunMetadata metadata = RunMetadata.capture(bootstrap);
        progress.accept(String.format("[%s] %s: %d producer(s) x %d B, rate=%s, %d partitions RF=%d, %ds (+%ds warm-up)",
                runId, spec.name(), spec.producers(), spec.messageSizeBytes(),
                spec.targetRatePerSec() == 0 ? "max" : spec.targetRatePerSec() + "/s",
                spec.partitions(), spec.replicationFactor(), spec.durationSec(), spec.warmupSec()));

        try (WorkloadSession session = new WorkloadSession(bootstrap, spec, runId);
             ResourceMonitor monitor = monitorFactory.get()) {
            session.createTopic();
            session.startConsumers(Duration.ofSeconds(60));
            session.startProducers();

            // Warm-up starts once the pipeline is live (first ack + first consumed record), not at a
            // fixed offset: on a fresh topic, leader/fetcher start-up can take seconds on slow hosts.
            long liveStart = System.nanoTime();
            while ((session.producedAcked() == 0 || session.consumed() == 0)
                    && System.nanoTime() - liveStart < 120_000_000_000L) {
                Thread.sleep(50);
            }
            if (session.producedAcked() == 0) {
                throw new IllegalStateException("no acknowledgement within 120 s — is the cluster healthy? last error: "
                        + session.producers().get(0).lastError());
            }
            progress.accept(String.format("[%s] pipeline live after %d ms (first ack + first consume)",
                    runId, (System.nanoTime() - liveStart) / 1_000_000));

            if (spec.warmupSec() > 0) {
                Thread.sleep(spec.warmupSec() * 1000L);
            }
            // Discard warm-up: JIT, connection setup and metadata fetches distort the first seconds.
            session.publishLatency().reset();
            session.endToEndLatency().reset();
            monitor.reset();
            monitor.start();

            long startAcked = session.producedAcked();
            long startAckedBytes = session.producedAckedBytes();
            long startConsumed = session.consumed();
            long startConsumedBytes = session.consumedBytes();
            long startErrors = session.producerErrors();
            long windowStart = System.nanoTime();

            List<TimeSeriesPoint> timeline = new ArrayList<>();
            long prevAcked = startAcked, prevConsumed = startConsumed, prevErrors = startErrors;
            for (int sec = 1; sec <= spec.durationSec(); sec++) {
                long target = windowStart + sec * 1_000_000_000L;
                long sleepNanos = target - System.nanoTime();
                if (sleepNanos > 0) Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
                LatencyStats pub = session.publishLatency().tick();
                LatencyStats e2e = session.endToEndLatency().tick();
                long acked = session.producedAcked(), consumed = session.consumed(), errs = session.producerErrors();
                timeline.add(new TimeSeriesPoint(sec, acked - prevAcked, consumed - prevConsumed, pub.p99Ms(),
                        e2e.p99Ms(), errs - prevErrors));
                if (sec % 5 == 0 || sec == spec.durationSec()) {
                    progress.accept(String.format("[%s]   t=%2ds  produced=%,d msg/s  consumed=%,d msg/s  publish p99=%.2f ms",
                            runId, sec, acked - prevAcked, consumed - prevConsumed, pub.p99Ms()));
                }
                prevAcked = acked;
                prevConsumed = consumed;
                prevErrors = errs;
            }
            double seconds = (System.nanoTime() - windowStart) / 1e9;
            long windowAcked = session.producedAcked() - startAcked;
            long windowAckedBytes = session.producedAckedBytes() - startAckedBytes;
            long windowConsumed = session.consumed() - startConsumed;
            long windowConsumedBytes = session.consumedBytes() - startConsumedBytes;
            monitor.stop();

            session.stopProducers(Duration.ofSeconds(60));
            boolean drained = session.drain(Duration.ofSeconds(30));
            session.stopConsumers(Duration.ofSeconds(30));
            if (!drained) {
                progress.accept("[" + runId + "] WARNING: consumers did not catch up with all acknowledged messages");
            }

            BenchmarkResult.ThroughputStats tp = new BenchmarkResult.ThroughputStats(
                    round(windowAcked / seconds), round(windowAckedBytes / seconds / (1024 * 1024)),
                    round(windowConsumed / seconds), round(windowConsumedBytes / seconds / (1024 * 1024)),
                    session.producedSent(), session.producedAcked(), session.producerErrors(), round(seconds));
            BenchmarkResult result = new BenchmarkResult(runId, spec, metadata, tp,
                    session.publishLatency().summary(), session.endToEndLatency().summary(),
                    session.tracker().report(), monitor.summary(), List.copyOf(timeline));
            progress.accept(String.format("[%s] done: %,.0f msg/s (%.2f MB/s), publish p95=%.2f p99=%.2f ms, e2e p99=%.2f ms, lost=%d dup=%d",
                    runId, tp.producedMsgsPerSec(), tp.producedMBPerSec(), result.publishLatency().p95Ms(),
                    result.publishLatency().p99Ms(), result.endToEndLatency().p99Ms(), result.integrity().lost(),
                    result.integrity().duplicates()));
            return result;
        }
    }

    private static double round(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
