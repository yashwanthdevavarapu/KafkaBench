package io.kafkacore.lab.bench.report;

import io.kafkacore.lab.bench.result.BenchmarkResult;

import java.util.List;
import java.util.Locale;

/** Flat CSV export (one row per run) for spreadsheets and plotting tools. */
public final class CsvReport {

    public static final String HEADER = "run_id,workload,message_size_bytes,target_rate,producers,consumers,partitions,"
            + "replication_factor,produced_msgs_per_sec,produced_mb_per_sec,consumed_msgs_per_sec,"
            + "publish_p50_ms,publish_p95_ms,publish_p99_ms,publish_p999_ms,publish_max_ms,"
            + "e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,acked,lost,duplicates,corrupt,errors";

    private CsvReport() {
    }

    public static String render(List<BenchmarkResult> results) {
        StringBuilder sb = new StringBuilder(HEADER).append('\n');
        for (BenchmarkResult r : results) {
            var w = r.workload();
            var p = r.publishLatency();
            var e = r.endToEndLatency();
            sb.append(String.format(Locale.ROOT,
                    "%s,\"%s\",%d,%d,%d,%d,%d,%d,%.2f,%.3f,%.2f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%d,%d,%d,%d%n",
                    r.runId(), w.name().replace("\"", "\"\""), w.messageSizeBytes(), w.targetRatePerSec(),
                    w.producers(), w.consumers(), w.partitions(), w.replicationFactor(),
                    r.throughput().producedMsgsPerSec(), r.throughput().producedMBPerSec(),
                    r.throughput().consumedMsgsPerSec(), p.p50Ms(), p.p95Ms(), p.p99Ms(), p.p999Ms(), p.maxMs(),
                    e.p50Ms(), e.p95Ms(), e.p99Ms(), r.integrity().acked(), r.integrity().lost(),
                    r.integrity().duplicates(), r.integrity().corrupt(), r.throughput().totalErrors()));
        }
        return sb.toString();
    }
}
