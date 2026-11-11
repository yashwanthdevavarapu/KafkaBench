package io.kafkacore.lab.bench.report;

import io.kafkacore.lab.bench.regression.RegressionReport;
import io.kafkacore.lab.bench.result.BenchmarkResult;

import java.util.List;
import java.util.Locale;

/** Renders results and regression comparisons as GitHub-flavoured Markdown (used in CI summaries). */
public final class MarkdownReport {

    private MarkdownReport() {
    }

    public static String results(List<BenchmarkResult> results) {
        StringBuilder sb = new StringBuilder("## KafkaCore Lab — benchmark results\n\n");
        sb.append("| Workload | Size (B) | Target rate | Producers | msg/s | MB/s | Publish p50 | p95 | p99 | p99.9 | E2E p99 | Lost | Dup |\n");
        sb.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for (BenchmarkResult r : results) {
            var w = r.workload();
            sb.append(String.format(Locale.ROOT,
                    "| %s | %d | %s | %d | %,.0f | %.2f | %.2f ms | %.2f ms | %.2f ms | %.2f ms | %.2f ms | %d | %d |%n",
                    w.name(), w.messageSizeBytes(), w.targetRatePerSec() == 0 ? "max" : w.targetRatePerSec() + "/s",
                    w.producers(), r.throughput().producedMsgsPerSec(), r.throughput().producedMBPerSec(),
                    r.publishLatency().p50Ms(), r.publishLatency().p95Ms(), r.publishLatency().p99Ms(),
                    r.publishLatency().p999Ms(), r.endToEndLatency().p99Ms(), r.integrity().lost(),
                    r.integrity().duplicates()));
        }
        return sb.toString();
    }

    public static String regression(RegressionReport report) {
        StringBuilder sb = new StringBuilder();
        sb.append("## Regression check: ").append(icon(report.verdict().name())).append(' ').append(report.verdict()).append("\n\n");
        for (RegressionReport.WorkloadComparison w : report.workloads()) {
            sb.append("### ").append(w.workload()).append(" — ").append(icon(w.verdict().name())).append(' ')
                    .append(w.verdict()).append("\n\n");
            sb.append("| Metric | Baseline | Current | Δ | Threshold | Status |\n|---|---:|---:|---:|---:|---|\n");
            for (RegressionReport.MetricComparison m : w.metrics()) {
                sb.append(String.format(Locale.ROOT, "| %s | %s | %s | %+.1f%% | %s | %s %s%s |%n",
                        m.metric(), SvgCharts.fmt(m.baseline()), SvgCharts.fmt(m.current()), m.deltaPct(),
                        m.thresholdPct() == 0 ? "—" : "±" + SvgCharts.fmt(m.thresholdPct()) + "%",
                        icon(m.status().name()), m.status(), m.note().isEmpty() ? "" : " — " + m.note()));
            }
            sb.append('\n');
        }
        if (!report.unmatched().isEmpty()) {
            sb.append("Unmatched workloads: ").append(String.join(", ", report.unmatched())).append('\n');
        }
        return sb.toString();
    }

    private static String icon(String status) {
        return switch (status) {
            case "FAIL" -> "❌";
            case "WARN" -> "⚠️";
            case "IMPROVED" -> "🚀";
            default -> "✅";
        };
    }
}
