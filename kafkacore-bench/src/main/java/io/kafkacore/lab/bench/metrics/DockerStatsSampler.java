package io.kafkacore.lab.bench.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafkacore.lab.bench.process.CommandExecutor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Samples broker containers via {@code docker stats --no-stream}: CPU %, memory and network/block I/O.
 * Unavailable containers (e.g. killed by a fault scenario) are simply absent from the sample.
 */
public final class DockerStatsSampler implements ResourceSampler {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern SIZE = Pattern.compile("([0-9.]+)\\s*([kKMGT]?i?B)");

    private final CommandExecutor executor;
    private final List<String> containers;

    public DockerStatsSampler(CommandExecutor executor, List<String> containers) {
        this.executor = executor;
        this.containers = List.copyOf(containers);
    }

    @Override
    public Map<String, Double> sample() {
        List<String> cmd = new ArrayList<>(List.of("docker", "stats", "--no-stream", "--format", "{{json .}}"));
        cmd.addAll(containers);
        CommandExecutor.CommandResult result = executor.execute(cmd, Duration.ofSeconds(15));
        Map<String, Double> out = new LinkedHashMap<>();
        for (String line : result.stdout().split("\\R")) {
            if (line.isBlank()) continue;
            parseLine(line, out);
        }
        return out;
    }

    static void parseLine(String line, Map<String, Double> out) {
        try {
            JsonNode n = JSON.readTree(line);
            String name = n.path("Name").asText("unknown");
            String prefix = "broker." + name + ".";
            out.put(prefix + "cpu.pct", percent(n.path("CPUPerc").asText("0%")));
            String mem = n.path("MemUsage").asText("0B / 0B");
            out.put(prefix + "mem.mb", toMiB(mem.split("/")[0]));
            String[] net = n.path("NetIO").asText("0B / 0B").split("/");
            out.put(prefix + "net.rx.mb", toMiB(net[0]));
            if (net.length > 1) out.put(prefix + "net.tx.mb", toMiB(net[1]));
            String[] blk = n.path("BlockIO").asText("0B / 0B").split("/");
            if (blk.length > 1) out.put(prefix + "disk.write.mb", toMiB(blk[1]));
        } catch (Exception ignored) {
            // A malformed line (e.g. container disappeared mid-sample) is skipped rather than failing the run.
        }
    }

    static double percent(String s) {
        String v = s.replace("%", "").trim();
        return v.isEmpty() || v.equals("--") ? 0.0 : Double.parseDouble(v);
    }

    static double toMiB(String s) {
        Matcher m = SIZE.matcher(s.trim());
        if (!m.find()) return 0.0;
        double v = Double.parseDouble(m.group(1));
        String unit = m.group(2).toUpperCase(Locale.ROOT);
        double bytes = switch (unit) {
            case "KIB" -> v * 1024;
            case "KB" -> v * 1000;
            case "MIB" -> v * 1024 * 1024;
            case "MB" -> v * 1000 * 1000;
            case "GIB" -> v * 1024 * 1024 * 1024;
            case "GB" -> v * 1e9;
            case "TIB" -> v * Math.pow(1024, 4);
            case "TB" -> v * 1e12;
            default -> v;
        };
        return Math.round(bytes / (1024.0 * 1024.0) * 100.0) / 100.0;
    }
}
