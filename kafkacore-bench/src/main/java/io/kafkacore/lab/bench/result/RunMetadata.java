package io.kafkacore.lab.bench.result;

import org.apache.kafka.common.utils.AppInfoParser;

import java.net.InetAddress;
import java.time.Instant;

/** Environment captured with each result so baselines can be compared like-for-like. */
public record RunMetadata(Instant startedAt, String host, String os, int cpus, String javaVersion,
                          String kafkaClientVersion, String bootstrapServers, String gitCommit) {

    public static RunMetadata capture(String bootstrap) {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown";
        }
        String git = System.getenv().getOrDefault("GITHUB_SHA", System.getProperty("kafkacore.gitCommit", "local"));
        return new RunMetadata(Instant.now(), host,
                System.getProperty("os.name") + " " + System.getProperty("os.arch"),
                Runtime.getRuntime().availableProcessors(), System.getProperty("java.version"),
                AppInfoParser.getVersion(), bootstrap, git);
    }
}
