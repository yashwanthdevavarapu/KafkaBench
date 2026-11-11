package io.kafkacore.lab.chaos.scenario;

import io.kafkacore.lab.chaos.cluster.ClusterInspector;
import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import io.kafkacore.lab.chaos.fault.FaultInjector;

import java.time.Duration;
import java.util.function.Consumer;

/**
 * Everything a scenario needs: where the cluster is, how to break it, how to observe it.
 *
 * @param ratePerSec       background integrity workload rate (msg/s) while faults are injected
 * @param steadyState      traffic time before the fault and after recovery
 * @param recoveryTimeout  maximum time allowed for any recovery step before the scenario fails
 */
public record ScenarioContext(ClusterTopology topology, FaultInjector injector, ClusterInspector inspector,
                              int ratePerSec, Duration steadyState, Duration recoveryTimeout, Consumer<String> log) {

    public static ScenarioContext defaults(ClusterTopology topology, FaultInjector injector, ClusterInspector inspector,
                                           Consumer<String> log) {
        return new ScenarioContext(topology, injector, inspector, 1000, Duration.ofSeconds(6), Duration.ofSeconds(90), log);
    }
}
