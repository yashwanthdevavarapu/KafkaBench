package io.kafkacore.lab.chaos.fault;

import io.kafkacore.lab.chaos.cluster.ClusterTopology;

/** Injects and heals faults on cluster nodes. Implementations must make {@link #heal} idempotent. */
public interface FaultInjector {

    void inject(ClusterTopology.Node node, FaultType fault);

    void heal(ClusterTopology.Node node, FaultType fault);
}
