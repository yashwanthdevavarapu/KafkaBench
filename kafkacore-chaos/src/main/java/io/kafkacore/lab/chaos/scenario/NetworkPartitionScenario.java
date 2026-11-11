package io.kafkacore.lab.chaos.scenario;

import io.kafkacore.lab.chaos.cluster.ClusterInspector.PartitionSnapshot;
import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import io.kafkacore.lab.chaos.fault.FaultType;

import java.time.Duration;

/**
 * Isolates a follower (not the partition-0 leader, not the active controller) from the cluster
 * network.
 *
 * <p>Hypothesis: the follower stops fetching, so leaders shrink the ISR after
 * replica.lag.time.max.ms and the controller fences it after broker.session.timeout.ms. Because
 * two replicas remain in sync, min.insync.replicas=2 is still met and writes continue. When the
 * network heals the follower catches up and the ISR expands back to three.
 */
public final class NetworkPartitionScenario extends AbstractScenario {

    @Override public String id() { return "network-partition"; }
    @Override public String title() { return "Follower network isolation"; }
    @Override public FaultType fault() { return FaultType.NETWORK_PARTITION; }

    @Override
    public String description() {
        return "Disconnect a follower from the cluster network; expect ISR shrink, continued writes at "
                + "min.insync=2, then ISR expansion after the partition heals.";
    }

    @Override
    protected void execute(Run run) throws Exception {
        String topic = run.session().topic();
        int controller = run.inspector().quorum().leaderId();
        PartitionSnapshot p0 = run.inspector().partitions(topic).get(0);
        int victimId = p0.replicas().stream().filter(id -> id != p0.leader() && id != controller).findFirst()
                .orElse(p0.replicas().stream().filter(id -> id != p0.leader()).findFirst().orElseThrow());
        ClusterTopology.Node victim = run.topology().node(victimId);
        run.metric("victimNode", victimId);
        run.log(String.format("isolating follower node %d (partition 0 leader = %d, controller = %d, ISR %s)",
                victimId, p0.leader(), controller, p0.isr()));

        run.inject(victim, FaultType.NETWORK_PARTITION);

        run.awaitAndMeasure("isrShrinkMs", "partition 0 ISR shrank to exclude the isolated follower",
                run.ctx().recoveryTimeout(), () -> !run.inspector().partitions(topic).get(0).isr().contains(victimId));
        run.awaitAndMeasure("fencedAfterMs", "controller fenced the isolated broker", run.ctx().recoveryTimeout(),
                () -> !run.inspector().liveBrokers().contains(victimId));
        long before = run.session().producedAcked();
        run.hold(Duration.ofSeconds(5));
        long during = run.session().producedAcked() - before;
        run.check("writes continue with 2 in-sync replicas", during > 0, during + " msgs acked during isolation (5 s)");

        run.heal(victim);

        run.awaitAndMeasure("isrExpandMs", "ISR expanded back to all replicas after heal", run.ctx().recoveryTimeout(),
                () -> run.inspector().partitions(topic).stream().allMatch(p -> p.isr().size() == p.replicas().size()));
    }
}
