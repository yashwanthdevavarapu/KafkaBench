package io.kafkacore.lab.chaos.scenario;

import io.kafkacore.lab.chaos.cluster.ClusterInspector.PartitionSnapshot;
import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import io.kafkacore.lab.chaos.fault.FaultType;

import java.time.Duration;
import java.util.List;

/**
 * Freezes (docker pause) a partition leader that is not the active controller, modelling a
 * stop-the-world GC pause or a hung disk: the JVM is alive but sends no heartbeats.
 *
 * <p>Hypothesis: after broker.session.timeout.ms the controller fences the frozen broker and moves
 * leadership; producers time out in-flight requests and retry against the new leader without
 * duplicating (idempotence). When the process thaws it discovers it was fenced, re-registers,
 * and re-joins the ISR.
 */
public final class BrokerPauseScenario extends AbstractScenario {

    @Override public String id() { return "broker-pause"; }
    @Override public String title() { return "Broker freeze (GC-pause simulation)"; }
    @Override public FaultType fault() { return FaultType.PAUSE; }

    @Override
    public String description() {
        return "Freeze a non-controller partition leader for 15 s; expect fencing, leader movement, "
                + "idempotent producer retries and clean re-join after thaw.";
    }

    @Override
    protected void execute(Run run) throws Exception {
        String topic = run.session().topic();
        int controller = run.inspector().quorum().leaderId();
        List<PartitionSnapshot> parts = run.inspector().partitions(topic);
        PartitionSnapshot target = parts.stream().filter(p -> p.leader() != controller).findFirst().orElse(parts.get(0));
        int victimId = target.leader();
        ClusterTopology.Node victim = run.topology().node(victimId);
        run.metric("victimNode", victimId);
        run.metric("victimWasActiveController", victimId == controller);
        run.log("freezing node " + victimId + " (leader of partition " + target.partition() + "); active controller = node " + controller);

        run.inject(victim, FaultType.PAUSE);

        run.awaitAndMeasure("fencedAfterMs", "controller fenced the frozen broker", run.ctx().recoveryTimeout(),
                () -> !run.inspector().liveBrokers().contains(victimId));
        run.awaitAndMeasure("leadershipMovedMs", "leadership moved off the frozen broker", run.ctx().recoveryTimeout(),
                () -> run.inspector().partitions(topic).stream().noneMatch(p -> p.leader() == victimId));

        run.hold(Duration.ofSeconds(8));
        run.heal(victim);

        run.awaitAndMeasure("rejoinAfterThawMs", "thawed broker re-registered and re-joined every ISR", run.ctx().recoveryTimeout(),
                () -> run.inspector().liveBrokers().contains(victimId)
                        && run.inspector().partitions(topic).stream().allMatch(p -> p.isr().contains(victimId)));
    }
}
