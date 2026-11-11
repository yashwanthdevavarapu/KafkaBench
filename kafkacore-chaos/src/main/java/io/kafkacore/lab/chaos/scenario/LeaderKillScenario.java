package io.kafkacore.lab.chaos.scenario;

import io.kafkacore.lab.chaos.cluster.ClusterInspector.PartitionSnapshot;
import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import io.kafkacore.lab.chaos.fault.FaultType;

import java.time.Duration;
import java.util.List;

/**
 * Hard-kills the broker leading partition 0 of the workload topic.
 *
 * <p>Hypothesis: the controller notices the missed heartbeats (broker.session.timeout.ms), fences
 * the broker, elects a new leader from the ISR for each affected partition, and because acks=all
 * with min.insync.replicas=2 no acknowledged write is lost. On restart the broker truncates to the
 * leader's high watermark, catches up and re-joins the ISR.
 */
public final class LeaderKillScenario extends AbstractScenario {

    @Override public String id() { return "leader-kill"; }
    @Override public String title() { return "Partition leader hard crash"; }
    @Override public FaultType fault() { return FaultType.KILL; }

    @Override
    public String description() {
        return "SIGKILL the broker leading partition 0; expect ISR-based leader election, zero acked-message loss "
                + "and ISR re-join after restart.";
    }

    @Override
    protected void execute(Run run) throws Exception {
        String topic = run.session().topic();
        PartitionSnapshot p0 = run.inspector().partitions(topic).get(0);
        int victimId = p0.leader();
        ClusterTopology.Node victim = run.topology().node(victimId);
        long led = run.inspector().partitions(topic).stream().filter(p -> p.leader() == victimId).count();
        int controller = run.inspector().quorum().leaderId();
        run.metric("victimNode", victimId);
        run.metric("victimWasActiveController", victimId == controller);
        run.metric("partitionsLedByVictim", led);
        run.log(String.format("partition 0 leader = node %d (leads %d/%d partitions, ISR %s); active controller = node %d",
                victimId, led, run.session().spec().partitions(), p0.isr(), controller));

        run.inject(victim, FaultType.KILL);

        run.awaitAndMeasure("leaderElectionMs", "new leader elected for partition 0", run.ctx().recoveryTimeout(), () -> {
            int l = run.inspector().partitions(topic).get(0).leader();
            return l >= 0 && l != victimId;
        });
        run.awaitAndMeasure("allLeadersMovedMs", "all partitions led by surviving brokers", run.ctx().recoveryTimeout(),
                () -> run.inspector().partitions(topic).stream().allMatch(p -> p.leader() >= 0 && p.leader() != victimId));
        List<PartitionSnapshot> during = run.inspector().partitions(topic);
        boolean shrunk = during.stream().noneMatch(p -> p.isr().contains(victimId));
        run.check("ISR shrank to exclude the dead broker", shrunk, "partition 0 ISR now " + during.get(0).isr());
        run.observe("partition 0 leader is now node " + during.get(0).leader());

        run.hold(Duration.ofSeconds(5));
        run.heal(victim);

        run.awaitAndMeasure("isrRejoinMs", "restarted broker re-joined the ISR of every partition", run.ctx().recoveryTimeout(),
                () -> run.inspector().partitions(topic).stream().allMatch(p -> p.isr().contains(victimId)));
    }
}
