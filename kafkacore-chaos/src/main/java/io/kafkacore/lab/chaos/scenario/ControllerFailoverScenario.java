package io.kafkacore.lab.chaos.scenario;

import io.kafkacore.lab.chaos.cluster.ClusterInspector.QuorumSnapshot;
import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import io.kafkacore.lab.chaos.fault.FaultType;
import org.apache.kafka.clients.admin.NewTopic;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Hard-kills the active KRaft controller (the Raft leader of {@code __cluster_metadata}).
 *
 * <p>Hypothesis: the remaining voters time out (controller.quorum.election.timeout.ms), run a
 * Raft election in a higher epoch, and the new controller keeps serving metadata writes (topic
 * creation) while one of three voters is down. The restarted voter catches up from the new
 * leader's log and its replication lag returns to zero.
 */
public final class ControllerFailoverScenario extends AbstractScenario {

    @Override public String id() { return "controller-failover"; }
    @Override public String title() { return "KRaft active controller failover"; }
    @Override public FaultType fault() { return FaultType.KILL; }

    @Override
    public String description() {
        return "SIGKILL the active KRaft controller; expect a Raft election in a higher epoch, uninterrupted "
                + "metadata writes and the old voter catching up after restart.";
    }

    @Override
    protected void execute(Run run) throws Exception {
        QuorumSnapshot before = run.inspector().quorum();
        int oldLeader = before.leaderId();
        long oldEpoch = before.leaderEpoch();
        ClusterTopology.Node victim = run.topology().node(oldLeader);
        run.metric("oldControllerId", oldLeader);
        run.metric("epochBefore", oldEpoch);
        run.log(String.format("active controller = node %d, epoch %d, metadata high-watermark %d, voters %s",
                oldLeader, oldEpoch, before.highWatermark(), before.voters().stream().map(QuorumSnapshot.Voter::id).toList()));

        run.inject(victim, FaultType.KILL);

        run.awaitAndMeasure("controllerFailoverMs", "new KRaft controller elected", run.ctx().recoveryTimeout(), () -> {
            QuorumSnapshot q = run.inspector().quorum();
            return q.leaderId() >= 0 && q.leaderId() != oldLeader;
        });
        QuorumSnapshot after = run.inspector().quorum();
        run.metric("newControllerId", after.leaderId());
        run.metric("epochAfter", after.leaderEpoch());
        run.check("leader epoch increased (Raft term bump)", after.leaderEpoch() > oldEpoch,
                "epoch " + oldEpoch + " → " + after.leaderEpoch() + ", new controller node " + after.leaderId());

        // Metadata write path: topic creation must commit through the new controller with 2/3 voters.
        String probe = "kafkacore-probe-" + UUID.randomUUID().toString().substring(0, 6);
        // Retried like a real client: the first attempt may be routed to the dead node through stale metadata.
        long t0 = System.nanoTime();
        long deadline = t0 + TimeUnit.SECONDS.toNanos(30);
        boolean created = false;
        String detail = "not attempted";
        int attempts = 0;
        while (!created && System.nanoTime() < deadline) {
            attempts++;
            try {
                run.inspector().admin().createTopics(List.of(new NewTopic(probe, 3, (short) 2))).all().get(15, TimeUnit.SECONDS);
                created = true;
            } catch (Exception e) {
                Throwable c = e.getCause() != null ? e.getCause() : e;
                if (c instanceof org.apache.kafka.common.errors.TopicExistsException) {
                    created = true; // an earlier timed-out attempt actually committed
                } else {
                    detail = String.valueOf(c);
                    Thread.sleep(500);
                }
            }
        }
        if (created) {
            long ms = (System.nanoTime() - t0) / 1_000_000;
            run.metric("metadataWriteDuringFailureMs", ms);
            detail = "created " + probe + " (RF=2) in " + ms + " ms (" + attempts + " attempt(s)) with one voter down";
        }
        run.check("metadata writes succeed with 2/3 voters", created, detail);

        run.hold(Duration.ofSeconds(3));
        run.heal(victim);

        run.awaitAndMeasure("voterCatchUpMs", "restarted voter caught up with the metadata log", run.ctx().recoveryTimeout(), () -> {
            QuorumSnapshot q = run.inspector().quorum();
            return q.voters().stream().anyMatch(v -> v.id() == oldLeader && v.lag() == 0)
                    && run.inspector().liveBrokers().contains(oldLeader);
        });
        try {
            run.inspector().admin().deleteTopics(List.of(probe)).all().get(30, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // probe cleanup is best effort
        }
    }
}
