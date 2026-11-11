package io.kafkacore.lab.chaos.scenario;

import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import io.kafkacore.lab.chaos.fault.FaultType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Gracefully restarts every node one at a time, active controller last — the procedure used for
 * configuration changes and version upgrades.
 *
 * <p>Hypothesis: controlled shutdown migrates leadership before each broker exits, so the rolling
 * restart completes with zero acknowledged-message loss and the cluster returns to full ISR
 * between every step.
 */
public final class RollingRestartScenario extends AbstractScenario {

    @Override public String id() { return "rolling-restart"; }
    @Override public String title() { return "Rolling restart (controlled shutdown)"; }
    @Override public FaultType fault() { return FaultType.STOP; }

    @Override
    public String description() {
        return "SIGTERM + restart each node in turn (controller last), waiting for full ISR between steps; "
                + "expect zero loss and continuous availability.";
    }

    @Override
    protected void execute(Run run) throws Exception {
        int controller = run.inspector().quorum().leaderId();
        List<ClusterTopology.Node> order = new ArrayList<>(run.topology().nodes());
        order.sort(Comparator.comparing((ClusterTopology.Node n) -> n.id() == controller).thenComparing(ClusterTopology.Node::id));
        run.log("restart order: " + order.stream().map(ClusterTopology.Node::id).toList() + " (active controller " + controller + " last)");
        int size = run.topology().size();
        for (ClusterTopology.Node node : order) {
            run.inject(node, FaultType.STOP);
            run.heal(node);
            run.awaitAndMeasure("node" + node.id() + "RestartToHealthyMs",
                    "node " + node.id() + " back and cluster fully in sync", run.ctx().recoveryTimeout(),
                    () -> run.inspector().healthy(size));
        }
    }
}
