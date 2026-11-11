package io.kafkacore.lab.chaos.fault;

import io.kafkacore.lab.chaos.cluster.ClusterTopology;
import io.kafkacore.lab.chaos.docker.DockerCli;

/** {@link FaultInjector} that maps each {@link FaultType} onto Docker container operations. */
public final class DockerFaultInjector implements FaultInjector {

    private final DockerCli docker;
    private final String network;

    public DockerFaultInjector(DockerCli docker, String network) {
        this.docker = docker;
        this.network = network;
    }

    @Override
    public void inject(ClusterTopology.Node node, FaultType fault) {
        String c = node.container();
        switch (fault) {
            case KILL -> docker.kill(c, "SIGKILL");
            case STOP -> docker.stop(c, 30);
            case PAUSE -> docker.pause(c);
            case NETWORK_PARTITION -> docker.networkDisconnect(network, c);
        }
    }

    @Override
    public void heal(ClusterTopology.Node node, FaultType fault) {
        String c = node.container();
        switch (fault) {
            case KILL, STOP -> {
                if (!"running".equals(docker.state(c))) docker.start(c);
            }
            case PAUSE -> {
                if ("paused".equals(docker.state(c))) docker.unpause(c);
            }
            case NETWORK_PARTITION -> {
                if (!docker.networks(c).contains(network)) docker.networkConnect(network, c);
            }
        }
    }
}
