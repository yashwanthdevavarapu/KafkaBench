package io.kafkacore.lab.chaos.cluster;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * Maps KRaft node ids to Docker containers and host-reachable listeners.
 *
 * @param network     the Docker network the nodes share (target of partition faults)
 * @param composeFile compose file that defines the cluster
 */
public record ClusterTopology(List<Node> nodes, String network, String composeFile) {

    /** One combined broker+controller node. */
    public record Node(int id, String container, String bootstrap) {
    }

    public ClusterTopology {
        if (nodes.isEmpty()) throw new IllegalArgumentException("topology needs at least one node");
        nodes = List.copyOf(nodes);
    }

    /** The topology defined by {@code docker/docker-compose.yml}. */
    public static ClusterTopology local() {
        return new ClusterTopology(List.of(
                new Node(1, "kafka-1", "localhost:19092"),
                new Node(2, "kafka-2", "localhost:29092"),
                new Node(3, "kafka-3", "localhost:39092")),
                "kafkacore-net", "docker/docker-compose.yml");
    }

    public String bootstrapServers() {
        return nodes.stream().map(Node::bootstrap).collect(Collectors.joining(","));
    }

    public Node node(int id) {
        return nodes.stream().filter(n -> n.id() == id).findFirst()
                .orElseThrow(() -> new NoSuchElementException("no node with id " + id));
    }

    public List<String> containers() {
        return nodes.stream().map(Node::container).toList();
    }

    /** Bootstrap list excluding one node, so clients never depend on a node we are about to break. */
    public String bootstrapExcluding(int nodeId) {
        return nodes.stream().filter(n -> n.id() != nodeId).map(Node::bootstrap).collect(Collectors.joining(","));
    }

    public int size() {
        return nodes.size();
    }
}
