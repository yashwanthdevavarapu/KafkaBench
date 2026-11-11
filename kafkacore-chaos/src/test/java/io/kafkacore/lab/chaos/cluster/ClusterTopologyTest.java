package io.kafkacore.lab.chaos.cluster;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterTopologyTest {

    @Test
    void localTopologyMatchesComposeFile() throws Exception {
        ClusterTopology t = ClusterTopology.local();
        assertThat(t.size()).isEqualTo(3);
        assertThat(t.bootstrapServers()).isEqualTo("localhost:19092,localhost:29092,localhost:39092");
        assertThat(t.bootstrapExcluding(2)).isEqualTo("localhost:19092,localhost:39092");
        assertThat(t.containers()).containsExactly("kafka-1", "kafka-2", "kafka-3");

        // Guard against drift between the Java topology and docker/docker-compose.yml.
        Path root = Path.of("").toAbsolutePath();
        while (!Files.exists(root.resolve(t.composeFile()))) root = root.getParent();
        String compose = Files.readString(root.resolve(t.composeFile()));
        assertThat(compose).contains("name: " + t.network());
        for (ClusterTopology.Node n : t.nodes()) {
            assertThat(compose).contains("container_name: " + n.container())
                    .contains("KAFKA_NODE_ID: \"" + n.id() + "\"")
                    .contains("EXTERNAL://" + n.bootstrap());
        }
    }

    @Test
    void unknownNodeIsRejected() {
        assertThatThrownBy(() -> ClusterTopology.local().node(9)).isInstanceOf(NoSuchElementException.class);
        assertThatThrownBy(() -> new ClusterTopology(List.of(), "n", "f")).isInstanceOf(IllegalArgumentException.class);
    }
}
