package io.kafkacore.lab.it;

import io.kafkacore.lab.chaos.cluster.ClusterInspector;
import io.kafkacore.lab.chaos.cluster.ClusterInspector.PartitionSnapshot;
import io.kafkacore.lab.chaos.cluster.ClusterInspector.QuorumSnapshot;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.kafkacore.lab.it.KafkaClusterExtension.TOPOLOGY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies KRaft quorum and replication invariants on the healthy 3-node cluster. */
@Tag("integration")
@ExtendWith(KafkaClusterExtension.class)
class ClusterInternalsIT {

    private static ClusterInspector inspector;

    @BeforeAll
    static void connect() {
        inspector = new ClusterInspector(TOPOLOGY.bootstrapServers());
    }

    @AfterAll
    static void close() {
        inspector.close();
    }

    @Test
    void kraftQuorumHasThreeVotersAndAnElectedLeader() throws Exception {
        QuorumSnapshot q = inspector.quorum();
        assertThat(q.voters()).extracting(QuorumSnapshot.Voter::id).containsExactly(1, 2, 3);
        assertThat(q.leaderId()).isIn(1, 2, 3);
        assertThat(q.leaderEpoch()).isPositive();
        assertThat(q.highWatermark()).isPositive();
        assertThat(q.observers()).as("combined mode: every node is a voter, none are observers").isZero();
        assertThat(q.voters()).allSatisfy(v -> assertThat(v.lag()).isLessThan(1_000));
    }

    @Test
    void allBrokersAreRegisteredAndUnfenced() throws Exception {
        assertThat(inspector.liveBrokers()).containsExactly(1, 2, 3);
        assertThat(inspector.underReplicated()).isEmpty();
    }

    @Test
    void replicatedTopicHasFullIsrAndSpreadsLeadership() throws Exception {
        String topic = "it-replication-" + UUID.randomUUID().toString().substring(0, 6);
        inspector.admin().createTopics(List.of(new NewTopic(topic, 6, (short) 3)
                .configs(Map.of("min.insync.replicas", "2")))).all().get(30, TimeUnit.SECONDS);
        try {
            List<PartitionSnapshot> parts = awaitLeaders(topic, 6);
            Set<Integer> leaders = new HashSet<>();
            for (PartitionSnapshot p : parts) {
                assertThat(p.replicas()).containsExactlyInAnyOrder(1, 2, 3);
                assertThat(p.isr()).containsExactlyInAnyOrder(1, 2, 3);
                assertThat(p.replicas().get(0)).as("preferred leader is first replica").isEqualTo(p.leader());
                leaders.add(p.leader());
            }
            assertThat(leaders).as("leadership is balanced across brokers").hasSizeGreaterThan(1);

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TOPOLOGY.bootstrapServers(),
                    ProducerConfig.ACKS_CONFIG, "all"), new StringSerializer(), new StringSerializer())) {
                RecordMetadata md = producer.send(new ProducerRecord<>(topic, 0, "k", "v")).get(30, TimeUnit.SECONDS);
                assertThat(md.offset()).isZero();
                assertThat(md.partition()).isZero();
            }
        } finally {
            inspector.admin().deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void replicationFactorAboveBrokerCountIsRejected() {
        String topic = "it-rf4-" + UUID.randomUUID().toString().substring(0, 6);
        assertThatThrownBy(() -> inspector.admin().createTopics(List.of(new NewTopic(topic, 1, (short) 4))).all().get(30, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InvalidReplicationFactorException.class);
    }

    private static List<PartitionSnapshot> awaitLeaders(String topic, int partitions) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            try {
                List<PartitionSnapshot> parts = inspector.partitions(topic);
                if (parts.size() == partitions && parts.stream().allMatch(p -> p.leader() >= 0 && p.isr().size() == 3)) {
                    return parts;
                }
            } catch (Exception ignored) {
                // metadata not propagated yet
            }
            if (System.nanoTime() > deadline) throw new AssertionError("topic " + topic + " never became fully in-sync");
            Thread.sleep(200);
        }
    }
}
