package io.kafkacore.lab.chaos.cluster;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeMetadataQuorumOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Read-only view of cluster internals through the Admin API: KRaft quorum state, broker
 * liveness and per-partition leader/ISR. All calls use short timeouts because they are polled
 * while nodes are failing.
 */
public final class ClusterInspector implements AutoCloseable {

    private static final int CALL_TIMEOUT_MS = 5_000;

    private final Admin admin;

    public ClusterInspector(String bootstrap) {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, CALL_TIMEOUT_MS);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, CALL_TIMEOUT_MS * 2);
        props.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1_000);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafkacore-inspector");
        this.admin = Admin.create(props);
    }

    public Admin admin() {
        return admin;
    }

    /** KRaft metadata quorum: leader, epoch, high watermark and per-voter replication lag. */
    public QuorumSnapshot quorum() throws Exception {
        QuorumInfo q = admin.describeMetadataQuorum(new DescribeMetadataQuorumOptions().timeoutMs(CALL_TIMEOUT_MS))
                .quorumInfo().get(CALL_TIMEOUT_MS * 2L, TimeUnit.MILLISECONDS);
        List<QuorumSnapshot.Voter> voters = new ArrayList<>();
        for (QuorumInfo.ReplicaState v : q.voters()) {
            voters.add(new QuorumSnapshot.Voter(v.replicaId(), v.logEndOffset(),
                    v.lastCaughtUpTimestamp().orElse(-1L), Math.max(0, q.highWatermark() - v.logEndOffset())));
        }
        voters.sort((a, b) -> Integer.compare(a.id(), b.id()));
        return new QuorumSnapshot(q.leaderId(), q.leaderEpoch(), q.highWatermark(), voters, q.observers().size());
    }

    /** Ids of brokers currently registered and unfenced. */
    public Set<Integer> liveBrokers() throws Exception {
        Collection<Node> nodes = admin.describeCluster(new DescribeClusterOptions().timeoutMs(CALL_TIMEOUT_MS))
                .nodes().get(CALL_TIMEOUT_MS * 2L, TimeUnit.MILLISECONDS);
        Set<Integer> ids = new TreeSet<>();
        nodes.forEach(n -> ids.add(n.id()));
        return ids;
    }

    public List<PartitionSnapshot> partitions(String topic) throws Exception {
        TopicDescription d = admin.describeTopics(List.of(topic), new DescribeTopicsOptions().timeoutMs(CALL_TIMEOUT_MS))
                .allTopicNames().get(CALL_TIMEOUT_MS * 2L, TimeUnit.MILLISECONDS).get(topic);
        List<PartitionSnapshot> out = new ArrayList<>();
        for (TopicPartitionInfo p : d.partitions()) {
            out.add(new PartitionSnapshot(topic, p.partition(),
                    p.leader() == null || p.leader().isEmpty() ? -1 : p.leader().id(),
                    p.replicas().stream().map(Node::id).toList(),
                    p.isr().stream().map(Node::id).toList()));
        }
        return out;
    }

    /** Under-replicated partitions (ISR smaller than replica set) across all non-internal topics. */
    public List<PartitionSnapshot> underReplicated() throws Exception {
        Set<String> topics = admin.listTopics(new ListTopicsOptions().timeoutMs(CALL_TIMEOUT_MS).listInternal(true))
                .names().get(CALL_TIMEOUT_MS * 2L, TimeUnit.MILLISECONDS);
        List<PartitionSnapshot> urp = new ArrayList<>();
        if (topics.isEmpty()) return urp;
        Map<String, TopicDescription> all = admin.describeTopics(topics, new DescribeTopicsOptions().timeoutMs(CALL_TIMEOUT_MS))
                .allTopicNames().get(CALL_TIMEOUT_MS * 2L, TimeUnit.MILLISECONDS);
        for (TopicDescription d : all.values()) {
            for (TopicPartitionInfo p : d.partitions()) {
                if (p.isr().size() < p.replicas().size()) {
                    urp.add(new PartitionSnapshot(d.name(), p.partition(),
                            p.leader() == null || p.leader().isEmpty() ? -1 : p.leader().id(),
                            p.replicas().stream().map(Node::id).toList(), p.isr().stream().map(Node::id).toList()));
                }
            }
        }
        return urp;
    }

    /** Full health snapshot; never throws (failures are captured in {@link ClusterSnapshot#error()}). */
    public ClusterSnapshot snapshot() {
        try {
            QuorumSnapshot q = quorum();
            Set<Integer> live = liveBrokers();
            List<PartitionSnapshot> urp = underReplicated();
            return new ClusterSnapshot(q, live, urp.size(), null);
        } catch (Exception e) {
            Throwable c = e.getCause() != null ? e.getCause() : e;
            return new ClusterSnapshot(null, Set.of(), -1, c.getClass().getSimpleName() + ": " + c.getMessage());
        }
    }

    /** Healthy = quorum has a leader, all expected brokers are live, no under-replicated partitions. */
    public boolean healthy(int expectedBrokers) {
        ClusterSnapshot s = snapshot();
        return s.error() == null && s.quorum().leaderId() >= 0 && s.liveBrokers().size() == expectedBrokers
                && s.underReplicatedPartitions() == 0
                && s.quorum().voters().stream().allMatch(v -> v.lag() < 1_000);
    }

    @Override
    public void close() {
        admin.close(Duration.ofSeconds(5));
    }

    /** KRaft quorum view. */
    public record QuorumSnapshot(int leaderId, long leaderEpoch, long highWatermark, List<Voter> voters, int observers) {
        public record Voter(int id, long logEndOffset, long lastCaughtUpTimestamp, long lag) {
        }
    }

    /** Leader / replicas / ISR for one partition. */
    public record PartitionSnapshot(String topic, int partition, int leader, List<Integer> replicas, List<Integer> isr) {
    }

    /** Point-in-time cluster health. */
    public record ClusterSnapshot(QuorumSnapshot quorum, Set<Integer> liveBrokers, int underReplicatedPartitions,
                                  String error) {
    }
}
