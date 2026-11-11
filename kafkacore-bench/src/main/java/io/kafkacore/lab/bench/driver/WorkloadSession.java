package io.kafkacore.lab.bench.driver;

import io.kafkacore.lab.bench.integrity.IntegrityTracker;
import io.kafkacore.lab.bench.metrics.LatencyRecorder;
import io.kafkacore.lab.bench.workload.WorkloadSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Owns the full lifecycle of one workload against a cluster: a fresh topic, a consumer group and
 * N paced producers, plus the shared latency recorders and {@link IntegrityTracker}.
 *
 * <p>Used directly by chaos scenarios (which need manual start/stop around fault injection) and by
 * {@link BenchmarkRunner} for fixed-duration measurements.
 */
public final class WorkloadSession implements AutoCloseable {

    private final String bootstrap;
    private final WorkloadSpec spec;
    private final String topic;
    private final String runId;
    private final LatencyRecorder publishLatency = new LatencyRecorder();
    private final LatencyRecorder endToEndLatency = new LatencyRecorder();
    private final IntegrityTracker tracker = new IntegrityTracker();
    private final List<ProducerWorker> producers = new ArrayList<>();
    private final List<ConsumerWorker> consumers = new ArrayList<>();
    private final List<Thread> threads = new ArrayList<>();
    private final Admin admin;
    private boolean deleteTopicOnClose = true;

    public WorkloadSession(String bootstrap, WorkloadSpec spec, String runId) {
        this.bootstrap = bootstrap;
        this.spec = spec.withDefaults();
        this.runId = runId;
        this.topic = this.spec.topic() + "-" + runId;
        Map<String, Object> adminProps = new HashMap<>();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
        this.admin = Admin.create(adminProps);
    }

    public String topic() { return topic; }
    public String runId() { return runId; }
    public WorkloadSpec spec() { return spec; }
    public LatencyRecorder publishLatency() { return publishLatency; }
    public LatencyRecorder endToEndLatency() { return endToEndLatency; }
    public IntegrityTracker tracker() { return tracker; }
    public List<ProducerWorker> producers() { return List.copyOf(producers); }
    public List<ConsumerWorker> consumers() { return List.copyOf(consumers); }

    public void keepTopic() {
        this.deleteTopicOnClose = false;
    }

    /** Creates the topic and waits until every partition has an elected leader. */
    public void createTopic() throws Exception {
        NewTopic nt = new NewTopic(topic, spec.partitions(), spec.replicationFactor().shortValue())
                .configs(spec.topicConfig());
        try {
            admin.createTopics(List.of(nt)).all().get(90, TimeUnit.SECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("creating topic " + topic + " timed out after 90 s (cluster overloaded or unhealthy?)", e);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            try {
                TopicDescription d = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
                if (d.partitions().size() == spec.partitions()
                        && d.partitions().stream().allMatch(p -> p.leader() != null && !p.leader().isEmpty())) {
                    return;
                }
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) throw e;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("Topic " + topic + " did not get leaders for all partitions within 60s");
    }

    /** Starts consumers and blocks until the group owns every partition of the topic. */
    public void startConsumers(Duration assignmentTimeout) throws InterruptedException {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkacore-" + runId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100");
        props.putAll(spec.consumerConfig());
        for (int i = 0; i < spec.consumers(); i++) {
            Map<String, Object> p = new HashMap<>(props);
            p.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafkacore-consumer-" + runId + "-" + i);
            ConsumerWorker w = new ConsumerWorker(i, topic, ConsumerWorker.kafkaConsumerFactory(p), endToEndLatency,
                    tracker);
            consumers.add(w);
            start(w, "consumer-" + i);
        }
        long deadline = System.nanoTime() + assignmentTimeout.toNanos();
        while (System.nanoTime() < deadline) {
            Set<TopicPartition> all = new HashSet<>();
            consumers.forEach(c -> all.addAll(c.assignment()));
            if (all.size() == spec.partitions()) return;
            Thread.sleep(100);
        }
        throw new IllegalStateException("Consumer group did not get all " + spec.partitions()
                + " partitions within " + assignmentTimeout);
    }

    /** Starts producers; the target rate is split evenly across them. */
    public void startProducers() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
        props.putAll(spec.producerConfig());
        double perProducerRate = spec.targetRatePerSec() <= 0 ? 0 : (double) spec.targetRatePerSec() / spec.producers();
        for (int i = 0; i < spec.producers(); i++) {
            Map<String, Object> p = new HashMap<>(props);
            p.put(ProducerConfig.CLIENT_ID_CONFIG, "kafkacore-producer-" + runId + "-" + i);
            ProducerWorker w = new ProducerWorker(i, topic, ProducerWorker.kafkaProducerFactory(p),
                    spec.messageSizeBytes(), perProducerRate, spec.keyStrategy(), publishLatency, tracker);
            producers.add(w);
            start(w, "producer-" + i);
        }
    }

    /** Signals producers to stop, then waits for them to flush outstanding sends. */
    public void stopProducers(Duration timeout) throws InterruptedException {
        producers.forEach(ProducerWorker::stop);
        for (ProducerWorker p : producers) {
            p.awaitFinished(timeout);
        }
    }

    /** Waits until every acknowledged message has been consumed, or the timeout passes. */
    public boolean drain(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (tracker.receivedAckedCount() >= tracker.ackedCount()) return true;
            Thread.sleep(100);
        }
        return tracker.receivedAckedCount() >= tracker.ackedCount();
    }

    public void stopConsumers(Duration timeout) throws InterruptedException {
        consumers.forEach(ConsumerWorker::stop);
        for (ConsumerWorker c : consumers) {
            c.awaitFinished(timeout);
        }
    }

    public long producedAcked() {
        return producers.stream().mapToLong(ProducerWorker::ackedCount).sum();
    }

    public long producedAckedBytes() {
        return producers.stream().mapToLong(ProducerWorker::ackedBytes).sum();
    }

    public long producedSent() {
        return producers.stream().mapToLong(ProducerWorker::sentCount).sum();
    }

    public long producerErrors() {
        return producers.stream().mapToLong(ProducerWorker::errorCount).sum();
    }

    public long consumed() {
        return consumers.stream().mapToLong(ConsumerWorker::consumedCount).sum();
    }

    public long consumedBytes() {
        return consumers.stream().mapToLong(ConsumerWorker::consumedBytes).sum();
    }

    /** Partition metadata for the session topic, for scenarios that target leaders/followers. */
    public List<TopicPartitionInfo> describePartitions() throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic).partitions();
    }

    private void start(Runnable r, String name) {
        Thread t = new Thread(r, "kafkacore-" + name + "-" + runId);
        t.setDaemon(true);
        threads.add(t);
        t.start();
    }

    @Override
    public void close() {
        producers.forEach(ProducerWorker::stop);
        consumers.forEach(ConsumerWorker::stop);
        try {
            for (Thread t : threads) {
                t.join(TimeUnit.SECONDS.toMillis(30));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            if (deleteTopicOnClose) {
                admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
            }
        } catch (Exception ignored) {
            // best effort: a topic left behind after a failed run is harmless
        } finally {
            admin.close(Duration.ofSeconds(5));
        }
    }
}
