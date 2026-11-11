package io.kafkacore.lab.bench.driver;

import io.kafkacore.lab.bench.integrity.IntegrityTracker;
import io.kafkacore.lab.bench.metrics.LatencyRecorder;
import io.kafkacore.lab.bench.payload.MessageCodec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * One consumer thread in the benchmark's consumer group. Verifies every payload, records
 * end-to-end latency and feeds the {@link IntegrityTracker}.
 */
public final class ConsumerWorker implements Runnable {

    private final int id;
    private final String topic;
    private final Supplier<Consumer<byte[], byte[]>> consumerFactory;
    private final LatencyRecorder endToEndLatency;
    private final IntegrityTracker tracker;
    private final LongAdder consumed = new LongAdder();
    private final LongAdder consumedBytes = new LongAdder();
    private final Set<TopicPartition> assigned = ConcurrentHashMap.newKeySet();
    private final CountDownLatch finished = new CountDownLatch(1);
    private volatile boolean running = true;
    private volatile Consumer<byte[], byte[]> consumer;

    public ConsumerWorker(int id, String topic, Supplier<Consumer<byte[], byte[]>> consumerFactory,
                          LatencyRecorder endToEndLatency, IntegrityTracker tracker) {
        this.id = id;
        this.topic = topic;
        this.consumerFactory = consumerFactory;
        this.endToEndLatency = endToEndLatency;
        this.tracker = tracker;
    }

    public static Supplier<Consumer<byte[], byte[]>> kafkaConsumerFactory(Map<String, Object> props) {
        return () -> new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    @Override
    public void run() {
        try (Consumer<byte[], byte[]> c = consumerFactory.get()) {
            consumer = c;
            c.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    assigned.removeAll(partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    assigned.addAll(partitions);
                }
            });
            while (running) {
                ConsumerRecords<byte[], byte[]> records = c.poll(Duration.ofMillis(100));
                long now = System.nanoTime();
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    handle(r, now);
                }
            }
        } catch (WakeupException | InterruptException e) {
            // normal shutdown path
        } finally {
            consumer = null;
            finished.countDown();
        }
    }

    private void handle(ConsumerRecord<byte[], byte[]> r, long nowNanos) {
        consumed.increment();
        byte[] value = r.value();
        consumedBytes.add(value == null ? 0 : value.length);
        try {
            MessageCodec.Decoded d = MessageCodec.decode(value);
            endToEndLatency.recordNanos(nowNanos - d.sendNanos());
            tracker.recordReceive(d.producerId(), d.sequence(), r.partition());
        } catch (MessageCodec.CorruptMessageException e) {
            tracker.recordCorrupt();
        }
    }

    public void stop() {
        running = false;
        Consumer<byte[], byte[]> c = consumer;
        if (c != null) {
            c.wakeup();
        }
    }

    public boolean awaitFinished(Duration timeout) throws InterruptedException {
        return finished.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public int id() { return id; }
    public Set<TopicPartition> assignment() { return Set.copyOf(assigned); }
    public long consumedCount() { return consumed.sum(); }
    public long consumedBytes() { return consumedBytes.sum(); }
}
