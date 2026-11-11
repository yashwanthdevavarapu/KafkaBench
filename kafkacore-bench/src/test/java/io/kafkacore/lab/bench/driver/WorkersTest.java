package io.kafkacore.lab.bench.driver;

import io.kafkacore.lab.bench.integrity.IntegrityReport;
import io.kafkacore.lab.bench.integrity.IntegrityTracker;
import io.kafkacore.lab.bench.metrics.LatencyRecorder;
import io.kafkacore.lab.bench.payload.MessageCodec;
import io.kafkacore.lab.bench.workload.WorkloadSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class WorkersTest {

    @Test
    void producerStampsGapFreeSequencesAndRecordsAcks() throws Exception {
        MockProducer<byte[], byte[]> mock = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        IntegrityTracker tracker = new IntegrityTracker();
        LatencyRecorder latency = new LatencyRecorder();
        ProducerWorker w = new ProducerWorker(3, "t", () -> mock, 128, 2_000, WorkloadSpec.KeyStrategy.RANDOM, latency, tracker);

        Thread t = new Thread(w);
        t.start();
        Thread.sleep(300);
        w.stop();
        assertThat(w.awaitFinished(Duration.ofSeconds(5))).isTrue();

        List<ProducerRecord<byte[], byte[]>> sent = mock.history();
        assertThat(sent).hasSizeGreaterThan(100);
        assertThat(w.sentCount()).isEqualTo(sent.size());
        assertThat(w.ackedCount()).isEqualTo(sent.size());
        assertThat(w.ackedBytes()).isEqualTo(128L * sent.size());
        for (int i = 0; i < sent.size(); i++) {
            MessageCodec.Decoded d = MessageCodec.decode(sent.get(i).value());
            assertThat(d.producerId()).isEqualTo(3);
            assertThat(d.sequence()).isEqualTo(i);
            assertThat(sent.get(i).key()).hasSize(8);
        }
        assertThat(tracker.ackedCount()).isEqualTo(sent.size());
        assertThat(latency.summary().count()).isEqualTo(sent.size());
    }

    @Test
    void producerCountsSendFailuresWithoutAcking() throws Exception {
        MockProducer<byte[], byte[]> mock = new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        mock.sendException = new org.apache.kafka.common.errors.TimeoutException("broker unavailable");
        IntegrityTracker tracker = new IntegrityTracker();
        ProducerWorker w = new ProducerWorker(1, "t", () -> mock, 64, 1_000, WorkloadSpec.KeyStrategy.NONE,
                new LatencyRecorder(), tracker);
        Thread t = new Thread(w);
        t.start();
        Thread.sleep(100);
        w.stop();
        w.awaitFinished(Duration.ofSeconds(5));

        assertThat(w.errorCount()).isPositive().isEqualTo(w.sentCount());
        assertThat(w.ackedCount()).isZero();
        assertThat(w.lastError()).hasMessageContaining("broker unavailable");
        IntegrityReport r = tracker.report();
        assertThat(r.sendFailed()).isEqualTo(w.errorCount());
        assertThat(r.lost()).isZero(); // never acked => not counted as lost
    }

    @Test
    void consumerVerifiesPayloadsAndFlagsCorruption() throws Exception {
        MockConsumer<byte[], byte[]> mock = new MockConsumer<>("earliest");
        TopicPartition tp = new TopicPartition("t", 0);
        MessageCodec codec = new MessageCodec(100, 1);
        byte[] good1 = codec.encode(9, 0, System.nanoTime());
        byte[] good2 = codec.encode(9, 1, System.nanoTime());
        byte[] corrupt = codec.encode(9, 2, System.nanoTime());
        corrupt[50] ^= 0x7F;

        mock.schedulePollTask(() -> {
            mock.rebalance(List.of(tp));
            mock.updateBeginningOffsets(Map.of(tp, 0L));
            mock.seek(tp, 0);
        });
        mock.schedulePollTask(() -> {
            mock.addRecord(new ConsumerRecord<>("t", 0, 0, null, good1));
            mock.addRecord(new ConsumerRecord<>("t", 0, 1, null, good2));
            mock.addRecord(new ConsumerRecord<>("t", 0, 2, null, corrupt));
            mock.addRecord(new ConsumerRecord<>("t", 0, 3, null, good2)); // re-delivery
        });

        IntegrityTracker tracker = new IntegrityTracker();
        LatencyRecorder e2e = new LatencyRecorder();
        ConsumerWorker w = new ConsumerWorker(0, "t", () -> mock, e2e, tracker);
        Thread t = new Thread(w);
        t.start();
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (w.consumedCount() < 4 && System.nanoTime() < deadline) Thread.sleep(20);
        w.stop();
        assertThat(w.awaitFinished(Duration.ofSeconds(5))).isTrue();

        IntegrityReport r = tracker.report();
        assertThat(w.consumedCount()).isEqualTo(4);
        assertThat(r.received()).isEqualTo(2);
        assertThat(r.corrupt()).isEqualTo(1);
        assertThat(r.duplicates()).isEqualTo(1);
        assertThat(e2e.summary().count()).isEqualTo(3);
    }
}
