package io.kafkacore.lab.bench.driver;

import io.kafkacore.lab.bench.integrity.IntegrityTracker;
import io.kafkacore.lab.bench.metrics.LatencyRecorder;
import io.kafkacore.lab.bench.payload.MessageCodec;
import io.kafkacore.lab.bench.workload.WorkloadSpec.KeyStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;

import java.time.Duration;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * One producer thread: paces sends with a {@link RateLimiter}, stamps every payload with
 * producerId/sequence/timestamp, and records publish (ack) latency plus integrity bookkeeping.
 */
public final class ProducerWorker implements Runnable {

    private final int id;
    private final String topic;
    private final Supplier<Producer<byte[], byte[]>> producerFactory;
    private final MessageCodec codec;
    private final double ratePerSec;
    private final KeyStrategy keyStrategy;
    private final LatencyRecorder publishLatency;
    private final IntegrityTracker tracker;
    private final LongAdder acked = new LongAdder();
    private final LongAdder ackedBytes = new LongAdder();
    private final LongAdder errors = new LongAdder();
    private final AtomicLong sent = new AtomicLong();
    private final CountDownLatch finished = new CountDownLatch(1);
    private volatile boolean running = true;
    private volatile Throwable lastError;

    public ProducerWorker(int id, String topic, Supplier<Producer<byte[], byte[]>> producerFactory, int messageSize,
                          double ratePerSec, KeyStrategy keyStrategy, LatencyRecorder publishLatency,
                          IntegrityTracker tracker) {
        this.id = id;
        this.topic = topic;
        this.producerFactory = producerFactory;
        this.codec = new MessageCodec(messageSize, 31L * id + 7);
        this.ratePerSec = ratePerSec;
        this.keyStrategy = keyStrategy;
        this.publishLatency = publishLatency;
        this.tracker = tracker;
    }

    /** Convenience factory using a real {@link KafkaProducer}. */
    public static Supplier<Producer<byte[], byte[]>> kafkaProducerFactory(java.util.Map<String, Object> props) {
        return () -> new KafkaProducer<>(props, new org.apache.kafka.common.serialization.ByteArraySerializer(),
                new org.apache.kafka.common.serialization.ByteArraySerializer());
    }

    @Override
    public void run() {
        RateLimiter limiter = new RateLimiter(ratePerSec);
        SplittableRandom rnd = new SplittableRandom(id);
        try (Producer<byte[], byte[]> producer = producerFactory.get()) {
            while (running && !Thread.currentThread().isInterrupted()) {
                long intended = limiter.acquire();
                if (!running) break;
                long seq = sent.getAndIncrement();
                long sendNanos = System.nanoTime();
                byte[] payload = codec.encode(id, seq, sendNanos);
                byte[] key = keyStrategy == KeyStrategy.RANDOM ? longBytes(rnd.nextLong()) : null;
                int size = payload.length;
                // With pacing, latency is charged from the intended slot (coordinated-omission correction).
                long start = limiter.unlimited() ? sendNanos : intended;
                try {
                    producer.send(new ProducerRecord<>(topic, key, payload), (md, ex) -> {
                        if (ex == null) {
                            publishLatency.recordNanos(System.nanoTime() - start);
                            tracker.recordAck(id, seq);
                            acked.increment();
                            ackedBytes.add(size);
                        } else {
                            errors.increment();
                            lastError = ex;
                            tracker.recordSendFailure(id, seq);
                        }
                    });
                } catch (InterruptException e) {
                    tracker.recordSendFailure(id, seq);
                    break;
                } catch (RuntimeException e) {
                    errors.increment();
                    lastError = e;
                    tracker.recordSendFailure(id, seq);
                }
            }
            // Wait for in-flight sends so every message is either acked or failed before reconciliation.
            producer.flush();
        } catch (InterruptException e) {
            Thread.currentThread().interrupt();
        } finally {
            finished.countDown();
        }
    }

    public void stop() {
        running = false;
    }

    /** Waits for the worker to flush and close its producer. */
    public boolean awaitFinished(Duration timeout) throws InterruptedException {
        return finished.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    public int id() { return id; }
    public long sentCount() { return sent.get(); }
    public long ackedCount() { return acked.sum(); }
    public long ackedBytes() { return ackedBytes.sum(); }
    public long errorCount() { return errors.sum(); }
    public Throwable lastError() { return lastError; }

    private static byte[] longBytes(long v) {
        byte[] b = new byte[8];
        for (int i = 7; i >= 0; i--) {
            b[i] = (byte) v;
            v >>>= 8;
        }
        return b;
    }
}
