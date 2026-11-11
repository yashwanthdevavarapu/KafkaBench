package io.kafkacore.lab.bench.integrity;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

class IntegrityTrackerTest {

    @Test
    void cleanRunIsDurableAndExactlyOnce() {
        IntegrityTracker t = new IntegrityTracker();
        for (int seq = 0; seq < 100; seq++) {
            t.recordAck(1, seq);
            t.recordReceive(1, seq, seq % 3);
        }
        IntegrityReport r = t.report();
        assertThat(r.acked()).isEqualTo(100);
        assertThat(r.received()).isEqualTo(100);
        assertThat(r.lost()).isZero();
        assertThat(r.exactlyOnceOrdered()).isTrue();
    }

    @Test
    void ackedButNeverConsumedIsLost() {
        IntegrityTracker t = new IntegrityTracker();
        for (int seq = 0; seq < 10; seq++) t.recordAck(1, seq);
        for (int seq = 0; seq < 7; seq++) t.recordReceive(1, seq, 0);
        IntegrityReport r = t.report();
        assertThat(r.lost()).isEqualTo(3);
        assertThat(r.durable()).isFalse();
        assertThat(t.receivedAckedCount()).isEqualTo(7);
    }

    @Test
    void redeliveryCountsAsDuplicateNotReordering() {
        IntegrityTracker t = new IntegrityTracker();
        for (int seq = 0; seq < 5; seq++) {
            t.recordAck(1, seq);
            t.recordReceive(1, seq, 0);
        }
        // consumer rebalance replays offsets 2..4
        t.recordReceive(1, 2, 0);
        t.recordReceive(1, 3, 0);
        IntegrityReport r = t.report();
        assertThat(r.duplicates()).isEqualTo(2);
        assertThat(r.outOfOrder()).isZero();
        assertThat(r.durable()).isTrue();
        assertThat(r.exactlyOnceOrdered()).isFalse();
    }

    @Test
    void lowerSequenceAfterHigherOnSamePartitionIsOutOfOrder() {
        IntegrityTracker t = new IntegrityTracker();
        t.recordReceive(1, 5, 0);
        t.recordReceive(1, 3, 0);
        t.recordReceive(1, 4, 1); // different partition: independent ordering
        assertThat(t.report().outOfOrder()).isEqualTo(1);
    }

    @Test
    void failedSendThatReachedTheLogIsPhantomNotLoss() {
        IntegrityTracker t = new IntegrityTracker();
        t.recordSendFailure(1, 0);
        t.recordReceive(1, 0, 0);
        IntegrityReport r = t.report();
        assertThat(r.phantom()).isEqualTo(1);
        assertThat(r.lost()).isZero();
        assertThat(r.sendFailed()).isEqualTo(1);
    }

    @Test
    void producersAreTrackedIndependentlyAndCorruptionIsCounted() {
        IntegrityTracker t = new IntegrityTracker();
        t.recordAck(1, 0);
        t.recordAck(2, 0);
        t.recordReceive(1, 0, 0);
        t.recordCorrupt();
        IntegrityReport r = t.report();
        assertThat(r.lost()).isEqualTo(1);
        assertThat(r.corrupt()).isEqualTo(1);
    }

    @Test
    void isSafeUnderConcurrentAcksAndReceives() throws Exception {
        IntegrityTracker t = new IntegrityTracker();
        int perThread = 20_000;
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < 4; p++) {
            int pid = p;
            threads.add(new Thread(() -> {
                await(start);
                for (int s = 0; s < perThread; s++) t.recordAck(pid, s);
            }));
            threads.add(new Thread(() -> {
                await(start);
                for (int s = 0; s < perThread; s++) t.recordReceive(pid, s, s % 6);
            }));
        }
        threads.forEach(Thread::start);
        start.countDown();
        for (Thread th : threads) th.join();
        IntegrityReport r = t.report();
        assertThat(r.acked()).isEqualTo(4L * perThread);
        assertThat(r.received()).isEqualTo(4L * perThread);
        assertThat(r.lost()).isZero();
        assertThat(r.duplicates()).isZero();
    }

    private static void await(CountDownLatch l) {
        try {
            l.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
