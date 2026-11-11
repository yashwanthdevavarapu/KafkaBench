package io.kafkacore.lab.bench.integrity;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Reconciles what producers had acknowledged by the cluster against what consumers actually read
 * (see ADR-0006). Sequence numbers are gap-free per producer, so a {@link BitSet} per producer is
 * enough to detect loss, duplicates and re-ordering in O(1) memory per message.
 *
 * <p>Acks for one producer arrive on that producer's single I/O thread; receives for one producer
 * can arrive from several consumer threads, so each per-producer state is guarded by its own lock.
 */
public final class IntegrityTracker {

    private final Map<Integer, ProducerState> producers = new ConcurrentHashMap<>();
    private final LongAdder corrupt = new LongAdder();

    public void recordAck(int producerId, long seq) {
        ProducerState s = state(producerId);
        synchronized (s) {
            s.acked.set(toIndex(seq));
        }
    }

    public void recordSendFailure(int producerId, long seq) {
        ProducerState s = state(producerId);
        synchronized (s) {
            s.failed.set(toIndex(seq));
        }
    }

    public void recordReceive(int producerId, long seq, int partition) {
        ProducerState s = state(producerId);
        int idx = toIndex(seq);
        synchronized (s) {
            if (s.received.get(idx)) {
                // Re-delivery (e.g. consumer re-reading after a rebalance) is a duplicate, not a re-ordering.
                s.duplicates++;
                return;
            }
            s.received.set(idx);
            Long last = s.lastSeqByPartition.get(partition);
            if (last != null && seq < last) {
                s.outOfOrder++;
            }
            if (last == null || seq > last) {
                s.lastSeqByPartition.put(partition, seq);
            }
        }
    }

    public void recordCorrupt() {
        corrupt.increment();
    }

    /** Number of distinct acknowledged messages that have been consumed so far. */
    public long receivedAckedCount() {
        long n = 0;
        for (ProducerState s : producers.values()) {
            synchronized (s) {
                BitSet both = (BitSet) s.acked.clone();
                both.and(s.received);
                n += both.cardinality();
            }
        }
        return n;
    }

    public long ackedCount() {
        long n = 0;
        for (ProducerState s : producers.values()) {
            synchronized (s) {
                n += s.acked.cardinality();
            }
        }
        return n;
    }

    public IntegrityReport report() {
        long acked = 0, failed = 0, received = 0, lost = 0, dup = 0, ooo = 0, phantom = 0;
        for (ProducerState s : producers.values()) {
            synchronized (s) {
                acked += s.acked.cardinality();
                failed += s.failed.cardinality();
                received += s.received.cardinality();
                BitSet missing = (BitSet) s.acked.clone();
                missing.andNot(s.received);
                lost += missing.cardinality();
                // Records reported as failed to the producer that nevertheless reached the log
                // (e.g. a timeout after the leader had already appended). Legal under at-least-once.
                BitSet ph = (BitSet) s.failed.clone();
                ph.and(s.received);
                phantom += ph.cardinality();
                dup += s.duplicates;
                ooo += s.outOfOrder;
            }
        }
        return new IntegrityReport(acked, failed, received, lost, dup, ooo, corrupt.sum(), phantom);
    }

    private ProducerState state(int producerId) {
        return producers.computeIfAbsent(producerId, k -> new ProducerState());
    }

    private static int toIndex(long seq) {
        if (seq < 0 || seq > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("sequence out of tracked range: " + seq);
        }
        return (int) seq;
    }

    private static final class ProducerState {
        final BitSet acked = new BitSet();
        final BitSet failed = new BitSet();
        final BitSet received = new BitSet();
        final Map<Integer, Long> lastSeqByPartition = new HashMap<>();
        long duplicates;
        long outOfOrder;
    }
}
