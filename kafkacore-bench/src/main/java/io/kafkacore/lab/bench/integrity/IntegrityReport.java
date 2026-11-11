package io.kafkacore.lab.bench.integrity;

/**
 * Outcome of reconciling producer acks against consumed records.
 *
 * @param acked      distinct messages the cluster acknowledged
 * @param sendFailed messages whose send completed exceptionally
 * @param received   distinct messages consumed
 * @param lost       acknowledged but never consumed: a durability violation
 * @param duplicates the same message consumed more than once
 * @param outOfOrder a lower sequence consumed after a higher one from the same producer on the same partition
 * @param corrupt    payloads failing magic/CRC validation
 * @param phantom    reported as failed to the producer but present in the log (allowed under at-least-once)
 */
public record IntegrityReport(long acked, long sendFailed, long received, long lost, long duplicates,
                              long outOfOrder, long corrupt, long phantom) {

    public static final IntegrityReport EMPTY = new IntegrityReport(0, 0, 0, 0, 0, 0, 0, 0);

    /** No acknowledged data lost and nothing corrupted: the minimum guarantee under acks=all. */
    public boolean durable() {
        return lost == 0 && corrupt == 0;
    }

    /** Durable and additionally free of duplicates and re-ordering (what idempotent producers promise). */
    public boolean exactlyOnceOrdered() {
        return durable() && duplicates == 0 && outOfOrder == 0;
    }
}
