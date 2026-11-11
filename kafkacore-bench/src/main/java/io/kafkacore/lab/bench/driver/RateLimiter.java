package io.kafkacore.lab.bench.driver;

import java.util.concurrent.locks.LockSupport;

/**
 * Open-loop pacer: issues send "slots" on a fixed schedule independent of how long sends take.
 *
 * <p>{@link #acquire()} returns the <em>intended</em> start time of the slot. Measuring latency from
 * the intended time (rather than the moment the call actually happened) corrects for coordinated
 * omission: if the client stalls, the queued-up slots are charged the full stall (see ADR-0004).
 */
public final class RateLimiter {

    private final long intervalNanos;
    private long nextSlot;

    /** @param ratePerSec slots per second; {@code <= 0} disables pacing */
    public RateLimiter(double ratePerSec) {
        this.intervalNanos = ratePerSec <= 0 ? 0 : Math.max(1, Math.round(1_000_000_000.0 / ratePerSec));
        this.nextSlot = System.nanoTime();
    }

    public boolean unlimited() {
        return intervalNanos == 0;
    }

    /** Blocks until the next slot and returns its intended {@link System#nanoTime()}. */
    public long acquire() {
        if (intervalNanos == 0) {
            return System.nanoTime();
        }
        long slot = nextSlot;
        nextSlot += intervalNanos;
        long wait;
        while ((wait = slot - System.nanoTime()) > 0) {
            if (wait > 100_000) {
                LockSupport.parkNanos(wait - 50_000);
            } else {
                Thread.onSpinWait();
            }
            if (Thread.currentThread().isInterrupted()) {
                break;
            }
        }
        return slot;
    }
}
