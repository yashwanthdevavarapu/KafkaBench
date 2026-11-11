package io.kafkacore.lab.chaos.scenario;

import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.Callable;

/** Polling helper used to measure how long the cluster takes to reach a condition. */
public final class Await {

    private Await() {
    }

    /**
     * Polls {@code condition} until it returns true or the timeout expires. Exceptions thrown by the
     * condition (e.g. Admin timeouts while a node is down) count as "not yet".
     *
     * @return elapsed milliseconds until the condition held, or empty on timeout
     */
    public static OptionalLong until(Duration timeout, Duration poll, Callable<Boolean> condition)
            throws InterruptedException {
        long start = System.nanoTime();
        long deadline = start + timeout.toNanos();
        while (true) {
            boolean ok;
            try {
                ok = Boolean.TRUE.equals(condition.call());
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                ok = false;
            }
            long now = System.nanoTime();
            if (ok) return OptionalLong.of((now - start) / 1_000_000);
            if (now >= deadline) return OptionalLong.empty();
            Thread.sleep(poll.toMillis());
        }
    }
}
