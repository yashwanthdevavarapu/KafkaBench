package io.kafkacore.lab.bench.driver;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimiterTest {

    @Test
    void pacesToTargetRate() {
        RateLimiter limiter = new RateLimiter(2_000);
        long start = System.nanoTime();
        for (int i = 0; i < 1_000; i++) limiter.acquire();
        double seconds = (System.nanoTime() - start) / 1e9;
        // 1000 slots at 2000/s = 0.5 s (first slot is immediate). Generous upper bound for CI noise.
        assertThat(seconds).isBetween(0.45, 1.5);
    }

    @Test
    void intendedTimesAreEvenlySpacedEvenWhenCallerStalls() throws InterruptedException {
        RateLimiter limiter = new RateLimiter(1_000); // 1 ms interval
        long first = limiter.acquire();
        Thread.sleep(50); // caller stalls: following slots are already due
        long second = limiter.acquire();
        long third = limiter.acquire();
        // Slots stay on the original schedule: latency measured from them includes the stall.
        assertThat(second - first).isEqualTo(1_000_000);
        assertThat(third - second).isEqualTo(1_000_000);
    }

    @Test
    void zeroRateMeansUnlimited() {
        RateLimiter limiter = new RateLimiter(0);
        assertThat(limiter.unlimited()).isTrue();
        long start = System.nanoTime();
        for (int i = 0; i < 100_000; i++) limiter.acquire();
        assertThat((System.nanoTime() - start) / 1e9).isLessThan(1.0);
    }
}
