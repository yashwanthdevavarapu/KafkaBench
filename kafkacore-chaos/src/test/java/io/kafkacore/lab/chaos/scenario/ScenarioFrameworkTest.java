package io.kafkacore.lab.chaos.scenario;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScenarioFrameworkTest {

    @Test
    void awaitMeasuresTimeUntilConditionHolds() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        OptionalLong ms = Await.until(Duration.ofSeconds(5), Duration.ofMillis(10), () -> calls.incrementAndGet() >= 5);
        assertThat(ms).isPresent();
        assertThat(ms.getAsLong()).isBetween(30L, 2_000L);
        assertThat(calls.get()).isEqualTo(5);
    }

    @Test
    void awaitTreatsExceptionsAsNotYetAndTimesOut() throws Exception {
        OptionalLong ms = Await.until(Duration.ofMillis(150), Duration.ofMillis(20), () -> {
            throw new IllegalStateException("admin timeout while node is down");
        });
        assertThat(ms).isEmpty();
    }

    @Test
    void timelineRecordsOrderedEventsAndEchoesThem() {
        List<String> lines = new ArrayList<>();
        Timeline t = new Timeline(lines::add);
        t.add(Timeline.Kind.FAULT, "kill node 1");
        t.add(Timeline.Kind.HEAL, "start node 1");
        assertThat(t.events()).extracting(Timeline.Event::kind).containsExactly(Timeline.Kind.FAULT, Timeline.Kind.HEAL);
        assertThat(t.events().get(1).atMs()).isGreaterThanOrEqualTo(t.events().get(0).atMs());
        assertThat(lines).hasSize(2).allMatch(l -> l.contains("node 1"));
    }

    @Test
    void registryExposesAllScenariosInOrderWithUniqueIds() {
        ScenarioRegistry r = new ScenarioRegistry();
        assertThat(r.all()).extracting(ChaosScenario::id)
                .containsExactly("leader-kill", "controller-failover", "broker-pause", "network-partition", "rolling-restart");
        assertThat(r.all()).allSatisfy(s -> {
            assertThat(s.title()).isNotBlank();
            assertThat(s.description()).isNotBlank();
            assertThat(s.fault()).isNotNull();
        });
        assertThat(r.resolve(List.of("all"))).hasSize(5);
        assertThat(r.resolve(List.of())).hasSize(5);
        assertThat(r.resolve(List.of("broker-pause", "leader-kill"))).extracting(ChaosScenario::id)
                .containsExactly("broker-pause", "leader-kill");
        assertThatThrownBy(() -> r.get("nope")).isInstanceOf(NoSuchElementException.class).hasMessageContaining("leader-kill");
        assertThatThrownBy(() -> r.register(new LeaderKillScenario())).isInstanceOf(IllegalArgumentException.class);
    }
}
