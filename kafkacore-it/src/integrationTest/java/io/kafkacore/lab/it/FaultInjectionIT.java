package io.kafkacore.lab.it;

import io.kafkacore.lab.chaos.cluster.ClusterInspector;
import io.kafkacore.lab.chaos.fault.DockerFaultInjector;
import io.kafkacore.lab.chaos.report.ScenarioReport;
import io.kafkacore.lab.chaos.scenario.ChaosScenario;
import io.kafkacore.lab.chaos.scenario.ScenarioContext;
import io.kafkacore.lab.chaos.scenario.ScenarioRegistry;
import io.kafkacore.lab.chaos.scenario.ScenarioResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static io.kafkacore.lab.it.KafkaClusterExtension.DOCKER;
import static io.kafkacore.lab.it.KafkaClusterExtension.TOPOLOGY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression suite for cluster resilience: every built-in chaos scenario must pass against the
 * real cluster. Results are written to build/it-results/ as JSON, Markdown and HTML.
 */
@Tag("integration")
@ExtendWith(KafkaClusterExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FaultInjectionIT {

    private static final ScenarioRegistry REGISTRY = new ScenarioRegistry();
    private static final List<ScenarioResult> RESULTS = new ArrayList<>();
    private static ClusterInspector inspector;
    private static ScenarioContext ctx;

    @BeforeAll
    static void setUp() {
        inspector = new ClusterInspector(TOPOLOGY.bootstrapServers());
        ctx = new ScenarioContext(TOPOLOGY, new DockerFaultInjector(DOCKER, TOPOLOGY.network()), inspector,
                Integer.getInteger("kafkacore.chaos.rate", 500), Duration.ofSeconds(5), Duration.ofSeconds(120),
                System.out::println);
    }

    @AfterAll
    static void tearDown() throws Exception {
        inspector.close();
        if (!RESULTS.isEmpty()) {
            Path dir = Path.of("build", "it-results");
            ScenarioReport.writeJson(dir.resolve("chaos-results.json"), RESULTS);
            Files.writeString(dir.resolve("chaos-summary.md"), ScenarioReport.markdown(RESULTS));
            Files.writeString(dir.resolve("chaos-report.html"), ScenarioReport.html(RESULTS));
        }
    }

    @Test
    @Order(1)
    void partitionLeaderHardCrash() {
        ScenarioResult r = run("leader-kill");
        assertThat(r.metrics()).containsKeys("leaderElectionMs", "isrRejoinMs");
    }

    @Test
    @Order(2)
    void kraftControllerFailover() {
        ScenarioResult r = run("controller-failover");
        assertThat(((Number) r.metrics().get("epochAfter")).longValue())
                .isGreaterThan(((Number) r.metrics().get("epochBefore")).longValue());
        assertThat(r.metrics().get("newControllerId")).isNotEqualTo(r.metrics().get("oldControllerId"));
    }

    @Test
    @Order(3)
    void brokerFreezeIsFencedAndRecovers() {
        ScenarioResult r = run("broker-pause");
        assertThat(r.metrics()).containsKeys("fencedAfterMs", "rejoinAfterThawMs");
    }

    @Test
    @Order(4)
    void isolatedFollowerLeavesAndRejoinsIsr() {
        ScenarioResult r = run("network-partition");
        assertThat(r.metrics()).containsKeys("isrShrinkMs", "isrExpandMs");
    }

    @Test
    @Order(5)
    void rollingRestartLosesNothing() {
        ScenarioResult r = run("rolling-restart");
        assertThat(r.integrity().lost()).isZero();
    }

    private static ScenarioResult run(String id) {
        ChaosScenario scenario = REGISTRY.get(id);
        ScenarioResult r = scenario.run(ctx);
        RESULTS.add(r);
        assertThat(r.error()).as("scenario error").isNull();
        assertThat(r.checks()).as("checks of %s", id).allSatisfy(c ->
                assertThat(c.passed()).as("%s — %s", c.name(), c.detail()).isTrue());
        assertThat(r.integrity().lost()).as("acknowledged messages lost").isZero();
        assertThat(r.integrity().corrupt()).isZero();
        assertThat(r.passed()).isTrue();
        return r;
    }
}
