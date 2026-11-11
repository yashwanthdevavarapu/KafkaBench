package io.kafkacore.lab.chaos.scenario;

import io.kafkacore.lab.chaos.fault.FaultType;

/** A repeatable fault-injection experiment with an explicit hypothesis and pass/fail checks. */
public interface ChaosScenario {

    /** Stable kebab-case id used on the command line and in reports. */
    String id();

    String title();

    /** The hypothesis being tested, e.g. "a new leader is elected and no acked write is lost". */
    String description();

    FaultType fault();

    ScenarioResult run(ScenarioContext ctx);
}
