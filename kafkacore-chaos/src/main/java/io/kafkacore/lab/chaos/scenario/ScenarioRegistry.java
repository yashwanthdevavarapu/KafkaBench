package io.kafkacore.lab.chaos.scenario;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/** Catalogue of built-in scenarios, in the order {@code chaos run all} executes them. */
public final class ScenarioRegistry {

    private final Map<String, ChaosScenario> scenarios = new LinkedHashMap<>();

    public ScenarioRegistry() {
        register(new LeaderKillScenario());
        register(new ControllerFailoverScenario());
        register(new BrokerPauseScenario());
        register(new NetworkPartitionScenario());
        register(new RollingRestartScenario());
    }

    public void register(ChaosScenario s) {
        if (scenarios.putIfAbsent(s.id(), s) != null) {
            throw new IllegalArgumentException("duplicate scenario id " + s.id());
        }
    }

    public List<ChaosScenario> all() {
        return List.copyOf(scenarios.values());
    }

    public ChaosScenario get(String id) {
        ChaosScenario s = scenarios.get(id);
        if (s == null) {
            throw new NoSuchElementException("unknown scenario '" + id + "'; available: " + scenarios.keySet());
        }
        return s;
    }

    /** Resolves ids (or "all") to scenarios, preserving the requested order. */
    public List<ChaosScenario> resolve(List<String> ids) {
        if (ids.isEmpty() || ids.contains("all")) return all();
        List<ChaosScenario> out = new ArrayList<>();
        for (String id : ids) out.add(get(id));
        return out;
    }
}
