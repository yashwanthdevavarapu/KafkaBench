package io.kafkacore.lab.chaos.scenario;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** Ordered, timestamped log of what happened during a scenario (relative to its start). */
public final class Timeline {

    /** Event categories; FAULT and HEAL are drawn as markers on the throughput chart. */
    public enum Kind { INFO, FAULT, OBSERVE, HEAL, RECOVERED, CHECK, ERROR }

    public record Event(long atMs, Kind kind, String message) {
    }

    private final long startNanos;
    private final List<Event> events = new ArrayList<>();
    private final Consumer<String> sink;

    public Timeline(Consumer<String> sink) {
        this(System.nanoTime(), sink);
    }

    Timeline(long startNanos, Consumer<String> sink) {
        this.startNanos = startNanos;
        this.sink = sink;
    }

    public long elapsedMs() {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    public synchronized Event add(Kind kind, String message) {
        Event e = new Event(elapsedMs(), kind, message);
        events.add(e);
        sink.accept(String.format("  %7.1fs  %-9s %s", e.atMs() / 1000.0, kind, message));
        return e;
    }

    public synchronized List<Event> events() {
        return List.copyOf(events);
    }
}
