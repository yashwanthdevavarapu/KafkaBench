package io.kafkacore.lab.chaos.fault;

/** The failure modes the lab can inject into a single node, and what each one models in production. */
public enum FaultType {
    /** SIGKILL: power loss / OOM-kill / kernel panic. No controlled shutdown, unflushed page cache survives in Docker. */
    KILL("hard crash (SIGKILL)"),
    /** SIGTERM with grace period: Kafka controlled shutdown moves leadership before exit (rolling restarts). */
    STOP("graceful shutdown (SIGTERM)"),
    /** cgroup freeze: a long GC pause or stalled disk; the process is alive but unresponsive. */
    PAUSE("process freeze (docker pause)"),
    /** Detach from the cluster network: the node is up but cannot reach peers or clients. */
    NETWORK_PARTITION("network isolation");

    private final String description;

    FaultType(String description) {
        this.description = description;
    }

    public String description() {
        return description;
    }
}
