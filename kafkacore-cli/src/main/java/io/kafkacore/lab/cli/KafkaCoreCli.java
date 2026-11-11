package io.kafkacore.lab.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Entry point: {@code kafkacore <command>}.
 *
 * <p>Exit codes: 0 success · 1 usage/runtime error · 2 performance regression detected ·
 * 3 a fault-injection scenario failed.
 */
@Command(name = "kafkacore",
        mixinStandardHelpOptions = true,
        version = "KafkaCore Lab 1.0.0",
        description = "Benchmarking, regression testing and fault injection for Apache Kafka (KRaft).",
        subcommands = {BenchCommand.class, ReportCommand.class, ChaosCommand.class, ClusterCommand.class,
                CommandLine.HelpCommand.class})
public final class KafkaCoreCli implements Runnable {

    public static final int EXIT_REGRESSION = 2;
    public static final int EXIT_CHAOS_FAILED = 3;

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @Override
    public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    public static CommandLine commandLine() {
        return new CommandLine(new KafkaCoreCli())
                .setExecutionExceptionHandler((ex, cl, parsed) -> {
                    StringBuilder msg = new StringBuilder("error: ").append(ex.getMessage() == null ? ex.toString() : ex.getMessage());
                    for (Throwable c = ex.getCause(); c != null && c != c.getCause(); c = c.getCause()) {
                        msg.append(System.lineSeparator()).append("  caused by: ").append(c);
                    }
                    cl.getErr().println(msg);
                    return 1;
                });
    }

    public static void main(String[] args) {
        System.exit(commandLine().execute(args));
    }
}
