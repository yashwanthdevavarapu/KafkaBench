package io.kafkacore.lab.bench.process;

import java.time.Duration;
import java.util.List;

/**
 * Runs external commands (docker CLI). An interface so that fault injectors and samplers can be
 * unit-tested with a recording fake instead of a real Docker daemon.
 */
@FunctionalInterface
public interface CommandExecutor {

    CommandResult execute(List<String> command, Duration timeout);

    /** Result of a finished command. */
    record CommandResult(int exitCode, String stdout, String stderr) {
        public boolean ok() {
            return exitCode == 0;
        }

        /** Returns this result or throws with the command's stderr when it failed. */
        public CommandResult orThrow(List<String> command) {
            if (!ok()) {
                throw new CommandFailedException("Command failed (exit " + exitCode + "): " + String.join(" ", command)
                        + (stderr.isBlank() ? "" : " -> " + stderr.strip()));
            }
            return this;
        }
    }

    /** Thrown by {@link CommandResult#orThrow(List)}. */
    final class CommandFailedException extends RuntimeException {
        public CommandFailedException(String message) {
            super(message);
        }
    }
}
