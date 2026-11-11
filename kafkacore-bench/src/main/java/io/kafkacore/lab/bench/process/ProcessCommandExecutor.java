package io.kafkacore.lab.bench.process;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/** {@link CommandExecutor} backed by {@link ProcessBuilder}. Output streams are drained concurrently. */
public final class ProcessCommandExecutor implements CommandExecutor {

    @Override
    public CommandResult execute(List<String> command, Duration timeout) {
        Process process;
        try {
            process = new ProcessBuilder(command).start();
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot start " + String.join(" ", command), e);
        }
        CompletableFuture<String> out = CompletableFuture.supplyAsync(() -> readAll(process.getInputStream()));
        CompletableFuture<String> err = CompletableFuture.supplyAsync(() -> readAll(process.getErrorStream()));
        try {
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                return new CommandResult(124, "", "timed out after " + timeout);
            }
            return new CommandResult(process.exitValue(), out.get(), err.get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            return new CommandResult(130, "", "interrupted");
        } catch (ExecutionException e) {
            return new CommandResult(process.exitValue(), "", String.valueOf(e.getCause()));
        }
    }

    private static String readAll(InputStream in) {
        try (in; ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            in.transferTo(bos);
            return bos.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "";
        }
    }
}
