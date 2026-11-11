package io.kafkacore.lab.bench.result;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * JSON persistence for results. A results file always holds a JSON array so that a sweep (many
 * runs) and a single run share one format, and baselines can be diffed run-by-run.
 */
public final class ResultIO {

    public static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(SerializationFeature.INDENT_OUTPUT)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private ResultIO() {
    }

    public static void write(Path file, List<BenchmarkResult> results) {
        try {
            if (file.getParent() != null) Files.createDirectories(file.getParent());
            JSON.writeValue(file.toFile(), results);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write " + file, e);
        }
    }

    public static List<BenchmarkResult> read(Path file) {
        try {
            return JSON.readValue(file.toFile(), new TypeReference<List<BenchmarkResult>>() { });
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read results " + file, e);
        }
    }
}
