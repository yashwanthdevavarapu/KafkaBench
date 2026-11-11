package io.kafkacore.lab.bench.workload;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Loads and validates {@link WorkloadSpec} YAML files. */
public final class WorkloadLoader {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    private WorkloadLoader() {
    }

    /** Parses a workload file, applies defaults and validates it. */
    public static WorkloadSpec load(Path path) {
        try {
            return parse(Files.readString(path), path.toString());
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read workload " + path, e);
        }
    }

    /** Parses YAML text, applies defaults and validates it. {@code source} is used in error messages. */
    public static WorkloadSpec parse(String yaml, String source) {
        WorkloadSpec spec;
        try {
            spec = YAML.readValue(yaml, WorkloadSpec.class);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new WorkloadValidationException(source + ": invalid YAML: " + e.getOriginalMessage(), List.of());
        }
        if (spec == null) {
            throw new WorkloadValidationException(source + ": workload file is empty", List.of());
        }
        List<String> errors = spec.validate();
        if (!errors.isEmpty()) {
            throw new WorkloadValidationException(source + ": invalid workload: " + String.join("; ", errors), errors);
        }
        return spec.withDefaults();
    }

    /** Thrown when a workload cannot be parsed or fails validation. */
    public static final class WorkloadValidationException extends RuntimeException {
        private final transient List<String> errors;

        public WorkloadValidationException(String message, List<String> errors) {
            super(message);
            this.errors = List.copyOf(errors);
        }

        public List<String> errors() {
            return errors;
        }
    }
}
