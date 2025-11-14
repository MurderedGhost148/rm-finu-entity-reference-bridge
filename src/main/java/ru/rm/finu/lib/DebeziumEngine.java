package ru.rm.finu.lib;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.format.Json;
import lombok.extern.slf4j.Slf4j;
import ru.rm.finu.config.AppConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class DebeziumEngine {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private io.debezium.engine.DebeziumEngine<ChangeEvent<String, String>> engine;

    public void start() {
        AppConfig.DebeziumConfig config = AppConfig.load().getDebezium();

        ensureOffsetDirectoryExists(config.getOffsetStorageFileFilename());

        Properties props = PropertiesBuilder.fromDebeziumConfig(config);
        engine = io.debezium.engine.DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(RecordProcessor::process)
                .build();

        executor.submit(() -> {
            try {
                engine.run();
            } catch (Exception e) {
                log.error("Debezium engine failed", e);

                System.exit(1);
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                engine.close();
            } catch (IOException e) {
                log.warn("Debezium engine failed to close: ", e);
            }

            executor.shutdown();
        }));
    }

    private void ensureOffsetDirectoryExists(String offsetFile) {
        Path path = Paths.get(offsetFile).toAbsolutePath();
        Path dir = path.getParent();

        if (dir != null) {
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create directory: " + dir, e);
            }
        }
    }
}
