package ru.rm.finu.lib;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.format.Json;
import lombok.extern.slf4j.Slf4j;
import ru.rm.finu.config.AppConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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

        Properties props = getProperties(config);
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

    private Properties getProperties(AppConfig.DebeziumConfig config) {
        Properties props = new Properties();

        props.setProperty("name", config.getName());
        props.setProperty("connector.class", config.getConnectorClass());
        props.setProperty("plugin.name", config.getPluginName());
        props.setProperty("database.hostname", config.getDatabase().getHostname());
        props.setProperty("database.port", String.valueOf(config.getDatabase().getPort()));
        props.setProperty("database.user", config.getDatabase().getUser());
        props.setProperty("database.password", config.getDatabase().getPassword());
        props.setProperty("database.dbname", config.getDatabase().getDbname());
        props.setProperty("database.server.name", config.getDatabase().getServerName());
        props.setProperty("slot.name", config.getSlotName());
        props.setProperty("publication.name", config.getPublicationName());
        props.setProperty("key.converter.schemas.enable", String.valueOf(config.isKeyConverterSchemasEnable()));
        props.setProperty("value.converter.schemas.enable", String.valueOf(config.isValueConverterSchemasEnable()));
        props.setProperty("topic.prefix", config.getTopicPrefix());
        props.setProperty("offset.storage", config.getOffsetStorage());
        props.setProperty("offset.storage.file.filename", config.getOffsetStorageFileFilename());
        props.setProperty("offset.flush.interval.ms", String.valueOf(config.getOffsetFlushIntervalMs()));
        props.setProperty("snapshot.mode", "initial");

        List<String> tables = config.getTableIncludeList();
        if (tables != null && !tables.isEmpty()) {
            props.setProperty("table.include.list", String.join(",", tables));
        }

        return props;
    }
}
