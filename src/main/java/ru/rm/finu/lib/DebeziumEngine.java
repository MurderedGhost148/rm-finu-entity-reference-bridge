package ru.rm.finu.lib;

import io.debezium.engine.ChangeEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import ru.rm.finu.config.AppConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class DebeziumEngine {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private io.debezium.engine.DebeziumEngine<ChangeEvent<String, String>> engine;

    public void start() {
        AppConfig config = AppConfig.load();

        createTopicIfNotExists(
                config.getDebezium().getOffsetStorageTopic(),
                config.getKafka().getBootstrapServers()
        );

        executor.submit(() -> {
            long delay = 1000; // старт 1s
            final long maxDelay = 30000; // максимум 30s

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    log.info("Starting Debezium engine...");

                    engine = createEngine();

                    engine.run();

                    log.warn("Debezium engine stopped normally");

                    delay = 1000;
                } catch (Exception e) {
                    log.error("Debezium crashed", e);

                    delay = Math.min(delay * 2, maxDelay);
                } finally {
                    try {
                        if (engine != null) {
                            engine.close();
                        }
                    } catch (Exception e) {
                        log.warn("Error closing engine", e);
                    }
                }

                try {
                    log.warn("Restarting Debezium in {} ms...", delay);
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (engine != null) {
                    engine.close();
                }
            } catch (IOException e) {
                log.warn("Debezium engine failed to close: ", e);
            }

            executor.shutdown();
        }));
    }

    private io.debezium.engine.DebeziumEngine<io.debezium.engine.ChangeEvent<String, String>> createEngine() {
        AppConfig config = AppConfig.load();

        Properties props = getProperties(config);

        return io.debezium.engine.DebeziumEngine.create(io.debezium.engine.format.Json.class)
                .using(props)
                .notifying(RecordProcessor::process)
                .build();
    }

    private void createTopicIfNotExists(String topic, String bootstrapServers) {
        try (AdminClient admin = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {

            if (!admin.listTopics().names().get().contains(topic)) {
                NewTopic newTopic = new NewTopic(topic, 1, (short) 1)
                        .configs(Map.of(
                                "cleanup.policy", "compact",
                                "min.cleanable.dirty.ratio", "0.01",
                                "segment.ms", "5000"
                        ));
                admin.createTopics(List.of(newTopic)).all().get();
                log.info("Created missing Kafka topic: {}", topic);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Kafka topic: " + topic, e);
        }
    }

    private Properties getProperties(AppConfig config) {
        Properties props = new Properties();
        AppConfig.DebeziumConfig debeziumConfig = config.getDebezium();

        props.setProperty("name", debeziumConfig.getName());
        props.setProperty("connector.class", debeziumConfig.getConnectorClass());
        props.setProperty("plugin.name", debeziumConfig.getPluginName());
        props.setProperty("database.hostname", debeziumConfig.getDatabase().getHostname());
        props.setProperty("database.port", String.valueOf(debeziumConfig.getDatabase().getPort()));
        props.setProperty("database.user", debeziumConfig.getDatabase().getUser());
        props.setProperty("database.password", debeziumConfig.getDatabase().getPassword());
        props.setProperty("database.dbname", debeziumConfig.getDatabase().getDbname());
        props.setProperty("database.server.name", debeziumConfig.getDatabase().getServerName());
        props.setProperty("slot.name", debeziumConfig.getSlotName());
        props.setProperty("publication.name", debeziumConfig.getPublicationName());
        props.setProperty("key.converter.schemas.enable", String.valueOf(debeziumConfig.isKeyConverterSchemasEnable()));
        props.setProperty("value.converter.schemas.enable", String.valueOf(debeziumConfig.isValueConverterSchemasEnable()));
        props.setProperty("topic.prefix", debeziumConfig.getTopicPrefix());
        props.setProperty("offset.storage", debeziumConfig.getOffsetStorage());
        props.setProperty("bootstrap.servers", config.getKafka().getBootstrapServers());
        props.setProperty("offset.storage.topic", debeziumConfig.getOffsetStorageTopic());
        props.setProperty("offset.storage.partitions", String.valueOf(debeziumConfig.getOffsetStoragePartitions()));
        props.setProperty("offset.storage.replication.factor", String.valueOf(debeziumConfig.getOffsetStorageReplicationFactor()));
        props.setProperty("snapshot.mode", "initial");

        List<String> tables = debeziumConfig.getTableIncludeList();
        if (tables != null && !tables.isEmpty()) {
            props.setProperty("table.include.list", String.join(",", tables));
        }

        return props;
    }
}
