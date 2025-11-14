package ru.rm.finu.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

@Data
public class AppConfig {
    private DebeziumConfig debezium;
    private KafkaConfig kafka;

    private static AppConfig instance = null;

    @Data
    @NoArgsConstructor
    public static class DebeziumConfig {
        private String name;
        @JsonProperty("connector.class")
        private String connectorClass;
        @JsonProperty("plugin.name")
        private String pluginName;
        private DatabaseConfig database;
        @JsonProperty("slot.name")
        private String slotName;
        @JsonProperty("publication.name")
        private String publicationName;
        @JsonProperty("table.include.list")
        private List<String> tableIncludeList;
        @JsonProperty("key.converter.schemas.enable")
        private boolean keyConverterSchemasEnable;
        @JsonProperty("value.converter.schemas.enable")
        private boolean valueConverterSchemasEnable;
        @JsonProperty("topic.prefix")
        private String topicPrefix;
        @JsonProperty("offset.storage")
        private String offsetStorage;
        @JsonProperty("offset.storage.file.filename")
        private String offsetStorageFileFilename;
        @JsonProperty("offset.flush.interval.ms")
        private int offsetFlushIntervalMs;
    }

    @Data
    @NoArgsConstructor
    public static class DatabaseConfig {
        private String hostname;
        private String port;
        private String user;
        private String password;
        private String dbname;
        @JsonProperty("server.name")
        private String serverName;
    }

    @Data
    @NoArgsConstructor
    public static class KafkaConfig {
        @JsonProperty("bootstrap-servers")
        private String bootstrapServers;
    }

    public static AppConfig load() {
        if(instance != null) {
            return instance;
        }

        try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream("application.yml")) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

            AppConfig config = mapper.readValue(in, AppConfig.class);
            config.applyEnvOverrides();

            instance = config;

            return config;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private void applyEnvOverrides() {
        if (debezium != null && debezium.database != null) {
            debezium.database.setHostname(resolveEnv(debezium.database.getHostname()));
            debezium.database.setPort(resolveEnv(debezium.database.getPort()));
            debezium.database.setDbname(resolveEnv(debezium.database.getDbname()));
            debezium.database.setUser(resolveEnv(debezium.database.getUser()));
            debezium.database.setPassword(resolveEnv(debezium.database.getPassword()));
        }
    }

    private String resolveEnv(String value) {
        if (value == null) return null;
        if (!value.startsWith("${")) return value;
        String inner = value.substring(2, value.length() - 1);
        String[] parts = inner.split(":", 2);
        String env = parts[0];
        String def = parts.length > 1 ? parts[1] : "";
        return System.getenv().getOrDefault(env, def);
    }
}
