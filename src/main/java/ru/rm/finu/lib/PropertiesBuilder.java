package ru.rm.finu.lib;

import ru.rm.finu.config.AppConfig;

import java.util.List;
import java.util.Properties;

public class PropertiesBuilder {
    public static Properties fromDebeziumConfig(AppConfig.DebeziumConfig config) {
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

        List<String> tables = config.getTableIncludeList();
        if (tables != null && !tables.isEmpty()) {
            props.setProperty("table.include.list", String.join(",", tables));
        }

        return props;
    }
}

