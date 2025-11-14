package ru.rm.finu.lib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import ru.rm.finu.model.enums.EntityType;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@UtilityClass
public class RecordProcessor {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, EntityType> tableToType = new HashMap<>();

    static {
        tableToType.put("accounts", EntityType.ACCOUNT);
        tableToType.put("categories", EntityType.CATEGORY);
        tableToType.put("contractors", EntityType.CONTRACTOR);
        tableToType.put("companies", EntityType.COMPANY);
        tableToType.put("groups", EntityType.GROUP);
        tableToType.put("group_values", EntityType.GROUP_VALUE);
    }

    public void process(ChangeEvent<String, String> record) {
        String value = record.value();
        if (value == null) {
            return;
        }

        try {
            JsonNode root = mapper.readTree(value);
            JsonNode after = root.get("after");
            JsonNode before = root.get("before");
            JsonNode source = root.get("source");

            if (source == null) return;

            String table = source.get("table").asText();
            EntityType type = tableToType.get(table);
            if (type == null) {
                return;
            }

            EventType eventType;
            JsonNode payload;
            if (after != null && before == null) {
                eventType = EventType.ADD;
                payload = after;
            } else if (after != null) {
                eventType = EventType.UPDATE;
                payload = after;
            } else if (before != null) {
                eventType = EventType.DELETE;
                payload = before;
            } else {
                return; // пустое событие
            }

            if (!payload.hasNonNull("id") || !payload.hasNonNull("name")) return;

            long id = payload.get("id").asLong();
            String name = payload.get("name").asText();

            log.info("EntityReference {}: id={}, type={}, name={}", eventType, id, type, name);
        } catch (Exception e) {
            log.error("Failed to process ChangeEvent: {}", value, e);
        }
    }

    private enum EventType {
        ADD, UPDATE, DELETE;
    }
}

