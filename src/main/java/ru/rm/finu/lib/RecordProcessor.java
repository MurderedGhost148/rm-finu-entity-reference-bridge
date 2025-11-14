package ru.rm.finu.lib;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import ru.rm.finu.model.dto.EntityReferenceDto;
import ru.rm.finu.model.enums.EntityType;
import ru.rm.finu.model.enums.EventType;

import java.util.Set;

@Slf4j
@UtilityClass
public class RecordProcessor {
    private final ObjectMapper mapper = new ObjectMapper();
    private final KafkaPublisher publisher = new KafkaPublisher();

    private final Set<String> REQUIRED_FIELDS = Set.of("id", "name");
    private static final String TOPIC = "t.entity.reference.event";

    public void process(ChangeEvent<String, String> record) {
        String value = record.value();
        if (value == null) { return; }

        try {
            JsonNode root = mapper.readTree(value);

            EntityType type = getEntityType(root);
            Payload payload = getPayload(root);
            if(type == null || payload == null) { return; }

            JsonNode body = payload.body();
            if (!hasRequiredFields(body)) { return; }

            long id = body.get("id").asLong();
            String name = body.get("name").asText();

            log.debug("EntityReference {}: id={}, type={}, name={}", payload.eventType(), id, type, name);

            publisher.send(TOPIC, id, new EntityReferenceDto(id, name, type, payload.eventType()));
        } catch (Exception e) {
            log.error("Failed to process ChangeEvent: {}", value, e);
        }
    }

    private EntityType getEntityType(JsonNode root) {
        JsonNode source = root.get("source");

        if (source == null) {
            return null;
        }

        String table = source.get("table").asText();
        return EntityType.fromTable(table);
    }

    private Payload getPayload(JsonNode root) {
        JsonNode after = root.get("after");
        JsonNode before = root.get("before");
        JsonNode op = root.get("op");

        if(isNull(op)) {
            return null;
        }

        String opValue = op.asText();
        return switch (opValue) {
            case "c", "r" -> {
                if (isNull(after)) yield null;
                yield new Payload(EventType.ADD, after);
            }
            case "u" -> {
                if (isNull(after)) yield null;
                yield new Payload(EventType.UPDATE, after);
            }
            case "d" -> {
                if (isNull(before)) yield null;
                yield new Payload(EventType.DELETE, before);
            }
            default -> null;
        };
    }

    private boolean hasRequiredFields(JsonNode node) {
        for (String field : REQUIRED_FIELDS) {
            if (!node.hasNonNull(field)) {
                return false;
            }
        }

        return true;
    }

    private boolean isNull(JsonNode node) {
        return node == null || node.isNull();
    }

    private record Payload(EventType eventType, JsonNode body) {}
}

