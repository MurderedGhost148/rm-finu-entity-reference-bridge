package ru.rm.finu.model.dto;

import lombok.Data;
import lombok.Value;
import ru.rm.finu.model.enums.EntityType;
import ru.rm.finu.model.enums.EventType;

@Data
@Value
public class EntityReferenceDto {
    Long id;
    String name;
    EntityType type;
    EventType event;
}
