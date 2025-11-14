package ru.rm.finu.model.enums;

import lombok.RequiredArgsConstructor;

import java.util.Arrays;

@RequiredArgsConstructor
public enum EntityType {
    ACCOUNT("accounts"),
    CATEGORY("categories"),
    CONTRACTOR("contractors"),
    COMPANY("companies"),
    GROUP("groups"),
    GROUP_VALUE("group_values");

    private final String table;

    public static EntityType fromTable(String table) {
        return Arrays.stream(values())
                .filter(e -> e.table.equals(table))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown table: " + table));
    }
}
