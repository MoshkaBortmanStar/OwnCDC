package org.example.cdc.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@RequiredArgsConstructor
public class RelationMetaInfo {

    private final String schemaName;
    private final String tableName;
    private Map<String, Column> columnsMap = new LinkedHashMap<>();

}
