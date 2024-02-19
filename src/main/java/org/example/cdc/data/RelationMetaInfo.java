package org.example.cdc.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.util.LinkedList;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@RequiredArgsConstructor
public class RelationMetaInfo {

    private final String schemaName;
    private final String tableName;
    private List<String> columns = new LinkedList<>();

}
