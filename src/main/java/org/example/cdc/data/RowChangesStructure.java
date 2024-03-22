package org.example.cdc.data;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.cdc.data.enums.OperationEnum;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RowChangesStructure {

    private String tableName;
    private OperationEnum operationEnum;
    private Map<String, String> columnsData = new LinkedHashMap<>();
    private Map<String, Column> columnsType = new LinkedHashMap<>();

}
