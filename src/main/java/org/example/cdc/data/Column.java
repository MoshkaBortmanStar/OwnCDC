package org.example.cdc.data;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.cdc.data.enums.DataType;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Column {

    private String name;
    private DataType dataType;

}
