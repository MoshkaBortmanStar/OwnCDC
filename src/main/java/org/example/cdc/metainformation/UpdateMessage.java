package org.example.cdc.metainformation;

import java.util.List;

public class UpdateMessage extends PgOutputMessage{
    private final int rowId;
    private final List<Column> columns;

    public UpdateMessage(int tableId, int rowId, List<Column> columns) {
        super(tableId);
        this.rowId = rowId;
        this.columns = columns;
    }

    public int getTableId() {
        return tableId;
    }

    public int getRowId() {
        return rowId;
    }

    public List<Column> getColumns() {
        return columns;
    }
}
