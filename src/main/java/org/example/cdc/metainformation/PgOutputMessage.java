package org.example.cdc.metainformation;

public class PgOutputMessage {
    protected final int tableId;

    public PgOutputMessage(int tableId) {
        this.tableId = tableId;
    }

    public int getTableId() {
        return tableId;
    }
}
