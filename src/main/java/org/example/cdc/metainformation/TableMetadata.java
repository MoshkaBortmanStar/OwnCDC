package org.example.cdc.metainformation;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TableMetadata {
    private Connection connection;

    public TableMetadata(Connection connection) {
        this.connection = connection;
    }

    public ResultSet getTableMetadata(String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        return metaData.getColumns(null, null, tableName, null);
    }


    public String getTableName(int tableId) throws SQLException {
        String query = "SELECT relname FROM pg_class WHERE oid = ?";
        PreparedStatement stmt = connection.prepareStatement(query);
        stmt.setInt(1, tableId);
        ResultSet rs = stmt.executeQuery();
        if (rs.next()) {
            return rs.getString("relname");
        } else {
            throw new SQLException("Table with id " + tableId + " not found");
        }
    }

    public void close() throws SQLException {
        connection.close();
    }

}
