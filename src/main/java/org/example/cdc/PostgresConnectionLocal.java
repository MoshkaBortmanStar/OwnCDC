package org.example.cdc;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public enum PostgresConnectionLocal {
    INSTANCE;

    private static PGConnection getInstance() throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/data";
        Properties props = new Properties();
        PGProperty.USER.set(props, "postgres");
        PGProperty.PASSWORD.set(props, "admin");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "10");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        var con = DriverManager.getConnection(url, props);
        PGConnection newConnection = con.unwrap(PGConnection.class);

        return newConnection;
    }

    public PGConnection getConnection() {
        try {
            return this.getInstance();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
