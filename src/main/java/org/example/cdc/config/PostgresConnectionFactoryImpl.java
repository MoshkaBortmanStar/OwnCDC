package org.example.cdc.config;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public enum PostgresConnectionFactoryImpl {
    INSTANCE;

    private static Connection getInstance() throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/data";
        var props = getProperties();

        return DriverManager.getConnection(url, props);
    }



    private  static Properties  getProperties(){
        // create a new Properties object
        Properties props = new Properties();
        // populate it with the properties from the file
        String fileName = "src/main/java/resource/db.properties";

        try (var stream = new FileInputStream(fileName)) {
            props.load(stream);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return props;
    }


    public Connection getConnection() {
        try {
            return this.getInstance();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public PGConnection getPGConnection() {
        try {
            return this.getInstance().unwrap(PGConnection.class);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
