package org.example.cdc;

import org.example.cdc.config.PostgresConnectionFactoryImpl;
import org.example.cdc.decode.PgoutHendler;
import org.postgresql.PGConnection;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class TestLocalCDC {





    public static void main(String[] args) throws SQLException, InterruptedException {
        var connection = PostgresConnectionFactoryImpl.INSTANCE.getConnection();
        /*connection.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName("cdc_local_slot_j")
                .withOutputPlugin("wal2json")
                .make();*//*

        var nano = System.nanoTime();
        var stream = connection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName("cdc_local_slot_j")
                //.withSlotOption("add-tables", "public.naruto")
                .withSlotOption("format-version", "2")
                .withSlotOption("include-not-null", true)
                .withSlotOption("include-lsn", true)
                .start();*/

     /*   String createPublicationQuery = "CREATE PUBLICATION  cdc_migration_data;";
        try (java.sql.Statement stmt = connection.createStatement()) {
            // Выполнение SQL запроса для создания публикации
            stmt.execute(createPublicationQuery);
            System.out.println("Publication created successfully");
        }*/
        var streamConnection = connection.unwrap(PGConnection.class);
       /* streamConnection.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName("cdc_my_test")
                .withOutputPlugin("pgoutput")
                .make();*/

        var stream = streamConnection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName("cdc_my_test")
                .withSlotOption("proto_version", "1")
                .withSlotOption("publication_names", "cdc_migration_data")
                .start();

       while (true) {

            ByteBuffer msg = stream.readPending();
            if (msg == null) {
                TimeUnit.MILLISECONDS.sleep(10L);
                continue;
            }

           //System.out.println(new String(msg.array(), StandardCharsets.UTF_8));

           PgoutHendler.decodeHandle(msg, rowChangesStructure -> System.out.println(rowChangesStructure.toString()));

            stream.setAppliedLSN(stream.getLastReceiveLSN());
            stream.setFlushedLSN(stream.getLastReceiveLSN());
        }
    }

    /*
    *
    * для pgout
    * connection.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName("cdc_local_slot_1")
                .withOutputPlugin("pgoutput")
                .make();

        var stream = connection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName("cdc_local_slot_1")
                .withSlotOption("proto_version", "1")
                .withSlotOption("publication_names", "cdc_migration_data")
                .start();
    *
    * */





    /*
    *
    *   var cobyBuffer = msg.duplicate();
            int offset = cobyBuffer.arrayOffset();
            byte[] source = cobyBuffer.array();
            int length = source.length - offset;
            System.out.println(new String(source, offset, length, StandardCharsets.UTF_8));
    * */

}
