package org.example.cdc;

import org.example.cdc.metainformation.PgOutput;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class TestLocalCDC {





    public static void main(String[] args) throws SQLException, InterruptedException {
        var connection = PostgresConnectionLocal.INSTANCE.getConnection();
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




        var stream = connection.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName("cdc_local_slot_1")
                .withSlotOption("proto_version", "1")
                .withSlotOption("publication_names", "cdc_migration_data")
                .start();

        while (true) {

            ByteBuffer msg = stream.readPending();
            if (msg == null) {
                TimeUnit.MILLISECONDS.sleep(10L);
                continue;
            }



            PgOutput pgOutput = new PgOutput(msg);
            System.out.println(pgOutput.toString());

            var cobyBuffer = msg.duplicate();
            int offset = cobyBuffer.arrayOffset();
            byte[] source = cobyBuffer.array();
            int length = source.length - offset;
            System.out.println(new String(source, offset, length, StandardCharsets.UTF_8));



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
