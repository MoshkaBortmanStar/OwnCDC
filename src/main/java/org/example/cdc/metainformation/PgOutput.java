package org.example.cdc.metainformation;

import org.example.cdc.PostgresConnectionLocal;
import org.example.cdc.decode.StringParser;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

import static org.example.cdc.decode.DecodePgoutConstant.NEW_VALUE_REPLACED;
import static org.example.cdc.decode.DecodePgoutConstant.UPDATE_NEW_VALUE_SEQUENCE;
import static org.slf4j.LoggerFactory.getLogger;

public class PgOutput {

    private static final Logger logger = getLogger(StringParser.class);

    ByteBuffer buffer;

    public PgOutput(ByteBuffer b) {
        buffer = b;
    }

    public String toString() {
        if (buffer.remaining() < 1) {
            return "";
        }

       /*  final byte[] source = buffer.array();
         final byte[] content = Arrays.copyOfRange(source, buffer.arrayOffset(), source.length + 2);
         var contentString = HexConverter.convertToHexString(content);
         System.out.println(contentString);
*/
        var cmd = (char) buffer.get();
        var cobyBuffer = buffer.duplicate();
        switch (cmd) {
            case 'C', 'B':
                return cmd == 'C' ? "COMMIT: " : "BEGIN: ";
            case 'R':
                int relationId = buffer.getInt();
                System.out.println(relationId);
                int offset = buffer.arrayOffset();
                byte[] source = buffer.array();
                int length = source.length - offset;
                var strR  = (new String(source, offset, length, StandardCharsets.UTF_8));

                var relationDto = StringParser.crateRelationMetaInfo(strR);
                logger.info("RelationDto {}", relationDto);
                return strR;
            case 'U':
                var relationIdU = cobyBuffer.getInt();
                System.out.println(relationIdU);
                StringBuffer sb = new StringBuffer("UPDATE: ");
                int oid = buffer.getInt();
                /*
                 this can be O or K if Delete or possibly N if UPDATE
                 K means key
                 O means old data
                 N means new data
                 */
                char keyOrTuple = (char) buffer.get();

                getTuple(beforeUpdate(buffer), sb);

                int len = sb.length();
                if (len > 1 && sb.charAt(len - 2) == ',' && sb.charAt(len - 1) == ' ') {
                    sb.setLength(len - 2);
                }

                return sb.toString();
            case 'D':

                System.out.println(cobyBuffer.getInt());
                StringBuffer sbB = new StringBuffer("DELETE: ");
                buffer.getInt();
                /*
                 this can be O or K if Delete or possibly N if UPDATE
                 K means key
                 O means old data
                 N means new data
                 */
                buffer.get();
                getTuple(buffer, sbB);


                return sbB.toString();

            case 'I':
                var relationIdI = cobyBuffer.getInt();
                System.out.println(relationIdI);

                sb = new StringBuffer("INSERT: ");
                // oid of relation that is being inserted
                oid = buffer.getInt();
                // should be an N
                char isNew = (char) buffer.get();
                getTuple(buffer, sb);
                return sb.toString();
            case 'T':
                sb = new StringBuffer("TRUNCATE: ");
                String name = null;
                try {
                    name = decodeTruncate(buffer);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                sb.append(name);

                return sb.toString();
        }
        return "";
    }

    private ByteBuffer beforeUpdate(ByteBuffer buffer) {
        var cobyBuffer = buffer.duplicate();
        int offset = cobyBuffer.arrayOffset();
        byte[] source = cobyBuffer.array();
        int length = source.length - offset;

        //get string from buffer
        var msg = new String(source, offset, length, StandardCharsets.UTF_8);

        //get index of sequence
        var replaceMsg = msg.replace(UPDATE_NEW_VALUE_SEQUENCE.getValue(), NEW_VALUE_REPLACED.getValue());

        //get string after sequence
        var resultNewValueMsg = replaceMsg
                .substring(replaceMsg.lastIndexOf(NEW_VALUE_REPLACED.getValue()) + NEW_VALUE_REPLACED.getValue().length() - 2);

        //get byte array from string
        return ByteBuffer.wrap(resultNewValueMsg.getBytes());
    }


    private void getTuple(ByteBuffer buffer, StringBuffer sb) {

        if (buffer.remaining() < 2) {
            return;
        }
        short numAttrs = buffer.getShort();
        for (int i = 0; i < numAttrs; i++) {
            if (buffer.remaining() < 1) {
                return;
            }
            byte c = buffer.get();
            switch (c) {
                case 'n': // null
                    sb.append("NULL, ");
                    break;
                case 'u': // unchanged toast column
                    break;
                case 't': // textual data
                    if (buffer.remaining() < 4) {
                        return;
                    }
                    int strLen = buffer.getInt();
                    if (buffer.remaining() < strLen) {
                        return;
                    }
                    byte[] bytes = new byte[strLen];
                    buffer.get(bytes, 0, strLen);
                    String value = new String(bytes);
                    sb.append(value).append(", ");
                    break;
                default:
                    sb.append((char) c);
            }
        }


    }

    private String getString(ByteBuffer buffer) {
        StringBuffer sb = new StringBuffer();
        while (true) {
            byte c = buffer.get();
            if (c == 0) {
                break;
            }
            sb.append((char) c);
        }
        return sb.toString();
    }

    private static String readString(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        byte b = 0;
        while ((b = buffer.get()) != 0) {
            sb.append((char) b);
        }
        return sb.toString();
    }


    private String decodeTruncate(ByteBuffer buffer) throws SQLException, InterruptedException {
        int numberOfRelations = buffer.getInt();
        //next byte is flags foe search tavleId
        buffer.get();
        int[] relationIds = new int[numberOfRelations];
        for (int i = 0; i < numberOfRelations; ++i) {
            relationIds[i] = buffer.getInt();
        }
        int[] var9 = relationIds;
        int i = relationIds.length;

        var tableMetadata = new TableMetadata((Connection) PostgresConnectionLocal.INSTANCE.getConnection());
        String tableName = null;
        for (int var11 = 0; var11 < i; ++var11) {
            int relationId = var9[var11];
            tableName = tableMetadata.getTableName(relationId);
        }

        return tableName;
    }


}
