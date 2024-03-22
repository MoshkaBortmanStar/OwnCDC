package org.example.cdc.decode;

import org.example.cdc.cache.LocalCache;
import org.example.cdc.data.Column;
import org.example.cdc.data.RelationMetaInfo;
import org.example.cdc.data.RowChangesStructure;
import org.example.cdc.data.enums.DataType;
import org.example.cdc.data.enums.OperationEnum;
import org.example.cdc.exception.RelationMetaInfoNotFoundException;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.slf4j.LoggerFactory.getLogger;

public class StringParser {

    private static final Logger logger = getLogger(StringParser.class);


    public static RelationMetaInfo crateRelationMetaInfo(String relationMsg) {
        ByteBuffer buffer = ByteBuffer.wrap(relationMsg.getBytes(StandardCharsets.UTF_8));
        Map<String, Column> columnsMap = new LinkedHashMap<>();
        String copyString = relationMsg;
        byte[] bytes = copyString.getBytes(StandardCharsets.UTF_8);
        relationMsg = relationMsg.substring(5); // delete 5 byte
        String schema = decodeRelationMsg(relationMsg);
        logger.info("Schema {}", schema);

        //one byte between schema and table name in UTF-8 code
        relationMsg = relationMsg.substring(schema.length() + 1);
        String tableName = decodeRelationMsg(relationMsg);
        logger.info("Table name {}", tableName);

        //five byte between table name and columns in UTF-8 code
        relationMsg = relationMsg.substring(tableName.length() + 5);
        boolean isRelationMsgFinish = false;

        while (!isRelationMsgFinish && !relationMsg.isEmpty()) {
            //find column name
            String columnName = decodeRelationMsg(relationMsg);
            //find buffer position of column name
            var position = copyString.indexOf(columnName) + columnName.length() + 1;
            int bytePosition = findBytePosition(bytes, position);
            //set buffer position to column name for reading type
            buffer.position(bytePosition);
            int typeId = buffer.getInt();
            columnsMap.put(columnName, Column.builder().name(columnName).dataType(DataType.fromOid(typeId)).build());

            if (relationMsg.length() < columnName.length() + 10) {
                isRelationMsgFinish = true;
                continue;
            }
            //ten byte between columns in UTF-8 code
            relationMsg = relationMsg.substring(columnName.length() + 10);
        }
        logger.info("Columns {}", columnsMap);


        return RelationMetaInfo.builder()
                .schemaName(schema)
                .tableName(tableName)
                .columnsMap(columnsMap)
                .build();
    }

    private static int findBytePosition(byte[] bytes, int charPosition) {
        return new String(bytes, StandardCharsets.UTF_8)
                .substring(0, charPosition)
                .getBytes(StandardCharsets.UTF_8).length;
    }



    /**
     * Create RowChangesStructure from byte message
    * */
    public static RowChangesStructure createRowChangesStructure(ByteBuffer byteMsg, OperationEnum operation) {
        int relationId;
        if (operation == OperationEnum.TRUNCATE) {
            relationId = byteMsg.position(operation.getAdditionallyBytes()).getInt();
            var relationMetaInfo = getRelationMetaInfo(relationId);

            return createRowChangesStructure(relationMetaInfo,
                    createEmptyValuesList(relationMetaInfo.getColumnsMap().size()),
                    operation);
        }

        //get id for relation
        relationId =  byteMsg.getInt();

        //switch to next byte
        byteMsg.get();
        short numberOfColumns = byteMsg.getShort();
        RelationMetaInfo relationMetaInfo = getRelationMetaInfo(relationId);

        // Search marker of operation
        int positionOfN = findPositionOfCharacter(byteMsg, operation);
        if (positionOfN == -1) {
            var strValue = new String(byteMsg.array(), StandardCharsets.UTF_8);
            logger.info("String cannot be decoded correctly {}", strValue);
            throw new StringIndexOutOfBoundsException("String cannot be decoded correctly " + strValue);
        }
        // Set startPosition to the first byte from the marker
        byteMsg.position(positionOfN);
        //get column and value information
        List<String> listOfColumnsName = relationMetaInfo.getColumnsMap().keySet().stream().toList();
        List<String> listOfValues = createValuesList(listOfColumnsName, byteMsg);

        return createRowChangesStructure(relationMetaInfo, listOfValues, operation);
    }

    private static List<String> createValuesList(List<String> listOfColumnsName, ByteBuffer byteMsg) {
        List<String> listOfValues = new LinkedList<>();
        for (String column : listOfColumnsName) {
            char type = (char) byteMsg.get();
            if (type == 't') { // textual data
                final String valueStr = convertToStringValue(byteMsg);
                listOfValues.add(valueStr);
            } else if (type == 'n') { // null data
                listOfValues.add("null");
            } else {
                logger.trace("Unsupported type '{}' for column: '{}'", type, column);
            }
        }
        return listOfValues;
    }


    private static RelationMetaInfo getRelationMetaInfo(int relationId) {
        RelationMetaInfo relationMetaInfo = LocalCache.getRelationMetaInfo(relationId);
        if (relationMetaInfo == null) {
            logger.error("RelationMetaInfo not found for relationKey {}", relationId);
            throw new RelationMetaInfoNotFoundException("RelationMetaInfo not found for relationKey " + relationId);
        }

        return relationMetaInfo;
    }


    private static List<String> createEmptyValuesList(int size) {
        List<String> values = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            values.add("null");
        }
        return values;
    }



    private static int findPositionOfCharacter(ByteBuffer buffer, OperationEnum operationEnum) {
        //switch on startPosition
        buffer.position(0);
        //if operation is update, then search for insert operation because in update operation we need to find new values, it's constant is 'N'
        var valueOperation = operationEnum == OperationEnum.DELETE ? OperationEnum.OLD_VALUE_REPLACED : operationEnum.NEW_VALUE_REPLACED;

        //find position of operation
        while (buffer.hasRemaining()) {
            if ((char) buffer.get() == valueOperation.getConstant()) {
                // Возвращение позиции символа 'N'
                return buffer.position() + valueOperation.getAdditionallyBytes();
            }
        }

        //operation not found
        return -1;
    }


    private static String convertToStringValue(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] value = new byte[length];
        buffer.get(value, 0, length);
        return new String(value, StandardCharsets.UTF_8);
    }


    //TODO delete

    /*    public static RowChangesStructure createRowchangesStructure(String updateMsg, int relationKey, OperationEnum operation) {
            logger.info("UpdateMsg {}", updateMsg);
            var DELIMETR_1 = new String(new char[]{116, 0, 0, 0});
            int startIndex = updateMsg.indexOf(operation.getConstant()) + operation.getConstant() + operation.getAdditionallyBytes();
            updateMsg = updateMsg.substring(startIndex);
            var list = findSubstringsBeforeSequences(updateMsg, DELIMETR_1);
            return null;
    //        return createRowChangesStructure(list, relationKey);
        }
    */
    private static RowChangesStructure createRowChangesStructure(RelationMetaInfo relationMetaInfo, List<String> values, OperationEnum operationEnum) {
        List<String> columns = relationMetaInfo.getColumnsMap().keySet().stream().toList();
        Map<String, String> columnsData = new LinkedHashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            columnsData.put(columns.get(i), values.get(i));
        }

        return RowChangesStructure.builder()
                .tableName(relationMetaInfo.getTableName())
                .operationEnum(operationEnum)
                .columnsData(columnsData)
                .columnsType(relationMetaInfo.getColumnsMap())
                .build();
    }

    private static boolean checkColumnAndValueSize(int columnSize, int valueSize) {
        return columnSize == valueSize;
    }


    private static List<String> findSubstringsBeforeSequences(String original, String sequence1) {
        List<String> substrings = new ArrayList<>();
        int start = 0;
        while (start < original.length()) {
            int index1 = original.indexOf(sequence1, start);
            int nextIndex = -1;

            // Находим ближайшее вхождение любой из последовательностей
            if (index1 != -1) {
                nextIndex = index1;
            }

            // Если вхождение найдено, добавляем подстроку до него в список
            if (nextIndex != -1) {
                substrings.add(original.substring(start, nextIndex));
                start = nextIndex + sequence1.length() + 1; // next byte after sequence in UTF-8 code
            } else {
                substrings.add(original.substring(start));
                break;
            }
        }
        return substrings;
    }


    /**
     * Decode relation message
     * search for the first occurrence of the '\u0000' character and return the string up to that character
     *
     * @param msg string to decode
     * @return decoded string
     */
    private static String decodeRelationMsg(String msg) {
        var sb = new StringBuilder();
        for (int i = 0; i < msg.length(); i++) {
            char c = msg.charAt(i);
            if (c != '\u0000') {
                sb.append(c);
            } else {
                return sb.toString();
            }
        }
        return sb.toString();
    }


    private static void parseString(String input) {
        // Регулярное выражение для извлечения схемы и имени таблицы
        Pattern pattern = Pattern.compile("R\\s+Ag(\\w+)\\s+(\\w+)");
        Matcher matcher = pattern.matcher(input);

        if (matcher.find()) {
            String schema = matcher.group(1);
            String tableName = matcher.group(2);

            // Извлечение колонок
            List<String> columns = extractColumns(input.substring(matcher.end()));

            logger.info("Schema: " + schema);
            logger.info("Table Name: " + tableName);
            logger.info("Columns: " + columns);
        } else {
            logger.info("No match found");
        }
    }

    private static List<String> extractColumns(String columnsPart) {
        List<String> columns = new ArrayList<>();
        Pattern columnPattern = Pattern.compile("\\w+");
        Matcher columnMatcher = columnPattern.matcher(columnsPart);

        while (columnMatcher.find()) {
            columns.add(columnMatcher.group());
        }

        return columns;
    }


}
