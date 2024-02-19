package org.example.cdc.decode;

import org.example.cdc.data.RelationMetaInfo;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.slf4j.LoggerFactory.getLogger;

public class StringParser {

    private static final Logger logger = getLogger(StringParser.class);

    public static void main(String[] args) {
        String input1 = "R\u0000\u0000Agpublic\u0000naruto\u0000f\u0000\u0005\u0001shinoby\u0000\u0000\u0000\u0004\u0013����\u0001age\u0000\u0000\u0000\u0006�����\u0001sys_id\u0000\u0000\u0000\u0000\u0014����\u0001change_author\u0000\u0000\u0000\u0004\u0013����\u0001update_date_time\u0000\u0000\u0000\u0004Z����";

        // relation marker is 5 bytes and eq to "R\u0000\u0000Ag"
        String newString = input1.substring(5);
        logger.info("RelationMsg {}", newString);
        
        var relationDto = crateRelationMetaInfo(newString);
        logger.info("RelationMetaInfo {}", relationDto);

    }


    private static RelationMetaInfo crateRelationMetaInfo(String relationMsg) {
        String schema = decodeRelationMsg(relationMsg);
        logger.info("Schema {}", schema);
        //one byte between schema and table name in UTF-8 code
        relationMsg = relationMsg.substring(schema.length() + 1);
        String tableName = decodeRelationMsg(relationMsg);
        logger.info("Table name {}", tableName);
        //five byte between table name and columns in UTF-8 code
        relationMsg = relationMsg.substring(tableName.length() + 5);
        List<String> columns = new LinkedList<>();
        boolean isRelationMsgFinish = false;

        while (!isRelationMsgFinish && !relationMsg.isEmpty()) {
            String column = decodeRelationMsg(relationMsg);
            columns.add(column);

            if (relationMsg.length() < column.length() + 10) {
                isRelationMsgFinish = true;
                continue;
            }
            //ten byte between columns in UTF-8 code
            relationMsg = relationMsg.substring(column.length() + 10);
        }
        logger.info("Columns {}", columns);


        return RelationMetaInfo.builder()
                .schemaName(schema)
                .tableName(tableName)
                .columns(columns)
                .build();
    }

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

            System.out.println("Schema: " + schema);
            System.out.println("Table Name: " + tableName);
            System.out.println("Columns: " + columns);
        } else {
            System.out.println("No match found");
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
