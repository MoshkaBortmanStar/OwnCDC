package org.example.cdc.decode;

import org.example.cdc.cache.LocalCache;
import org.example.cdc.data.RowChangesStructure;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import static org.example.cdc.data.enums.OperationEnum.getOperationEnum;
import static org.example.cdc.decode.StringParser.createRowChangesStructure;
import static org.slf4j.LoggerFactory.getLogger;

public class PgoutHendler {

    private static final Logger logger = getLogger(PgoutHendler.class);

    public static void decodeHandle(ByteBuffer buffer, Consumer<RowChangesStructure> changesStructureConsumer) {
        if (buffer.remaining() < 1) {
            logger.info("Buffer is empty");
            return;
        }

        var operationByte = (char) buffer.get();
        var operation = getOperationEnum(operationByte);
        var cobyBuffer = buffer.duplicate();

        switch (operation) {
            case BEGIN, COMMIT:
                logger.info(operation.name());
                break;
            case RELATION:
                int relationId = buffer.getInt();
                var changeStr = new String(cobyBuffer.array(), cobyBuffer.arrayOffset(), cobyBuffer.array().length - cobyBuffer.arrayOffset(),
                        StandardCharsets.UTF_8);
                var relationDto = StringParser.crateRelationMetaInfo(changeStr);
                LocalCache.putRelationMetaInfo(relationId, relationDto);
                logger.info("RelationDto {}", relationDto);
                break;
            case INSERT, UPDATE, DELETE:
                changesStructureConsumer.accept(createRowChangesStructure(cobyBuffer, operation));
                break;
            case TRUNCATE:
                changesStructureConsumer.accept(createRowChangesStructure(cobyBuffer, operation));
                break;
            default:
                logger.error("Unsupported command: {}", operationByte);
        }

    }


}
