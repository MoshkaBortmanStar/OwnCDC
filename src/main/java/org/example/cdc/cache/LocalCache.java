package org.example.cdc.cache;

import org.example.cdc.data.RelationMetaInfo;

import java.util.concurrent.ConcurrentHashMap;

public class LocalCache {


    private static ConcurrentHashMap<Integer, RelationMetaInfo> RELATION_META_INFO_MAP = new ConcurrentHashMap<>();



    public static void putRelationMetaInfo(Integer key, RelationMetaInfo relationMetaInfo) {
        RELATION_META_INFO_MAP.put(key, relationMetaInfo);
    }

    public static RelationMetaInfo getRelationMetaInfo(Integer key) {
        return RELATION_META_INFO_MAP.get(key);
    }


}
