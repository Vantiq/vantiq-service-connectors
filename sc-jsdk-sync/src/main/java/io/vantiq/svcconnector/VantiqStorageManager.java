package io.vantiq.svcconnector;

import java.util.List;
import java.util.Map;

public interface VantiqStorageManager {
    enum apis {
        fetchTypeRestrictions,
        initializeType,
    }

    /**
     * do anything necessary prior to receiving storage manager related requests
     */
    void initialize();

    /**
     * What is and is not supported for types managed by this storage manager
     *
     * @return map of key value pairs for the restrtions. see
     * <a href= https://dev.vantiq.com/docs/system/storagemanagers/index.html#restricting-capabilities>restrictions</a>
     */
    OpResult<Map<String, Object>> getTypeRestrictions();

    OpResult<Map<String, Object>> initializeTypeDefinition(Map<String, Object> proposedType,
                                                           Map<String, Object> existingType);

    OpResult<Void> typeDefinitionDeleted(Map<String, Object> type, Map<String, Object> options);
    
    OpResult<Map<String, Object>> insert(String storageName, Map<String, Object> storageManagerReference,
                                         Map<String, Object> values);
    
    OpResult<List<Map<String, Object>>> insertMany(String storageName, Map<String, Object> storageManagerReference,
                                                   List<Map<String, Object>> values);
    
    OpResult<Map<String, Object>> update(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> values,
                                         Map<String, Object> qual);
    
    OpResult<Integer> count(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual,
                            Map<String, Object> options);
    
    OpResult<Map<String, Object>> select(String storageName, Map<String, Object> storageManagerReference,
                                         Map<String, Object> properties, Map<String, Object> qual, Map<String, Object> options);
    
    OpResult<Map<String, Object>> selectOne(String storageName, Map<String, Object> storageManagerReference,
                                            Map<String, Object> properties, Map<String, Object> qual,
                                            Map<String, Object> options);
    
    OpResult<Integer> delete(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual);
}
