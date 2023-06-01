package io.vantiq.testServiceConnector;

import com.google.common.collect.Lists;
import io.vantiq.svcconnector.OpResult;
import io.vantiq.svcconnector.VantiqStorageManager;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class TestStorageManagerImpl implements VantiqStorageManager {
    @Override
    public void initialize() {
    }

    @Override
    public OpResult<Map<String, Object>> initializeTypeDefinition(Map<String, Object> proposedType, Map<String, Object> existingType) {
        Object props = proposedType.get("properties");
        if (!(props instanceof Map)) {
            throw new IllegalArgumentException("Created types should have a Map of properties");
        }
        //noinspection unchecked
        Object raw = ((Map<String, Object>) props).get("illegalName");
        if (raw != null) {
            throw new IllegalArgumentException("Created types cannot use an illegalName");
        }
        return new OpResult<>(proposedType);
    }

    @Override
    public OpResult<Map<String, Object>> insert(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> values) {
        return new OpResult<>(values);
    }

    @Override
    public OpResult<Map<String, Object>> update(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> values, Map<String, Object> qual) {
        return new OpResult<>(values);
    }

    @Override
    public OpResult<Integer> count(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual, Map<String, Object> options) {
        return new OpResult<>(3);
    }

    @Override
    public OpResult<List<Map<String, Object>>> insertMany(String storageName, Map<String, Object> storageManagerReference,
                                                          List<Map<String, Object>> values) {
        return new OpResult<>(values);
    }

    @Override
    public OpResult<Map<String, Object>> select(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> properties, Map<String, Object> qual, Map<String, Object> options) {
        return new OpResult<>() {
            int count = 0;

            public boolean hasNext() {
                return count < 3;
            }

            public Map<String, Object> next() {
                if (count >= 3) {
                    throw new NoSuchElementException("Gone past the end of the cursor");
                }
                count++;
                Map<String, Object> result = new LinkedHashMap<>(1);
                result.put("name", "El Greco");
                result.put("address", "123 Main Street, USA");
                result.put("count", count);
                return result;
            }
        };
    }

    @Override
    public OpResult<Map<String, Object>> selectOne(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> properties, Map<String, Object> qual, Map<String, Object> options) {
        Map<String, Object> result = new LinkedHashMap<>(1);
        result.put("name", "El Greco");
        result.put("address", "123 Main Street, USA");
        result.put("count", 11);
        return new OpResult<>(result);
    }

    @Override
    public OpResult<Integer> delete(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual) {
        return new OpResult<>(3);
    }

    @Override
    public OpResult<Map<String, Object>> getTypeRestrictions() {
        Map<String, Object> restrictions = new HashMap<>(3);
        restrictions.put("restrictedOps", Lists.newArrayList("createIndex"));
        restrictions.put("restrictedDeclaredTypes", Lists.newArrayList("GeoJSON"));
        restrictions.put("restrictedTypeProperties", Lists.newArrayList("audited", "expiresAfter", "groupEnabled"));
        return new OpResult<>(restrictions);
    }
}
