package io.vantiq.testServiceConnector;

import com.google.common.collect.Lists;
import io.vantiq.svcconnector.OpResult;
import io.vantiq.svcconnector.VantiqStorageManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class MemoryStorageManagerImpl implements VantiqStorageManager {
    Map<String, Map<String, Map<String, Object>>> storage = new LinkedHashMap<>();
    Map<String, Map<String, Object>> typeMap = new LinkedHashMap<>();
    @Override
    public void initialize() {
    }

    @Override
    public OpResult<Map<String, Object>> initializeTypeDefinition(Map<String, Object> proposedType, Map<String, Object> existingType) {

        Object raw = proposedType.get("name");
        if (!(raw instanceof String)) {
            throw new IllegalArgumentException("Created types must have a name");
        } else {
            String newTypeName = (String) raw;

            // Else, if we have properties, require a name...
            //noinspection unchecked
            Map<String, Map<String, Object>> props = (Map<String, Map<String,Object>>) proposedType.get("properties");
            if (props != null && props.size() > 0) {
                if (!(props.containsKey("name"))) {
                    throw new IllegalArgumentException("Created types must have name property");
                } else {
                    String nprop = (String) props.get("name").get("type");
                    if (!"String".equals(nprop)) {
                        throw new IllegalArgumentException("Created types must have name property of type 'String'");
                    }
                }
            }
            typeMap.put(newTypeName, proposedType);
            storage.put(newTypeName, new LinkedHashMap<>());
        }
        return new OpResult<>(proposedType);
    }

    @Override
    public OpResult<Void> typeDefinitionDeleted(Map<String, Object> type, Map<String, Object> options) {
        typeMap.remove((String)type.get("name"));
        storage.remove((String)type.get("name"));
        return new OpResult<>(null);
    }

    @Override
    public OpResult<Map<String, Object>> insert(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> values) {
        System.out.println("insert() called with storageName: " + storageName + " & values: " + values);
        storage.get(storageName).put((String) values.get("name"), values);
        return new OpResult<>(values);
    }

    @Override
    public OpResult<Map<String, Object>> update(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> values, Map<String, Object> qual) {
        storage.get(storageName).put((String) values.get("name"), values);
        return new OpResult<>(values);
    }

    @Override
    public OpResult<Integer> count(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual, Map<String, Object> options) {
        if (qual != null && !qual.isEmpty()) {
            String key = (String) qual.get("name");
            if (key == null) {
                throw new IllegalArgumentException("SelectOne requires a qualification on 'name'");
            }
            Map<String, Object> res = storage.get(storageName).get(key);
            int count = res == null ? 0 : 1;
            return new OpResult<>(count);
        } else {
            return new OpResult<>(storage.get(storageName).size());
        }
    }

    @Override
    public OpResult<List<Map<String, Object>>> insertMany(String storageName, Map<String, Object> storageManagerReference,
                                                          List<Map<String, Object>> values) {
        values.forEach( aVal -> storage.get(storageName).put((String) aVal.get("name"), aVal));
        return new OpResult<>(values);
    }

    @Override
    public OpResult<Map<String, Object>> select(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> properties, Map<String, Object> qual, Map<String, Object> options) {
        return new OpResult<>() {
            int count = 0;
            int storeSize = storage.get(storageName).size();

            public boolean hasNext() {
                return count < storeSize;
            }

            public Map<String, Object> next() {
                if (count > storeSize) {
                    throw new NoSuchElementException("Gone past the end of the cursor");
                }
                Integer startAt = (Integer) options.get("skip");
                if (startAt == null || startAt == 0) {
                    startAt = count;
                }
                if (qual != null && !qual.isEmpty()) {
                    String key = (String) qual.get("name");
                    if (key == null) {
                        throw new IllegalArgumentException("SelectOne requires a qualification on 'name'");
                    }
                    Map<String, Object> res = storage.get(storageName).get(key);
                    count = storeSize + 1; // That's all there is...
                    return res == null ? Collections.emptyMap() : res;
                } else {
                    List<Map<String, Object>> vals = new ArrayList<>(storage.get(storageName).values());
                    if (startAt < vals.size()) {
                        count++;
                        return vals.get(startAt);
                    }
                    return Collections.emptyMap();
                }
            }
        };
    }

    @Override
    public OpResult<Map<String, Object>> selectOne(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> properties, Map<String, Object> qual, Map<String, Object> options) {
        Map<String, Object> result = new LinkedHashMap<>(1);
        if (qual != null) {
            String key = (String) qual.get("name");
            if (key == null) {
                throw new IllegalArgumentException("SelectOne requires a qualification on 'name'");
            }
            Map<String, Object> res = storage.get(storageName).get(key);
            result.putAll(res);
            return new OpResult<>(result);
        } else {
            throw new IllegalArgumentException("No qualification for SelectOne");
        }
    }

    @Override
    public OpResult<Integer> delete(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual) {
        if (qual != null) {
            String key = (String) qual.get("name");
            if (key == null) {
                throw new IllegalArgumentException("delete requires a qualification on 'name'");
            }
            Map<String, Object> res = storage.get(storageName).remove(key);
            return new OpResult<>(res == null ? 0 : 1);
        } else {
            // delete sans qual means dump them all
            int formerSize = storage.get(storageName).size();
            storage.get(storageName).clear();
            return new OpResult<>(formerSize);
        }
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
