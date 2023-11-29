package io.vantiq.svcconnector;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

import java.util.List;
import java.util.Map;

/**
 * Interface for storage manager service connector implementations. It encapsulates the storage manager API used by
 * the Vantiq server to interact with storage managers.
 * <p>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */
public interface VantiqStorageManager {
    /**
     * do anything necessary prior to receiving storage manager related requests
     */
    Completable initialize();

    /**
     * What is and is not supported for types managed by this storage manager
     *
     * @return map of key value pairs for the restrtions. see
     * <a href= https://dev.vantiq.com/docs/system/storagemanagers/index.html#restricting-capabilities>restrictions</a>
     */
    Single<Map<String, Object>> getTypeRestrictions();

    Single<Map<String, Object>> initializeTypeDefinition(Map<String, Object> proposedType,
                                                           Map<String, Object> existingType);

    Completable typeDefinitionDeleted(Map<String, Object> type, Map<String, Object> options);

    Single<Map<String, Object>> insert(String storageName, Map<String, Object> storageManagerReference,
                                         Map<String, Object> values, Map<String, Object> options);
    
    Flowable<Map<String, Object>> insertMany(String storageName, Map<String, Object> storageManagerReference,
                                             List<Map<String, Object>> values, Map<String, Object> options);
    
    Maybe<Map<String, Object>> update(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> values,
                                         Map<String, Object> qual, Map<String, Object> options);
    
    Single<Integer> count(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual,
                            Map<String, Object> options);
    
    Flowable<Map<String, Object>> select(String storageName, Map<String, Object> storageManagerReference,
                                         Map<String, Object> properties, Map<String, Object> qual, Map<String, Object> options);
    
    Maybe<Map<String, Object>> selectOne(String storageName, Map<String, Object> storageManagerReference,
                                         Map<String, Object> properties, Map<String, Object> qual,
                                         Map<String, Object> options);
    
    Single<Integer> delete(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual,
                           Map<String, Object> options);
    
    Flowable<Map<String, Object>> aggregate(String storageName, Map<String, Object> storageManagerReference,
                                            List<Map<String, Object>> pipeline, Map<String, Object> options);

    Completable startTransaction(String vantiqTransactionId, Map<String, Object> storageManagerReference,
                                    Map<String, Object> options);

    Completable commitTransaction(String vantiqTransactionId, Map<String, Object> storageManagerReference,
                                 Map<String, Object> options);

    Completable abortTransaction(String vantiqTransactionId, Map<String, Object> storageManagerReference,
                                  Map<String, Object> options);
}
