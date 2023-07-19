package io.vantiq.svcconnector;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestStorageManager implements VantiqStorageManager {
    @Override
    public Completable initialize() {
        return Completable.complete();
    }

    @Override
    public Single<Map<String, Object>> getTypeRestrictions() {
        return Single.just(Collections.emptyMap());
    }

    @Override
    public Single<Map<String, Object>> initializeTypeDefinition(Map<String, Object> proposedType, Map<String, Object> existingType) {
        return Single.just(Collections.emptyMap());
    }

    @Override
    public Completable typeDefinitionDeleted(Map<String, Object> type, Map<String, Object> options) {
        return Completable.complete();
    }

    @Override
    public Single<Map<String, Object>> insert(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> values) {
        return Single.just(Collections.emptyMap());
    }

    @Override
    public Flowable<Map<String, Object>> insertMany(String storageName, Map<String, Object> storageManagerReference, List<Map<String, Object>> values) {
        return Flowable.empty();
    }

    @Override
    public Maybe<Map<String, Object>> update(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> values, Map<String, Object> qual) {
        return Maybe.empty();
    }

    @Override
    public Single<Integer> count(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual, Map<String, Object> options) {
        return Single.just(0);
    }

    @Override
    public Flowable<Map<String, Object>> select(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> properties, Map<String, Object> qual, Map<String, Object> options) {
        return Flowable.empty();
    }

    @Override
    public Maybe<Map<String, Object>> selectOne(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> properties, Map<String, Object> qual, Map<String, Object> options) {
        return Maybe.empty();
    }

    @Override
    public Single<Integer> delete(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual) {
        return Single.just(0);
    }
}
