package io.vantiq.util;
/*
 * <p>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import lombok.val;

import java.util.concurrent.atomic.AtomicReference;

public class CacheIfNotEmptyOrError<T> implements FlowableTransformer<T, T> {
    private final AtomicReference<Flowable<T>> cachedRef = new AtomicReference<>();

    @NonNull
    @Override
    public Flowable<T> apply(@NonNull Flowable<T> source) {
        return Flowable.defer(() -> {
            // Spin loop until we get a definitive result
            for (;;) {
                // See if we already have a cached observable, if so, use it
                val cached = cachedRef.get();
                if (cached != null) {
                    return cached;
                }

                // Construct a cached observable that clears the cache if it is empty or generates an error
                val newObs = source.cache()
                        .switchIfEmpty(Flowable.<T>empty().doOnComplete(() -> cachedRef.set(null)))
                        .onErrorResumeNext( ex -> {
                            cachedRef.set(null);
                            return Flowable.error(ex);
                        });

                // Make sure no one beat us to this
                if (cachedRef.compareAndSet(null, newObs)) {
                    return newObs;
                }
            }
        });
    }
}