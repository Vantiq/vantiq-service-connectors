package io.vantiq.util;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.Weigher;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.Shareable;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Atomic, shareable object reference.
 * <p/>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p/>
 * All rights reserved.
 */
public class VertxWideData<T> extends AtomicReference<T> implements Shareable {
    public static final String GLOBALS_MAP_NAME = "io.vantiq.vertx.globalData";

    VertxWideData(T ref) { //noinspection GroovyAssignabilityCheck
        super(ref);
    }

    /**
     * Get a concurrent map which will be shared across the given Vertx instance.
     *
     * @param vertx current vertx
     * @param mapName name of the map
     * @param <V> type of map values
     * @return the vertx wide map
     */
    @SuppressWarnings("unused")
    public static <V> ConcurrentMap<String, V> getVertxWideMap(Vertx vertx, String mapName) {
        LocalMap<String, VertxWideData<ConcurrentMap<String, V>>> globalsMap = vertx.sharedData().getLocalMap(GLOBALS_MAP_NAME);
        VertxWideData<ConcurrentMap<String, V>> sharedMapGlobal = new VertxWideData<>(new ConcurrentHashMap<>());
        if (globalsMap.putIfAbsent(mapName, sharedMapGlobal) != null) {
            sharedMapGlobal = globalsMap.get(mapName);
        }
        return sharedMapGlobal.get();
    }

    /**
     * Get a Guava Cache instance which will be shared across the given Vert.x instance.
     *
     * @param vertx current vertx
     * @param mapName name for the cache we find or create
     * @param cacheSpec guava cache specification
     * @param ticker time source
     * @param removalListener listener for removal events
     * @param weigher weigher for cache entries
     * @return the vertx wide cache either newly created or found in vertx shared data
     */
    public static <K, V> Cache<K, V> getVertxWideCache(Vertx vertx, String mapName, String cacheSpec, Ticker ticker,
                                                RemovalListener<K, V> removalListener, Weigher<K, V> weigher) {
        return getVertxWideInstance(vertx, mapName, () -> {
            @SuppressWarnings("unchecked")
            CacheBuilder<K,V> builder = (CacheBuilder<K, V>) CacheBuilder.from(cacheSpec);
            if (ticker != null) {
                builder.ticker(ticker);
            }
            if (removalListener != null) {
                builder.removalListener(removalListener);
            }
            if (weigher != null) {
                builder.weigher(weigher);
            }
            return builder.build();
        }); 
    }

    /**
     * Return an object instance that is shared across the given Vert.x.  If the object does not yet exist, it will
     * be obtained from the given supplier.  Otherwise, the shared instance will be returned.
     *
     * @param vertx current vertx
     * @param instanceName name of the instance
     * @param instanceSupplier supplier for the instance
     * @return the vertx wide instance
     */
    public static <T> T getVertxWideInstance(Vertx vertx, String instanceName, Supplier<T> instanceSupplier) {
        LocalMap<String, VertxWideData<T>> globalsMap = vertx.sharedData().getLocalMap(GLOBALS_MAP_NAME);
        VertxWideData<T> instanceGlobal = globalsMap.computeIfAbsent(instanceName, key ->
            new VertxWideData<>(instanceSupplier.get())
        );
        T instance = instanceGlobal.get();
        Objects.requireNonNull(instance, "Supplier for vertx wide instance ${instanceName} returned null.");
        return instance;
    }

    /**
     * Return an object instance that is shared across the given Vert.x.  If the object does not yet exist, it will
     * be obtained from the given supplier.  Otherwise, the shared instance will be returned.
     *
     * @param vertx current vertx
     * @param instanceName the name of the instance
     * @return the vertx wide instance
     */
    @SuppressWarnings("unused")
    public static <T> T getVertxWideInstance(Vertx vertx, String instanceName) {
        LocalMap<String, VertxWideData<T>> globalsMap = vertx.sharedData().getLocalMap(GLOBALS_MAP_NAME);
        VertxWideData<T> instanceGlobal = globalsMap.get(instanceName);
        return instanceGlobal != null ? instanceGlobal.get() : null;
    }

    /**
     * Remove the named Vert.x wide instance.
     *
     * @param vertx current vertx
     * @param dataName name of the instance
     */
    @SuppressWarnings("unused")
    static void removeVertxWideData(Vertx vertx, String dataName) {
        LocalMap<String, VertxWideData<?>> globalsMap = vertx.sharedData().getLocalMap(GLOBALS_MAP_NAME);
        globalsMap.remove(dataName);
    }

    /**
     * Remove all Vert.x wide instances for the given Vert.x.
     *
     * @param vertx current vertx
     */
    @SuppressWarnings("unused")
    static void clearAll(Vertx vertx) {
        LocalMap<String, VertxWideData<Object>> globalsMap = vertx.sharedData().getLocalMap(GLOBALS_MAP_NAME);
        globalsMap.forEach( (name, data) -> {
                Object obj = data.getAndSet(null);
            if (obj instanceof Collection) {
                ((Collection<?>)obj).clear();
            }
        });
        globalsMap.clear();
    }
}