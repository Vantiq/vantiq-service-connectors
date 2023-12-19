package io.vantiq.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vantiq.svcconnector.SvcConnSvrMessage;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Put together the basic operations for interacting with the storage manager over a websocket.
 * 
 * On connect we establish a websocket connection using the default port. Once established the Storage Manager API-like
 * calls can be made. Every entry point is synchronous.
 * <p/>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p/>
 * All rights reserved.
 */
@Slf4j
public class SocketSession {
    private final static ObjectMapper mapper = DatabindCodec.mapper();
    WebSocket websocket;
    Consumer<Throwable> errorHandler;
    
    public SocketSession(Consumer<Throwable> errorHandler) {
        this.errorHandler = errorHandler;
    }
    
    public void connect(Vertx vertx) {
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        val sync = new CountDownLatch(1);
        
        vertx.createHttpClient().webSocket(8888, "localhost", "/wsock/websocket", ar -> {
            if (ar.succeeded()) {
                // save the websocket for later use
                websocket = ar.result();

                // try / handle ping message to make sure we're connected
                websocket.handler(buffer -> {
                    // got response to ping (hopefully, its a pong)
                    log.debug("received message from server: " + buffer);
                    if (!buffer.toString().equals("pong")) {
                        errorRef.set(new Throwable("unexpected response from server: " + buffer));
                    }
                    sync.countDown();
                });

                websocket.write(Buffer.buffer("ping"), writeAr -> {
                    if (writeAr.failed()) {
                        // could not write to the websocket, set the error and proceed
                        errorRef.set(writeAr.cause());
                        sync.countDown();
                    }
                });
            } else {
                // could not create the websocket, set the error and proceed
                errorRef.set(ar.cause());
                sync.countDown();
            }
        });
        try {
            sync.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            errorRef.set(e);
        }
        if (errorRef.get() != null) {
            errorHandler.accept(errorRef.get());
        }
    }
    
    public void disconnect() {
        if (websocket != null) {
            websocket.close();
            websocket = null;
        }
    }

    private void writeAndHandle(String procName, Map<String, Object> params, Consumer<JsonObject> handler) {
        SvcConnSvrMessage message = new SvcConnSvrMessage();
        if (params != null && !params.containsKey("options")) {
            // add an empty options map, watch out for immutable params args
            params = new HashMap<>(params);
            params.put("options", new HashMap<>());
        }
        message.params = params;
        message.procName = procName;
        message.requestId = UUID.randomUUID().toString();

        // make the call synchronous
        final CountDownLatch sync = new CountDownLatch(1);

        AtomicReference<RuntimeException> error = new AtomicReference<>();
        // actually set up the handler first
        websocket.handler(buffer -> {
            JsonObject result = (JsonObject)buffer.toJson();
            if (result.getString("errorMsg") != null) {
                error.set(new RuntimeException(result.getString("errorMsg")));
            }
            // will get two (or more) results, one for the procedure call and one for the EOF -- ignore the latter
            if (result.getBoolean("isEOF")) {
                sync.countDown();
                return;
            }
            if (handler != null) {
                handler.accept(result);
            }
        });

        // send a fully qualified procedure invocation
        try {
            websocket.write(Buffer.buffer(mapper.writeValueAsBytes(message)), ar -> {
                if (ar.failed()) {
                    errorHandler.accept(ar.cause());
                }
            });
        } catch (JsonProcessingException e) {
            errorHandler.accept(e);
            return;
        }

        try {
            sync.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            error.set(new RuntimeException(ie));
        }
        if (error.get() != null) {
            errorHandler.accept(error.get());
        }
    }

    public void typeDefinitionDeleted(Map<String, Object> testType) {
        writeAndHandle("com.pkg.myService.typeDefinitionDeleted", Map.of("type", testType), null);
    }

    public Map<String, Object> getTypeRestrictions() {
        AtomicReference<Map<String, Object>> restrs = new AtomicReference<>();
        writeAndHandle("com.pkg.myService.getTypeRestrictions", null, json ->
                restrs.set(json.getJsonObject("result").getMap())
        );
        return restrs.get();
    }

    public Map<String, Object> initializeTypeDefinition(Map<String, Object> typeDefinition) {
        AtomicReference<Map<String, Object>> resultType = new AtomicReference<>();
        String procName = "com.pkg.myService.initializeTypeDefinition";
        writeAndHandle(procName, Map.of("proposedType", new JsonObject(typeDefinition)), json ->
                resultType.set(json.getJsonObject("result").getMap())
        );
        return resultType.get();
    }

    public Map<String, Object> insert(String storageName, Map<String, Object> instance) {
        String procName = "com.pkg.myService.insert";
        AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        writeAndHandle(procName, Map.of("storageManagerReference", Map.of(), "storageName", storageName, "values", instance), json ->
                result.set(json.getJsonObject("result").getMap()));
        return result.get();
    }

    public List<Map<String, Object>> insertMany(@SuppressWarnings("SameParameterValue") String storageName,
                           List<Map<String, Object>> instances) {
        List<Map<String, Object>> allResults = new ArrayList<>();
        
        String procName = "com.pkg.myService.insertMany";
        writeAndHandle(procName, Map.of("storageManagerReference", Map.of(), "storageName", storageName, "values",
                instances), json -> allResults.add(json.getJsonObject("result").getMap()));
        return allResults;
    }
    
    @SuppressWarnings("SameParameterValue")
    public Map<String, Object> selectOne(String storageName, List<String> properties, Map<String, Object> qual) {
        AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        // storage manager API expects a property map, in our case just map the property to itself
        Map<String, Object> propertyMap = properties.stream().collect(HashMap::new, (m, v) -> m.put(v, v),
                HashMap::putAll);
        String procName = "com.pkg.myService.selectOne";
        writeAndHandle(procName, Map.of("storageManagerReference", Map.of(), "storageName", storageName, "properties",
                propertyMap, "qual", qual), json -> result.set(json.getJsonObject("result").getMap()));
        return result.get();
    }

    public List<Map<String, Object>> select(String storageName, List<String> properties, Map<Object, Object> qual) {
        // storage manager API expects a property map, in our case just map the property to itself
        Map<String, Object> propertyMap = properties.stream().collect(HashMap::new, (m, v) -> m.put(v, v),
                HashMap::putAll);
        String procName = "com.pkg.myService.select";
        List<Map<String, Object>> allResults = new ArrayList<>();
        writeAndHandle(procName, Map.of("storageManagerReference", Map.of(), "storageName", storageName, "properties",
                propertyMap, "qual", qual), json -> allResults.add(json.getJsonObject("result").getMap()));
        return allResults;
    }

    public Map<String, Object> update(String storageName, Map<String, String> qual, Map<String, Integer> instance) {
        String procName = "com.pkg.myService.update";
        AtomicReference<Map<String, Object>> result = new AtomicReference<>();
        writeAndHandle(procName, Map.of("storageManagerReference", Map.of(), "storageName", storageName, "values",
                instance, "qual", qual), json -> result.set(json.getJsonObject("result").getMap()));
        return result.get();
    }

    public int delete(String storageName, Map<String, String> qual) {
        AtomicInteger count = new AtomicInteger();
        String procName = "com.pkg.myService.delete";
        writeAndHandle(procName, Map.of("storageManagerReference", Map.of(), "storageName", storageName, "qual", qual),
                json -> count.set(json.getInteger("result")));
        return count.get();
    }

    public int count(String storageName, Map<String, Map<String, String>> qual) {
        AtomicInteger count = new AtomicInteger();
        String procName = "com.pkg.myService.count";
        writeAndHandle(procName, Map.of("storageManagerReference", Map.of(), "storageName", storageName, "qual", qual),
                json -> count.set(json.getInteger("result")));
        return count.get();
    }

    public List<Map<String, Object>> aggregate(String storageName, List<Map<String, Object>> pipeline) {
        List<Map<String, Object>> results = new ArrayList<>();
        String procName = "com.pkg.myService.aggregate";
        writeAndHandle(procName, Map.of("storageManagerReference", Map.of(), "storageName", storageName, "pipeline",
                        pipeline), json -> {
            results.add(json.getJsonObject("result").getMap());
        });
        return results;
    }
}
