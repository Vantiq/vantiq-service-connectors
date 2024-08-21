package io.vantiq.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vantiq.svcconnector.SvcConnSvrMessage;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Put together the basic operations for interacting with the storage manager over a websocket.<p/>
 * 
 * On connect we establish a websocket connection using the default port. Once established the Storage Manager API-like
 * calls can be made. Every entry point is synchronous.
 * <p/>
 * A limited copy of the SocketSession in the MongoDB Atlas manager's tests.
 * <p/>
 * Copyright (c) 2024 Vantiq, Inc.
 * <p/>
 * All rights reserved.
 */
@Slf4j
public class SocketSession {
    private final static ObjectMapper mapper = DatabindCodec.mapper();
    WebSocket websocket;
    @Setter
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
            //noinspection ResultOfMethodCallIgnored
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
    
    private void writeAndHandle(String procName, Map<String, Object> params, Boolean isSystemRequest,
                                Consumer<JsonObject> handler) {
        SvcConnSvrMessage message = new SvcConnSvrMessage();
        if (params != null && !params.containsKey("options")) {
            // add an empty options map, watch out for immutable params args
            params = new HashMap<>(params);
            params.put("options", new HashMap<>());
        }
        message.params = params;
        message.procName = procName;
        message.requestId = UUID.randomUUID().toString();
        if (isSystemRequest != null) {
            message.isSystemRequest = isSystemRequest;
        }

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
            //noinspection ResultOfMethodCallIgnored
            sync.await(15, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            error.set(new RuntimeException(ie));
        }
        if (error.get() != null) {
            errorHandler.accept(error.get());
        }
    }
    
    public int count(String storageName, Map<String, Map<String, String>> qual, Boolean isSystemNamespace) {
        AtomicInteger count = new AtomicInteger();
        String procName = "com.pkg.myService.count";
        writeAndHandle(procName, Map.of("storageManagerReference", Map.of(), "storageName", storageName, "qual", qual),
                isSystemNamespace, json -> count.set(json.getInteger("result")));
        return count.get();
    }
}
