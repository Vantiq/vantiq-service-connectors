package io.vantiq.svcconnector;

import static org.junit.jupiter.api.Assertions.fail;

import groovy.lang.Tuple2;
import io.vantiq.utils.SocketSession;
import io.vantiq.utils.StorageManagerError;
import io.vantiq.utils.StorageManagerErrorCodec;
import io.vantiq.utils.SvcConnSvcMsgCodec;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * basic sanity test for the service connector server -- bring it up and send a ping and hopefully get a pong back
 * <p>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */
@ExtendWith(VertxExtension.class)
public class TestMainVerticle {
    /*
     * Deploy a single instance of the request processing verticle for Mongodb Atlas
     */
    @BeforeAll
    static void deployVerticle(Vertx vertx, VertxTestContext testContext) {
        JsonObject verticleConfig = new JsonObject().put("storageManagerClassName",
                "io.vantiq.utils.NoopStorageManager");
        DeploymentOptions deployOptions = new DeploymentOptions()
                .setInstances(1)
                .setConfig(verticleConfig);
        Supplier<Verticle> verticleSupplier = WebSocketRequestVerticle::new;
        vertx.eventBus().registerDefaultCodec(StorageManagerError.class, new StorageManagerErrorCodec());
        vertx.eventBus().registerDefaultCodec(SvcConnSvrMessage.class, new SvcConnSvcMsgCodec());
        vertx.deployVerticle(verticleSupplier, deployOptions, ar -> {
            if (ar.succeeded()) {
                vertx.deployVerticle(StorageManagerVerticle::new, deployOptions, ar2 -> {
                    if (!ar2.succeeded()) {
                        testContext.failNow(ar2.cause());
                    }
                    testContext.completeNow();
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }
    
    private SocketSession session;
    @BeforeEach
    void setup(Vertx vertx, VertxTestContext vtc) {
        session = new SocketSession(t -> {
            vtc.failNow(t);
            fail("Unexpected error encountered in test", t);
        });
        session.connect(vertx);
        vtc.completeNow();
    }
    
    @AfterEach
    void cleanup() {
        // close the session
        session.disconnect();
    }

    /*
     * This test sends a ping to the storage manager service connector and expects a pong in response
     */
    @Test
    void testVerticleDeploy(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpClient().webSocket(8888, "localhost", "/wsock/websocket", ar -> {
            if (ar.succeeded()) {
                ar.result().handler(buffer -> {
                    System.out.println("received message from server: " + buffer);
                    if (buffer.toString().equals("pong")) {
                        testContext.completeNow();
                    } else {
                        testContext.failNow(new Throwable("unexpected response from server: " + buffer));
                    }
                });
                ar.result().write(Buffer.buffer("ping"), writeAr -> {
                    if (writeAr.failed()) {
                        testContext.failNow(writeAr.cause());
                    }
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }
    
    @Test
    void testPackagedProcInvoke(Vertx vertx, VertxTestContext testContext) {
        vertx.createHttpClient().webSocket(8888, "localhost", "/wsock/websocket", ar -> {
            if (ar.succeeded()) {
                ar.result().handler(buffer -> {
                    System.out.println("received message from server: " + buffer);
                    JsonObject result = (JsonObject)buffer.toJson();
                    // will get two results, one for the procedure call and one for the EOF -- ignore the latter
                    if (result.getBoolean("isEOF")) {
                        return;
                    }
                    // should get an empty map for type restrictions
                    if (result.getJsonObject("result").isEmpty()) {
                        testContext.completeNow();
                    } else {
                        testContext.failNow(new Throwable("unexpected response from server: " + buffer));
                    }
                });
                
                // send a fully qualified procedure invocation
                ar.result().write(Buffer.buffer("{\"procName\": \"com.pkg.myService.getTypeRestrictions\"}"), writeAr -> {
                    if (writeAr.failed()) {
                        testContext.failNow(writeAr.cause());
                    }
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }
    
    @Test
    void testsystemOnlyRestrictions() {
        // Count is set up so it's only legal from the system namespace. Check that it works there
        int count = session.count("", Collections.emptyMap(), true);
        assert count == 0;
        
        // When messages are not from the system namespace, it should error. Set up the handler for the errors.
        String expectedError = "Cannot run the procedure count from non-system namespaces.";
        AtomicBoolean gotError = new AtomicBoolean(false);
        session.setErrorHandler(ex -> {
            String message = ex.getMessage();
            if (message.equals(expectedError)) {
                gotError.set(true);
            } else {
                fail("Unexpected error running from non-system namespace", ex);
            }
        });
        
        // Check that it fails when explicitly not the system namespace
        session.count("", Collections.emptyMap(), false);
        assert gotError.get();
        
        // Reset the error checker and make sure it fails implicitly not the system namespace
        gotError.set(false);
        session.count("", Collections.emptyMap(), null);
        assert gotError.get();
    }
    
    public void writeBuffer(WebSocket ws, String jsonParams, VertxTestContext tc) {
        try {
            ws.write(Buffer.buffer(jsonParams), writeAr -> {
                if (writeAr.failed()) {
                    tc.failNow(writeAr.cause());
                }
            });
        } catch (Throwable t) {
            tc.failNow(t);
        }
    }
}
