package io.vantiq.svcconnector;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

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
    @BeforeEach
    void deploy_verticle(Vertx vertx, VertxTestContext testContext) {
        JsonObject verticleConfig = new JsonObject().put("storageManagerClassName",
                "io.vantiq.svcconnector.NoopStorageManager");
        DeploymentOptions deployOptions = new DeploymentOptions()
                .setInstances(1)
                .setConfig(verticleConfig);
        Supplier<Verticle> verticleSupplier = RequestProcessingVerticle::new;
        vertx.deployVerticle(verticleSupplier, deployOptions, ar -> {
            if (ar.succeeded()) {
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    /*
     * This test sends a ping to the storage manager service connector and expects a pong in response
     */
    @Test
    void verticle_deployed(Vertx vertx, VertxTestContext testContext) throws Throwable {
        vertx.createHttpClient().webSocket(8888, "localhost", "/wsock/websocket", ar -> {
            if (ar.succeeded()) {
                ar.result().handler(buffer -> {
                    System.out.println("received message from server: " + buffer.toString());
                    if (buffer.toString().equals("pong")) {
                        testContext.completeNow();
                    } else {
                        testContext.failNow(new Throwable("unexpected response from server: " + buffer.toString()));
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
}
