package io.vantiq.svcconnector;

import static io.vantiq.svcconnector.InstanceConfigUtils.CONNECT_THREADS_PROP;
import static io.vantiq.svcconnector.InstanceConfigUtils.WORKER_THREADS_PROP;

import io.vantiq.utils.StorageManagerError;
import io.vantiq.utils.StorageManagerErrorCodec;
import io.vantiq.utils.SvcConnSvcMsgCodec;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

/**
 * The main class for the service connector server.  This class is responsible for starting the server and deploying the
 * verticles that handle incoming requests from the vantiq server and process them. It is necessitated by the dance with
 * configuring and starting Vert.x
 * <p/>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p/>
 * All rights reserved.
 */
@Slf4j
public class SvcConnectorServer {
    static Vertx vertx;
    
    public void start(SvcConnectorConfig config) {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
        
        // convert the config to a JsonObject -- suitable for verticle deployment
        JsonObject verticleConfig = new JsonObject().put("storageManagerClassName", config.getStorageManagerClassName());
        Runtime runtime = Runtime.getRuntime();
        // ToDo: will want to adjust some vertx options here (blocked thread checking etc.) when in debug mode.
        VertxOptions options = new VertxOptions();
        vertx = Vertx.vertx(options);
        
        // define a couple of codecs, so we can send messages between verticles
        vertx.eventBus().registerDefaultCodec(SvcConnSvrMessage.class, new SvcConnSvcMsgCodec());
        vertx.eventBus().registerDefaultCodec(StorageManagerError.class, new StorageManagerErrorCodec());

        Properties properties = new InstanceConfigUtils().loadServerConfig();
        // unless overridden via property setting start 1 connection thread per processor
        int nConnectThreads = Integer.parseInt(properties.getProperty(CONNECT_THREADS_PROP,
                String.valueOf(runtime.availableProcessors())));
        
        // unless overridden via property setting start 4x the number of processors of the storage manager verticle
        int nWorkerThreads = Integer.parseInt(properties.getProperty(WORKER_THREADS_PROP,
                String.valueOf(4*runtime.availableProcessors())));
        
        log.info("Detected {} processors, starting {} instances of the websocket processing verticle and " +
                        "{} instances of the storage manager verticle",
                runtime.availableProcessors(), nConnectThreads, nWorkerThreads);
        
        // start a web socket verticle for each of the processors. it is the case that once a connection is established
        // all subsequent requests are handled by the same vertx event loop.
        DeploymentOptions deployOptions = new DeploymentOptions()
                .setInstances(nConnectThreads)
                .setConfig(verticleConfig);
        vertx.deployVerticle(WebSocketRequestVerticle::new, deployOptions).onFailure(t -> {
            System.err.println("Failed to deploy main verticle: " + t.getMessage());
            vertx.close();
            System.exit(1);
        });
        
        deployOptions.setInstances(nWorkerThreads);
        vertx.deployVerticle(StorageManagerVerticle::new, deployOptions).onFailure(t -> {
            System.err.println("Failed to deploy storage manager verticle: " + t.getMessage());
            vertx.close();
            System.exit(1);
        }).onComplete(ar -> {
            if (ar.succeeded()) {
                log.info("Successfully deployed {} instances of the storage manager verticle",
                        deployOptions.getInstances());
            } else {
                log.error("Failed to deploy storage manager verticle", ar.cause());
            }
        });
    }
}
