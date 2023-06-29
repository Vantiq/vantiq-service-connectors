package io.vantiq.svcconnector;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

@Slf4j
public class SvcConnectorServer {
    static Vertx vertx;
    
    public static Vertx getVertx() {
        if (vertx == null) {
            throw new IllegalStateException("Vertx has not been initialized");
        }
        return vertx;
    }
    
    public void start(SvcConnectorConfig config) {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
        
        // convert the config to a JsonObject -- suitable for verticle deployment
        JsonObject verticleConfig = new JsonObject().put("storageManagerClassName", config.getStorageManagerClassName());
        Runtime runtime = Runtime.getRuntime();
        log.info("Detected {} processors, starting {} instances of the request processing verticle",
                runtime.availableProcessors(), 4*runtime.availableProcessors());
        DeploymentOptions deployOptions = new DeploymentOptions()
                .setInstances(4*runtime.availableProcessors())
                .setConfig(verticleConfig);
        vertx = Vertx.vertx();
        Supplier<Verticle> verticleSupplier = RequestProcessingVerticle::new;
        vertx.deployVerticle(verticleSupplier, deployOptions).onFailure(t -> {
            System.err.println("Failed to deploy main verticle: " + t.getMessage());
            vertx.close();
        });
    }
}
