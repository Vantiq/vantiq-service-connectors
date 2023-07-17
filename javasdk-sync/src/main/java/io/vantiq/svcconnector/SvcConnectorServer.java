package io.vantiq.svcconnector;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class SvcConnectorServer {
    Vertx vertx;
    public void start(SvcConnectorConfig config) {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
        
        // convert the config to a JsonObject -- suitable for verticle deployment
        JsonObject verticleConfig = new JsonObject().put("storageManagerClassName", config.getStorageManagerClassName());
        DeploymentOptions deployOptions = new DeploymentOptions()
                .setConfig(verticleConfig);
        vertx = Vertx.vertx();
        vertx.deployVerticle(new MainVerticle(), deployOptions).onFailure(t -> {
            System.err.println("Failed to deploy main verticle: " + t.getMessage());
            vertx.close();
        });
    }
}
