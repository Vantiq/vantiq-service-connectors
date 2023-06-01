package io.vantiq.svcconnector;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;
import io.vertx.ext.web.sstore.LocalSessionStore;
import io.vertx.ext.web.sstore.SessionStore;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MainVerticle extends AbstractVerticle implements Sessionizer {
    @Getter
    SessionStore sessionStore;
    @Getter
    SessionCreator sessionCreator;
    static VantiqStorageManager storageManager;

    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
        sessionStore = LocalSessionStore.create(vertx);
        sessionCreator = new SessionCreator(vertx, sessionStore);
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        String className = config().getString("storageManagerClassName");
        if (className == null) {
            throw new Exception("storageManager implementation class must be configured when starting the service connector");
        }
        // now load the class from the class name and instantiate the VantiqStorageManager object
        Class<?> clazz = Class.forName(className);
        storageManager = (VantiqStorageManager) clazz.getDeclaredConstructor().newInstance();
        storageManager.initialize();
        
        InstanceConfigUtils config = new InstanceConfigUtils();
        config.loadServerConfig();
        Router router = Router.router(vertx);
        router.get("/").handler(req -> req.response()
            .putHeader("content-type", "text/plain")
            .end("Hello from the Service Connector!"));
        router.get("/healthz").handler(req -> req.response()
                .putHeader("content-type", "text/plain")
                .end("Healthy"));
        SockJSHandlerOptions options = new SockJSHandlerOptions().setMaxBytesStreaming(1024 * 1024);
        SockJSHandler sockJSHandler = SockJSHandler.create(getVertx(), options);
        router.route("/wsock/*").subRouter(sockJSHandler.socketHandler(this::handleWebsocketRequest));
        vertx.createHttpServer()
            .requestHandler(router)
            .listen(config.obtainPrimaryPort(), http -> {
                if (http.succeeded()) {
                    startPromise.complete();
                    log.info("HTTP server started on port " + config.obtainPrimaryPort());
                } else {
                    startPromise.fail(http.cause());
                }
            });
    }

    public void handleWebsocketRequest(SockJSSocket socket) {
        System.out.println("new websocket connection");
        ConnectorListener listener = new ConnectorListener(socket, storageManager, this);
    }
}
