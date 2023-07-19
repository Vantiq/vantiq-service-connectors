package io.vantiq.svcconnector;

import io.reactivex.rxjava3.core.Completable;
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

/**
 * This class is the main entry point for the service connector. It is responsible for starting the HTTP server and
 * handling incoming connection requests. Once a connection is established, it hands off the websocket to a 
 * ConnectionListener. 
 */
@Slf4j
public class RequestProcessingVerticle extends AbstractVerticle implements Sessionizer {
    @Getter
    SessionStore sessionStore;
    @Getter
    SessionCreator sessionCreator;
    static VantiqStorageManager storageManager;

    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
        // needed? for what?
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
        //noinspection ResultOfMethodCallIgnored
        storageManager.initialize().andThen(
            Completable.fromAction(() -> {
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
                vertx.createHttpServer().requestHandler(router).listen(config.obtainPrimaryPort(), http -> {
                    if (http.succeeded()) {
                        startPromise.complete();
                        log.info("HTTP server started on port " + config.obtainPrimaryPort());
                    } else {
                        startPromise.fail(http.cause());
                    }
                });
            })
        ).subscribe(
            () -> {
                log.debug("Service connector started");
            },
            err -> {
                log.error("Error starting service connector", err);
                startPromise.fail(err);
            }
        );
    }

    public void handleWebsocketRequest(SockJSSocket socket) {
        log.debug("new websocket connection");
        ConnectorListener listener = new ConnectorListener(socket, storageManager, this);
    }
}
