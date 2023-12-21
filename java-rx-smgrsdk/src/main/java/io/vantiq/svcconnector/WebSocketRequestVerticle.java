package io.vantiq.svcconnector;

import io.reactivex.rxjava3.core.Completable;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.ext.web.Router;
import io.vertx.rxjava3.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.rxjava3.ext.web.handler.sockjs.SockJSSocket;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is the main entry point for the service connector. It is responsible for starting the HTTP server and
 * handling incoming connection requests. Once a connection is established, it hands off the websocket to a 
 * ConnectionListener. Subsequent requests are dispatched over the vertx event bus to the StorageManagerVerticle.
 * <p/>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p/>
 * All rights reserved.
 */
@Slf4j
public class WebSocketRequestVerticle extends AbstractVerticle {

    @Override
    public Completable rxStart() {
        return Completable.defer(() -> {
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
            SockJSHandler sockJSHandler = SockJSHandler.create(vertx, options);
            router.route("/wsock/*").subRouter(sockJSHandler.socketHandler(this::handleWebsocketRequest));
            log.debug("creating http server at port {}", config.obtainPrimaryPort());
            return vertx.createHttpServer().requestHandler(router).listen(config.obtainPrimaryPort()).doOnSuccess(s -> 
                log.info("HTTP server successfully started on port " + config.obtainPrimaryPort())
            ).ignoreElement();
        });
    }

    private void handleWebsocketRequest(SockJSSocket socket) {
        log.debug("new websocket connection");
        @SuppressWarnings("unused")
        ConnectorListener listener = new ConnectorListener(vertx, socket);
    }
}
