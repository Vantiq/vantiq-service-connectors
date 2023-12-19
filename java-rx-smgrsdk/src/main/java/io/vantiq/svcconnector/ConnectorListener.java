package io.vantiq.svcconnector;

import static io.vantiq.svcconnector.StorageManagerVerticle.STORAGE_MANAGER_ADDRESS;
import static io.vantiq.svcconnector.SvcConnSvrMessage.WS_PING;
import static io.vantiq.svcconnector.SvcConnSvrMessage.WS_PONG;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.Flowable;
import io.vantiq.utils.LocalMessageSender;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.buffer.Buffer;
import io.vertx.rxjava3.core.eventbus.Message;
import io.vertx.rxjava3.ext.web.handler.sockjs.SockJSSocket;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * This class is responsible for listening to the web socket and handling messages from the Vantiq server. It is
 * instantiated by the main verticle when a new web socket connection is made. It dispatches requests to the class
 * implementing the storage manager interface and sends the results back to the Vantiq server.
 * <p>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */
@Slf4j
public class ConnectorListener {

    private final ObjectMapper mapper = new ObjectMapper();
    LocalMessageSender dispatcher;
    SockJSSocket webSocket;
    Vertx vertx;
    ConcurrentLinkedDeque<Pair<String, Flowable<?>>> queuedResults = new ConcurrentLinkedDeque<>();

    public ConnectorListener(Vertx vertx, SockJSSocket webSocket) {
        this.webSocket = webSocket;
        this.vertx = vertx;
        dispatcher = new LocalMessageSender(vertx, log);
        webSocket.handler(this::onMessage);
        webSocket.exceptionHandler(this::onException);
        webSocket.closeHandler(aVoid -> {
            log.debug("web socket closed");
            // this is a close handler, don't call close again
            cleanup(false);
        });
        webSocket.drainHandler(this::drainQ);
    }

    void drainQ(Void aVoid) {
        Pair<String, Flowable<?>> resultPair;
        while (!webSocket.writeQueueFull() && (resultPair = queuedResults.poll()) != null) {
            pumpResult(resultPair.getLeft(), resultPair.getRight());
        }
    }

    void cleanup(boolean doClose) {
        if (doClose) {
            webSocket.close();
        }
    }

    void onException(Throwable t) {
        log.warn("exception on websocket: ", t);
        cleanup(true);
    }

    void onMessage(Buffer buffer) {
        SvcConnSvrMessage clientMsg;
        if (buffer.length() == WS_PING.getBytes().length && buffer.toString().equals(WS_PING)) {
            log.trace("got a ping, writing a pong");
            webSocket.write(WS_PONG);
            return;
        }
        log.debug("received service connector message: {}", buffer);
        try {
            clientMsg = mapper.readValue(buffer.getBytes(), 0, buffer.length(), SvcConnSvrMessage.class);
        } catch (IOException e) {
            log.warn("Unable to read web socket message: ", e);
            return;
        }
        Flowable<?> result = dispatcher.sendMessage(STORAGE_MANAGER_ADDRESS, clientMsg, Message::body);
        if (webSocket.writeQueueFull()) {
            queuedResults.add(new ImmutablePair<>(clientMsg.requestId, result));
            return;
        }
        pumpResult(clientMsg.requestId, result);
    }

    private void pumpResult(String requestId, Flowable<?> result) {
        //noinspection ResultOfMethodCallIgnored
        result.subscribe(
            next -> {
                SvcConnSvrResponse response = new SvcConnSvrResponse(requestId);
                response.result = next;
                response.isEOF = false;
                response.errorMsg = null;
                log.trace("In pumpResult, writing response {}", response);
                writeResponse(response);
            },
            error -> {
                SvcConnSvrResponse response = new SvcConnSvrResponse(requestId);
                response.result = null;
                response.errorMsg = error.getMessage() == null ? error.toString() : error.getMessage();
                log.trace("In pumpResult, writing error: {}", response.errorMsg);
                writeResponse(response);
            },
            () -> {
                SvcConnSvrResponse response = new SvcConnSvrResponse(requestId);
                response.errorMsg = null;
                response.result = null;
                writeResponse(response);
            }
        );
    }

    private void writeResponse(SvcConnSvrResponse response) {
        Buffer b;
        try {
            b = Buffer.buffer(mapper.writeValueAsBytes(response));
            log.debug("writing reply {}", b);
        } catch (JsonProcessingException e) {
            log.error("Error encountered serializing service connector result: ", e);
            return;
        }
        webSocket.write(b);
    }
}
