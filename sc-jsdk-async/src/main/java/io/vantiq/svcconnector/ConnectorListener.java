package io.vantiq.svcconnector;

import static io.vantiq.svcconnector.SvcConnSvrMessage.WS_PING;
import static io.vantiq.svcconnector.SvcConnSvrMessage.WS_PONG;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.Flowable;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.Session;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class ConnectorListener {

    private final ObjectMapper mapper = new ObjectMapper();
    VantiqStorageManager storageManager;
    SockJSSocket webSocket;
    Session session;
    Sessionizer sessionizer;
    ConcurrentLinkedDeque<Pair<String, Flowable<?>>> queuedResults = new ConcurrentLinkedDeque<>();

    public ConnectorListener(SockJSSocket webSocket, VantiqStorageManager storageImpl, Sessionizer sessionizer) {
        storageManager = storageImpl;
        this.webSocket = webSocket;
        this.sessionizer = sessionizer;

        Session session = sessionizer.getSessionStore().createSession(SessionCreator.SESSION_TIMEOUT);
        session.put("publishAddress", webSocket.writeHandlerID());
        sessionizer.getSessionCreator().startSession(session, Collections.emptyMap(), null);
        this.session = session;
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
        sessionizer.getSessionCreator().closeSession(session.id());
        sessionizer.getSessionStore().delete(session.id(), avoid -> {
        });
        if (doClose) {
            webSocket.close();
        }
    }

    void onException(Throwable t) {
        log.warn("exception on websocket: ", t);
        cleanup(true);
    }

    void onMessage(Buffer buffer) {
        SvcConnSvrMessage msg;
        if (buffer.length() == WS_PING.getBytes().length && buffer.toString().equals(WS_PING)) {
            log.trace("got a ping, writing a pong");
            webSocket.write(WS_PONG);
            return;
        }
        log.debug("received service connector message: {}", buffer);
        try {
            msg = mapper.readValue(buffer.getBytes(), 0, buffer.length(), SvcConnSvrMessage.class);
        } catch (IOException e) {
            log.warn("Unable to read web socket message: ", e);
            return;
        }
        Flowable<?> result;
        try {
            result = dispatch(msg);
        } catch (Throwable t) {
            result = Flowable.error(t);
        }
        if (webSocket.writeQueueFull()) {
            queuedResults.add(new ImmutablePair<>(msg.requestId, result));
            return;
        }
        pumpResult(msg.requestId, result);
    }

    private void pumpResult(String requestId, Flowable<?> result) {
        SvcConnSvrResponse response = new SvcConnSvrResponse(requestId);
        //noinspection ResultOfMethodCallIgnored
        result.subscribe(
            next -> {
                response.result = next;
                response.isEOF = false;
                response.errorMsg = null;
                writeResponse(response);
            },
            error -> {
                response.result = null;
                response.errorMsg = error.getMessage();
                writeResponse(response);
            },
            () -> {
                response.errorMsg = null;
                response.result = null;
                response.isEOF = true;
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
    
    @SuppressWarnings("unchecked")
    private Flowable<?> dispatch(SvcConnSvrMessage msg) {
        Flowable<?> result;
        if (msg.procName != null) {
            // procedure name is <service name>.<procedure name>
            String[] parts = msg.procName.split("\\.");
            switch (parts.length == 2 ? parts[1] : parts[0]) {
                case "update":
                    result = storageManager.update((String) msg.params.get("storageName"),
                        (Map<String, Object>) msg.params.get("storageManagerReference"),
                        (Map<String, Object>) msg.params.get("values"), (Map<String, Object>) msg.params.get("qual"))
                            .toFlowable();
                    break;
                case "insertMany":
                    result = storageManager.insertMany((String) msg.params.get("storageName"),
                        (Map<String, Object>) msg.params.get("storageManagerReference"),
                        (List<Map<String, Object>>) msg.params.get("values"));
                    break;
                case "insert":
                    result = storageManager.insert((String) msg.params.get("storageName"),
                        (Map<String, Object>) msg.params.get("storageManagerReference"),
                        (Map<String, Object>) msg.params.get("values")).toFlowable();
                    break;
                case "count":
                    result = storageManager.count((String) msg.params.get("storageName"),
                        (Map<String, Object>) msg.params.get("storageManagerReference"),
                        (Map<String, Object>) msg.params.get("qual"), (Map<String, Object>) msg.params.get("options"))
                            .toFlowable();
                    break;
                case "select":
                    result = storageManager.select((String) msg.params.get("storageName"),
                        (Map<String, Object>) msg.params.get("storageManagerReference"),
                        (Map<String, Object>) msg.params.get("properties"), (Map<String, Object>) msg.params.get("qual"),
                        (Map<String, Object>) msg.params.get("options"));
                    break;
                case "selectOne":
                    result = storageManager.selectOne((String) msg.params.get("storageName"),
                        (Map<String, Object>) msg.params.get("storageManagerReference"),
                        (Map<String, Object>) msg.params.get("properties"), (Map<String, Object>) msg.params.get("qual"),
                        (Map<String, Object>) msg.params.get("options"))
                            .toFlowable();
                    break;
                case "delete":
                    result = storageManager.delete((String) msg.params.get("storageName"),
                        (Map<String, Object>) msg.params.get("storageManagerReference"),
                        (Map<String, Object>) msg.params.get("qual"))
                            .toFlowable();
                    break;
                case "getTypeRestrictions":
                    result = storageManager.getTypeRestrictions().toFlowable();
                    break;
                case "initializeTypeDefinition":
                    if (msg.params == null || !(msg.params.get("proposedType") instanceof Map)
                        || !(msg.params.get("proposedType") instanceof Map)) {
                        result = Flowable.error(new Exception("unrecognized storage manager service procedure call: " +
                            msg.procName));
                    } else {
                        //noinspection unchecked
                        result = storageManager.initializeTypeDefinition(
                            (Map<String, Object>) msg.params.get("proposedType"),
                            (Map<String, Object>) msg.params.get("existingType")
                        ).toFlowable();
                    }
                    break;
                default:
                    result = Flowable.error(new Exception("unrecognized storage manager service procedure call: " +
                        msg.procName));
            }
        } else {
            result = Flowable.error(new Exception("no procedure name given in service procedure call"));
        }
        return result;
    }
}
