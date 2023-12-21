package io.vantiq.svcconnector;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vantiq.utils.StorageManagerError;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.rxjava3.core.AbstractVerticle;
import io.vertx.rxjava3.core.eventbus.MessageConsumer;
import io.vertx.rxjava3.core.eventbus.MessageProducer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * This is the work-horse verticle for the service connector. It takes requests off the hands of the
 * WebSocketRequestVerticle and dispatches the requests to the storage manager.
 * <p/>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p/>
 * All rights reserved.
 */
@Slf4j
public class StorageManagerVerticle extends AbstractVerticle {
    public static final String STORAGE_MANAGER_ADDRESS = "storageManager";
    VantiqStorageManager storageManager;
    private Disposable activeMsgProcessor;

    @Override
    public Completable rxStart() {
        String className = config().getString("storageManagerClassName");
        if (className == null) {
            return Completable.error(new Exception(
                    "storageManager implementation class must be configured when starting the service connector"));
        }

        // now load the class from the class name and instantiate the VantiqStorageManager object
        Class<?> clazz;
        try {
            clazz = Class.forName(className);
            storageManager = (VantiqStorageManager) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            return Completable.error(e);
        }
        MessageConsumer<SvcConnSvrMessage> consumer = vertx.eventBus().consumer(STORAGE_MANAGER_ADDRESS);
        activeMsgProcessor = consumer.toFlowable().flatMap(msg -> {
            String replyAddr = msg.headers().get("REPLY_ADDRESS");
            MessageProducer<Object> mp = vertx.eventBus().sender(replyAddr);
            log.debug("Received storage manager message on event bus");
            return dispatch(msg.body()).map(v -> {
                log.trace("Sending storage manager response on event bus {}", v);
                mp.write(v);
                return v;
            }).doOnError(t -> {
                log.debug("Error processing message", t);
                mp.write(new StorageManagerError("io.vantiq.storage.manager.error", t.getMessage()));
            }).onErrorComplete().doOnComplete(() -> {
                log.debug("Terminating storage manager response for {} on event bus", msg.body().procName);
                mp.deliveryOptions(new DeliveryOptions().addHeader("EOF_HEADER", "true"));
                mp.write(null);
            });
        }).subscribe(
            m -> log.debug("received storage manager request {}", m),
            e -> log.warn("Error processing message", e),
            () -> log.warn("storage manager request processing terminated")
        );
        return consumer.completionHandler().andThen(storageManager.initialize(vertx));
    }

    @Override
    public Completable rxStop() {
        return Completable.fromAction(() -> {
            if (activeMsgProcessor != null) {
                activeMsgProcessor.dispose();
            }
        });
    }

    @SuppressWarnings("unchecked")
    private Flowable<?> dispatch(SvcConnSvrMessage msg) {
        log.debug("In dispatch dispatching the next request msg: {}", msg.procName);
        Flowable<?> result;
        if (msg.procName != null) {
            // procedure name is <package name>.<service name>.<procedure name>
            String[] parts = msg.procName.split("\\.");
            if (parts.length < 2) {
                throw new IllegalArgumentException("unrecognized storage manager service procedure call: "
                        + msg.procName);
            }
            String procName = parts[parts.length-1];
            switch (procName) {
                case "update":
                    result = storageManager.update((String) msg.params.get("storageName"),
                                    (Map<String, Object>) msg.params.get("storageManagerReference"),
                                    (Map<String, Object>) msg.params.get("values"), (Map<String, Object>) msg.params.get("qual"),
                                    (Map<String, Object>) msg.params.get("options"))
                            .toFlowable();
                    break;
                case "insertMany":
                    result = storageManager.insertMany((String) msg.params.get("storageName"),
                            (Map<String, Object>) msg.params.get("storageManagerReference"),
                            (List<Map<String, Object>>) msg.params.get("values"),
                            (Map<String, Object>)msg.params.get("options"));
                    break;
                case "insert":
                    result = storageManager.insert((String) msg.params.get("storageName"),
                            (Map<String, Object>) msg.params.get("storageManagerReference"),
                            (Map<String, Object>) msg.params.get("values"),
                            (Map<String, Object>) msg.params.get("options")).toFlowable();
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
                case "aggregate":
                    result = storageManager.aggregate((String) msg.params.get("storageName"),
                            (Map<String, Object>) msg.params.get("storageManagerReference"),
                            (List<Map<String, Object>>) msg.params.get("pipeline"),
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
                                    (Map<String, Object>) msg.params.get("qual"), (Map<String, Object>) msg.params.get("options"))
                            .toFlowable();
                    break;
                case "getTypeRestrictions":
                    result = storageManager.getTypeRestrictions().toFlowable();
                    break;
                case "initializeTypeDefinition":
                    if (msg.params == null || !(msg.params.get("proposedType") instanceof Map)) {
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
                case "typeDefinitionDeleted":
                    if (msg.params == null || !(msg.params.get("type") instanceof Map)) {
                        result = Flowable.error(new Exception("invalid parameters for storage manager service procedure call: " +
                                msg.procName));
                    } else {
                        //noinspection unchecked
                        result = storageManager.typeDefinitionDeleted(
                                (Map<String, Object>) msg.params.get("type"),
                                (Map<String, Object>) msg.params.get("options")
                        ).toFlowable();
                    }
                    break;
                case "startTransaction":
                    result = storageManager.startTransaction(
                            (String)msg.params.get("vantiqTransactionId"),
                            (Map<String, Object>) msg.params.get("options")
                    ).toFlowable();
                    break;
                case "commitTransaction":
                    result = storageManager.commitTransaction(
                            (String)msg.params.get("vantiqTransactionId"),
                            (Map<String, Object>) msg.params.get("options")
                    ).toFlowable();
                    break;
                case "abortTransaction":
                    result = storageManager.abortTransaction(
                            (String)msg.params.get("vantiqTransactionId"),
                            (Map<String, Object>) msg.params.get("options")
                    ).toFlowable();
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
