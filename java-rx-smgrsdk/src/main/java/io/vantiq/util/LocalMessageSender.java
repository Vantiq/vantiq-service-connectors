package io.vantiq.util;
/*
 * <p>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */

import io.reactivex.rxjava3.core.Flowable;
import io.vantiq.svcconnector.SvcConnSvrMessage;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.rxjava3.core.Vertx;
import io.vertx.rxjava3.core.eventbus.Message;
import io.vertx.rxjava3.core.eventbus.MessageConsumer;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;

import java.util.UUID;
import java.util.function.Function;

/**
 * Delegate object used to perform sending of an event bus message and observation of the subsequent reply. Only
 * supports sending/receiving via local EB addresses.
 *
 * <p>
 * Created by jmeredith
 */
@RequiredArgsConstructor
public class LocalMessageSender {
    private final Vertx vertx;
    private final Logger log;

    public <T> Flowable<T> sendMessage(String address, SvcConnSvrMessage msg, Function<Message<?>, T> decodeReply) {
        // Wrap this whole thing in a defer so the caller can control when the work occurs
        return Flowable.defer(() -> {
            // Create a consumer to receive the response from the message we will be sending
            String clientAddr = UUID.randomUUID() + "-reply";
            MessageConsumer<Object> consumer = vertx.eventBus().localConsumer(clientAddr);
            // Chain in the send after the consumer is fully registered and before processing the Flowable. This
            // ensures that we won't miss anything.
            return consumer.toFlowable().doOnSubscribe(s -> {
                log.trace("{} -- sending message", consumer.address());
                DeliveryOptions options = new DeliveryOptions()
                        .setLocalOnly(true)
                        .addHeader("REPLY_ADDRESS", consumer.address());
                
                vertx.eventBus().send(address, msg, options);
            }).takeWhile(reply -> {
                // Keep reading data until we're told there is no more.
                String eofHeader = reply.headers().get("EOF_HEADER");
                return eofHeader == null || eofHeader.compareToIgnoreCase("true") != 0;
            }).map(decodeReply::apply).doFinally(() -> {
                log.trace("{} -- unregister client", consumer.address());
                consumer.unregister();
            });
        });
    }
}

