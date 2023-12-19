package io.vantiq.utils;

/*
 * <p>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vantiq.svcconnector.SvcConnSvrMessage;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.jackson.DatabindCodec;

import java.io.IOException;

public class SvcConnSvcMsgCodec implements MessageCodec<SvcConnSvrMessage, SvcConnSvrMessage> {

    @Override
    public void encodeToWire(Buffer buffer, SvcConnSvrMessage restMessage) {
        byte[] encoded;
        try {
            encoded = DatabindCodec.mapper().writeValueAsBytes(restMessage);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        buffer.appendInt(encoded.length);
        Buffer buff = Buffer.buffer(encoded);
        buffer.appendBuffer(buff);
    }

    @Override
    public SvcConnSvrMessage decodeFromWire(int pos, Buffer buffer) {
        int length = buffer.getInt(pos);
        pos += 4;
        byte[] encoded = buffer.getBytes(pos, pos + length);
        try {
            return DatabindCodec.mapper().readValue(encoded, SvcConnSvrMessage.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SvcConnSvrMessage transform(SvcConnSvrMessage restMessage) {
        return restMessage;
    }

    @Override
    public String name() {
        return SvcConnSvrMessage.class.getName();
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
