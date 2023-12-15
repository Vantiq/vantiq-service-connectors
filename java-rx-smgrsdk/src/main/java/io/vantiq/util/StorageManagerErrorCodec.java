package io.vantiq.util;

/*
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.jackson.DatabindCodec;

import java.io.IOException;

public class StorageManagerErrorCodec implements MessageCodec<StorageManagerError, StorageManagerError> {

    @Override
    public void encodeToWire(Buffer buffer, StorageManagerError restMessage) {
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
    public StorageManagerError decodeFromWire(int pos, Buffer buffer) {
        int length = buffer.getInt(pos);
        pos += 4;
        byte[] encoded = buffer.getBytes(pos, pos + length);
        try {
            return DatabindCodec.mapper().readValue(encoded, StorageManagerError.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StorageManagerError transform(StorageManagerError restMessage) {
        return restMessage;
    }

    @Override
    public String name() {
        return StorageManagerError.class.getName();
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
