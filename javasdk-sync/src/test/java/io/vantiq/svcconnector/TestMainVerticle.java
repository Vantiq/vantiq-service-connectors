package io.vantiq.svcconnector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.UUID;

@ExtendWith(VertxExtension.class)
public class TestMainVerticle {

  private ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void deploy_verticle(Vertx vertx, VertxTestContext testContext) {
      vertx.deployVerticle(new MainVerticle(), testContext.succeeding(id -> testContext.completeNow()));
    }

    @Test
    void verticle_deployed(Vertx vertx, VertxTestContext testContext) throws Throwable {
      vertx.createHttpClient().webSocket(8888, "localhost", "/wsock/websocket", ar -> {
        if (ar.succeeded()) {
          ar.result().handler(buffer -> {
            System.out.println("received message from server: " + buffer.toString());
            if (buffer.toString().equals("pong")) {
              testContext.completeNow();
            }
          });
          SvcConnSvrMessage msg = new SvcConnSvrMessage();
          msg.requestId = UUID.randomUUID().toString();
          try {
            ar.result().write(Buffer.buffer(mapper.writeValueAsBytes(msg)), wrAr -> {
              if (wrAr.failed()) {
                testContext.failNow(wrAr.cause());
              }
            });
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        } else {
          testContext.failNow(ar.cause());
        }
      });
    }
}
