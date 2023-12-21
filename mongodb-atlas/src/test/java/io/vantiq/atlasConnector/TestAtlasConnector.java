package io.vantiq.atlasConnector;

import static org.junit.jupiter.api.Assertions.fail;

import io.vantiq.svcconnector.InstanceConfigUtils;
import io.vantiq.svcconnector.StorageManagerVerticle;
import io.vantiq.svcconnector.SvcConnSvrMessage;
import io.vantiq.svcconnector.WebSocketRequestVerticle;
import io.vantiq.utils.SocketSession;
import io.vantiq.utils.StorageManagerError;
import io.vantiq.utils.StorageManagerErrorCodec;
import io.vantiq.utils.SvcConnSvcMsgCodec;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * run through some basic atlas connector tests
 * <p/>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p/>
 * All rights reserved.
 */
@Slf4j
@ExtendWith(VertxExtension.class)
public class TestAtlasConnector {
    
    private SocketSession session;

    /*
     * Deploy a single instance of the request processing verticles for the AtlasStorageManager
     */
    @BeforeAll
    static void deployVerticles(Vertx vertx, VertxTestContext testContext) {
        InstanceConfigUtils config = new InstanceConfigUtils();
        Properties secrets = config.loadServerSecrets();
        if (secrets.getProperty("secret") == null) {
            // skip the tests if the property is set
            testContext.failNow("no credentials to authenticate to Atlas");
            return;
        }
        // define a couple of codecs that the verticles need
        vertx.eventBus().registerDefaultCodec(SvcConnSvrMessage.class, new SvcConnSvcMsgCodec());
        vertx.eventBus().registerDefaultCodec(StorageManagerError.class, new StorageManagerErrorCodec());
        
        JsonObject verticleConfig = new JsonObject().put("storageManagerClassName",
                "io.vantiq.atlasConnector.AtlasStorageMgr");
        DeploymentOptions deployOptions = new DeploymentOptions()
                .setInstances(1)
                .setConfig(verticleConfig);
        vertx.deployVerticle(WebSocketRequestVerticle::new, deployOptions, ar -> {
            if (!ar.succeeded()) {
                testContext.failNow(ar.cause());
            }
            vertx.deployVerticle(StorageManagerVerticle::new, deployOptions, ar2 -> {
                if (!ar2.succeeded()) {
                    testContext.failNow(ar.cause());
                }
                testContext.completeNow();
            });
        });
    }

    @BeforeEach
    void setup(Vertx vertx, VertxTestContext vtc) {
        session = new SocketSession(t -> {
            vtc.failNow(t);
            fail("Unexpected error encountered in test", t);
        });
        session.connect(vertx);
        vtc.completeNow();
    }
    @AfterEach
    void cleanup() {
        // remove TestType collection created during tests
        Map<String, Object> oldType = createTypeDefinition("TestType");
        oldType.put("storageName", "cluster0:TestType");
        session.typeDefinitionDeleted(oldType);
        session.disconnect();
    }

    private Map<String, Object> createTypeDefinition(@SuppressWarnings("SameParameterValue") String name) {
        return createTypeDefinition(name, null);
    }
    
    @SuppressWarnings("SameParameterValue")
    private Map<String, Object> createTypeDefinition(String name, String storageName) {
        // build up a type definition (I miss groovy)
        val type = new HashMap<>(Map.of(
            "name", name,
            "properties", Map.of(
                "name", Map.of("type", "String", "required", true),
                "address", Map.of("type", "String"),
                "amount", Map.of("type", "Integer")
            ),
            "indexes", List.of(
                Map.of(
                    "keys", List.of("name"),
                    "options", Map.of("unique", true))
            )
        ));
        if (storageName != null) {
            type.put("storageName", storageName);
        }
        return type;
    }
    
    @SuppressWarnings("unused")
    @Test
    void testInvocations() {
        val restricts = session.getTypeRestrictions();
        // should get a restrictedTypeProperties for the getTypeRestrictions call
        assert restricts.get("restrictedTypeProperties") != null;
        assert restricts.get("restrictedTypeProperties") instanceof List;
        //noinspection unchecked
        List<String> restrictedTypeProperties = (List<String>)restricts.get("restrictedTypeProperties");
        assert restrictedTypeProperties.size() == 1 && restrictedTypeProperties.get(0).equals("expiresAfter");

        val type = session.initializeTypeDefinition(createTypeDefinition("TestType"));
        // should get an empty map for type restrictions
        assert type.get("storageName").equals("cluster0:TestType");

        var result = session.insert("cluster0:TestType", Map.of("name", "Bart Simpson", "address", "123 Main St", "amount", 100));
        // should get an empty map for type restrictions
        assert result.get("name").equals("Bart Simpson") ;
        assert result.get("address").equals("123 Main St");
        assert result.get("amount").equals(100);
        
        result = session.selectOne("cluster0:TestType", List.of("name", "amount"), Map.of("name", "Bart Simpson"));
        // should get our one result with no address property
        assert result.get("name").equals("Bart Simpson");
        assert result.get("address") == null;
        assert result.get("amount").equals(100);

        session.insert("cluster0:TestType", Map.of("name", "Lisa Simpson", "address", "123 Main St", "amount", 200));
        var results = session.select("cluster0:TestType", List.of("name", "amount"), Map.of());
        // should 2 results, one for Bart and one for Lisa
        assert results.size() == 2;
        assert results.stream().anyMatch(m -> m.get("name").equals("Bart Simpson"));
        assert results.stream().anyMatch(m -> m.get("name").equals("Lisa Simpson"));
        assert results.stream().allMatch(m -> m.get("address") == null);
        
        List<Map<String, Object>> instances = new ArrayList<>();
        instances.add(Map.of("name", "Homer J. Simpson", "address", "Springfield", "amount", 300));
        instances.add(Map.of("name", "Marge Simpson", "address", "Springfield", "amount", 400));
        results = session.insertMany("cluster0:TestType", instances);
        assert results.size() == 2;

        results = session.select("cluster0:TestType", List.of("name", "amount"), Map.of());
        // should 2 results, one for Bart and one for Lisa
        assert results.size() == 4;
        assert results.stream().anyMatch(m -> m.get("name").equals("Bart Simpson"));
        assert results.stream().anyMatch(m -> m.get("name").equals("Lisa Simpson"));
        assert results.stream().anyMatch(m -> m.get("name").equals("Homer J. Simpson"));
        assert results.stream().anyMatch(m -> m.get("name").equals("Marge Simpson"));
        
        result = session.update("cluster0:TestType", Map.of("name", "Bart Simpson"), Map.of("amount", 150));
        assert result.get("amount") instanceof Number;
        assert result.get("amount").equals(150);
        
        var count = session.delete("cluster0:TestType", Map.of("name", "Homer J. Simpson"));
        assert count == 1;
        
        count = session.count("cluster0:TestType", Map.of("name", Map.of("$ne", "Homer J. Simpson")));
        assert count == 3;
        
        List<Map<String, Object>> pipeline = List.of(
            Map.of("$group",
                Map.of(
                    "_id", "null",
                    "total", Map.of("$sum", "$amount")
                )
            )
        );
        var aggResults = session.aggregate("cluster0:TestType", pipeline);
        assert aggResults.size() == 1;
        assert aggResults.get(0).get("total").equals(750);
    }
}
