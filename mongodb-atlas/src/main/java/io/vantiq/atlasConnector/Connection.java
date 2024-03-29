package io.vantiq.atlasConnector;

import static io.vantiq.svcconnector.InstanceConfigUtils.MONGO_AUTH_SOURCE_PROP;
import static io.vantiq.svcconnector.InstanceConfigUtils.MONGO_CONN_PROTOCOL_PROP;
import static io.vantiq.svcconnector.InstanceConfigUtils.MONGO_DEFAULT_DB_PROP;
import static io.vantiq.svcconnector.InstanceConfigUtils.MONGO_HOST_PROP;
import static io.vantiq.svcconnector.InstanceConfigUtils.MONGO_SERVER_API_PROP;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vantiq.svcconnector.InstanceConfigUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * This class (re)establishes connections to MongoDB Atlas cloud services
 * <p>
 * Initially it works by using username / password credentials, but can be extended to utilize
 * MongoDB x509 certificate based authentication.
 * <p>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p>
 * All rights reserved.
 */
@Slf4j
public class Connection {
    volatile Single<MongoClient> clientObservable;
    
    public static void main(String[] args) {
        Connection conn = new Connection();
        final CountDownLatch latch = new CountDownLatch(1);
        Disposable d = conn.connect(new InstanceConfigUtils()).subscribe(
            doc -> { System.out.println("Successfully connected to MongoDB Atlas"); latch.countDown(); },
            t -> System.out.println("Failed to connect to MongoDB Atlas: " + t.getMessage())
        );
        try {
            latch.await();
            d.dispose();
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for connection to MongoDB Atlas", e);
            System.exit(1);
        }
        conn.close().subscribe();
        System.exit(0);
    }
    
    public Completable close() {
        Single<MongoClient> oldClientObs;
        synchronized(this) {
            oldClientObs = clientObservable;
            clientObservable = null;
        }
        if (oldClientObs != null) {
            return oldClientObs.doOnSuccess(MongoClient::close).ignoreElement();
        }
        return Completable.complete();
    }
    
    public Single<MongoClient> connect(InstanceConfigUtils config) {
        if (clientObservable == null) {
            Properties properties = config.loadServerConfig();
            Properties secrets = config.loadServerSecrets();
            if (secrets.getProperty("secret") == null) {
                return Single.error(new RuntimeException(
                        "secrets.properties lacks credentials to authenticate to MongoDB"));
            }
            
            String connectionString = properties.getProperty(MONGO_CONN_PROTOCOL_PROP, "mongodb+srv") + "://" +
                    secrets.getProperty("secret") + "@" + 
                    properties.getProperty(MONGO_HOST_PROP, "cluster0.h7jzx3i.mongodb.net");
            if (properties.containsKey(MONGO_DEFAULT_DB_PROP)) {
                connectionString += "/" + properties.getProperty(MONGO_DEFAULT_DB_PROP) + "?retryWrites=true&w=1";
            } else {
                connectionString += "/?retryWrites=true&w=1";
            }
            if (properties.containsKey(MONGO_AUTH_SOURCE_PROP)) {
                connectionString += "&authSource=" + properties.getProperty(MONGO_AUTH_SOURCE_PROP);
            }

            log.debug("connecting to mongoDB with connection string: {}",
                    connectionString.replace(secrets.getProperty("secret"), "********"));

            MongoClientSettings.Builder builder = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(connectionString));
            
            if (properties.containsKey(MONGO_SERVER_API_PROP)) {
                ServerApi serverApi = ServerApi.builder()
                        .version(ServerApiVersion.findByValue(properties.getProperty(MONGO_SERVER_API_PROP)))
                        .build();
                builder.serverApi(serverApi);
            }
            MongoClientSettings settings = builder.build();

            synchronized (this) {
                if (clientObservable == null) {
                    clientObservable = Single.fromSupplier(() -> MongoClients.create(settings)).cache();
                }
            }
            // Create a new client and connect to the server
        }
        return clientObservable;
    }
}
