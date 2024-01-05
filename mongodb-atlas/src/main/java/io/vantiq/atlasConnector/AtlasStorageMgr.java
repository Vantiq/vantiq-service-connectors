package io.vantiq.atlasConnector;

import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.include;

import com.google.common.cache.Cache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.Lists;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.vantiq.svcconnector.InstanceConfigUtils;
import io.vantiq.svcconnector.VantiqStorageManager;
import io.vantiq.utils.CacheIfNotEmptyOrError;
import io.vantiq.utils.VertxWideData;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava3.ContextScheduler;
import io.vertx.rxjava3.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Storage manager for MongoDB Atlas. This storage manager is designed to work with MongoDB Atlas clusters. It
 * supports all the storage manager API calls.
 * <p/>
 * Copyright (c) 2023 Vantiq, Inc.
 * <p/>
 * All rights reserved.
 */
@Slf4j
public class AtlasStorageMgr implements VantiqStorageManager {
    static final long QUERY_TIMEOUT = 50L;
    static final TimeUnit QUERY_TIMEOUT_TIMEUNIT = TimeUnit.SECONDS;
    
    Connection connection;
    InstanceConfigUtils config = new InstanceConfigUtils();
    Cache<String, Single<ImmutablePair<MongoDatabase, ClientSession>>> dbSessions;
    Cache<String, Single<ImmutablePair<MongoClient, ClientSession>>> transactions;
    volatile Scheduler rxScheduler = null;
    
    @Override
    public Completable initialize(Vertx vertx) {
        rxScheduler = new ContextScheduler(vertx.getDelegate().getOrCreateContext(), false);
        RemovalListener<String, Single<ImmutablePair<MongoDatabase,ClientSession>>> sessDbRemovalListener =
                notification -> {
            Single<ImmutablePair<MongoDatabase, ClientSession>> value = notification.getValue();
            if (notification.wasEvicted() && value != null) {
                value.doOnSuccess(sessionDb -> sessionDb.getRight().close()).subscribe();
            }
        };
        dbSessions = VertxWideData.getVertxWideCache(vertx, this.getClass().getName() + ".sessions",
                "expireAfterAccess=30m", null, sessDbRemovalListener, null);
        
        RemovalListener<String, Single<ImmutablePair<MongoClient, ClientSession>>> sessionRemovalListener = notification -> {
            Single<ImmutablePair<MongoClient, ClientSession>> value = notification.getValue();
            if (notification.wasEvicted() && value != null) {
                value.doOnSuccess(cliSess -> cliSess.getRight().close()).subscribe();
            }
        };
        transactions = VertxWideData.getVertxWideCache(vertx, this.getClass().getName() + ".transactions",
                "expireAfterAccess=15m", null, sessionRemovalListener, null);
        
        config.loadServerConfig();
        connection = new Connection();
        // get a connection and run a ping command
        return connection.connect(config).flatMapPublisher(client -> {
            MongoDatabase adminDb = client.getDatabase("admin");
            AtomicLong start = new AtomicLong();
            return Flowable.fromPublisher(adminDb.runCommand(new Document("ping", 1))).doOnSubscribe(s ->
                start.set(System.currentTimeMillis())
            ).doOnComplete(() ->
                log.debug("Connected to MongoDB, ping time: {}ms", System.currentTimeMillis() - start.get())
            ).doOnError(t -> 
                log.error("Failed to connect to MongoDB", t)
            );
        }).ignoreElements();
    }

    /**
     * Implement the storage manager API getTypeRestrictions -- the only thing we don't allow is the expiresAfter
     * property.
     */
    @Override
    public Single<Map<String, Object>> getTypeRestrictions() {
        // we support almost everything the VANTIQ type system supports. we restrict expiresAfter since we don't keep
        // a createdAt property in the collection by default. Could revisit 
        Map<String, Object> restrictions = new HashMap<>();
        restrictions.put("restrictedTypeProperties", Lists.newArrayList("expiresAfter"));
        return Single.just(restrictions);
    }

    /**
     * initialize the type definition by creating the collection and indexes in Atlas
     */
    @Override
    public Single<Map<String, Object>> initializeTypeDefinition(Map<String, Object> proposedType, Map<String,
            Object> existingType) {
        boolean capped = false;
        int cappedSize = proposedType.containsKey("maxStorage") ? (Integer)proposedType.get("maxStorage") : 0;
        int maxDocs = 0;
        if (cappedSize > 0) {
            if (proposedType.containsKey("maxObjects")) {
                maxDocs = (Integer)proposedType.get("maxObjects");
            }
            capped = true;
        }
        // puzzle out the database and collection names
        String dbName = config.obtainDefaultDatabase();
        Map<String, Object> type = existingType != null ? existingType : proposedType;
        String collectionName = (String)type.get("name");
        if (type.containsKey("storageName")&& type.get("storageName") instanceof String) {
            String[] parts = ((String)type.get("storageName")).split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "MongoDB Atlas type storage name must have the format: <database>:<collection>");
            }
            dbName = parts[0];
            collectionName = parts[1];
        }
        if (dbName == null) {
            throw new IllegalStateException(
                    "No storage name specified for MongoDB Atlas collection and no default database was configured");
        }
        String finalName = collectionName;
        String finalDbName = dbName;
        if (existingType == null) {
            CreateCollectionOptions createOptions = new CreateCollectionOptions()
                    .capped(capped)
                    .sizeInBytes(cappedSize)
                    .maxDocuments(maxDocs);
            return usingSession(finalDbName, null, (db, sess) ->
                    db.createCollection(sess, finalName, createOptions)).ignoreElements().andThen(
                proposedType.containsKey("indexes") ?
                    this.usingSession(finalDbName, null, (db, sess) -> {
                        MongoCollection<Document> collection = db.getCollection((String) proposedType.get("name"));
                        //noinspection unchecked
                        return Flowable.fromIterable((List<Map<String, Object>>) proposedType.get("indexes")).flatMap(index ->
                            createIndex(collection, sess, index)
                        );
                    }).ignoreElements() : Completable.complete()
            ).doOnComplete(() -> {
                // track the database for collection in the storage name of the proposed type. use : as a separator
                // as it is not a valid character in a collection name or vantiq type name
                proposedType.put("storageName", buildStorageName(finalDbName, finalName));
                
                // set up the _id property in the proposed type
                //noinspection unchecked
                Map<String, Object> properties = (Map<String, Object>) proposedType.get("properties");
                properties.put("_id", createIdProperty());
            }).onErrorResumeNext(t -> {
                // if the collection already exists, we're good
                if (t.getMessage().contains("already exists")) {
                    proposedType.put("storageName", buildStorageName(finalDbName, finalName));
                    return Completable.complete();
                } else {
                    return Completable.error(t);
                }
            }).andThen(Single.just(proposedType));
        } else {
            // check if the definition of indexes has changed
            List<Map<String, Object>> delete = new ArrayList<>();
            List<Map<String, Object>> add = new ArrayList<>();
            analyzeIndexes(existingType, proposedType, delete, add);
            return usingSession(finalDbName, null, (db, sess) -> {
                MongoCollection<Document> collection = db.getCollection(finalName);
                Publisher<Document> sizeChange = Flowable.empty();
                if (proposedType.containsKey("maxStorage") && proposedType.get("maxStorage") != existingType.get("maxStorage")) {
                    // change the size of the collection, currently hit permission problems, but it should work
                    sizeChange = db.runCommand(new Document("collMod", finalName)
                            .append("cappedSize", proposedType.get("maxStorage")));
                }
                return Flowable.concat(
                    sizeChange,
                    // delete indexes that are no longer in the proposed type
                    Flowable.fromIterable(delete).flatMap(index -> collection.dropIndex(sess, computeIndexKeySet(index))),
                    // add indexes that are in the proposed type but not in the existing type
                    Flowable.fromIterable(add).flatMap(addIndex -> createIndex(collection, sess, addIndex))
                );
            }).ignoreElements().andThen(Single.just(proposedType));
        }
    }

    /**
     * On type deletion we remove the collection from Atlas. Beware if you are connecting a vantiq type to a pre-existing
     * collection in Atlas, it will be deleted when the type is deleted.
     * 
     * @param type the type defn
     * @param options unused at present
     */
    @Override
    public Completable typeDefinitionDeleted(Map<String, Object> type, Map<String, Object> options) {
        return collectionFromStorageName((String)type.get("storageName"), options, MongoCollection::drop)
                .ignoreElements();
    }

    private Publisher<String> createIndex(MongoCollection<Document> collection, ClientSession session,
                                          Map<String, Object> index) {
        //noinspection unchecked
        Map<String,Object> options = (Map<String,Object>)index.get("options");
        if (options == null) {
            options = Collections.emptyMap();
        }
        boolean unique = options.containsKey("unique") && (boolean)options.get("unique");
        // build in the background when the index is not unique and index options do not
        // explicitly specify background = false (i.e. not specified or set to true)
        boolean background = !unique && (options.containsKey("background")&&
                (boolean) options.get("background"));
        IndexOptions idxOptions = new IndexOptions()
                .background(background)
                .unique(unique)
                .name((String)options.get("name"));
        if (options.containsKey("expireAfterSeconds")) {
            idxOptions.expireAfter((Long)options.get("expireAfterSeconds"), TimeUnit.SECONDS);
        }
        return collection.createIndex(session, computeIndexKeySet(index), idxOptions);
    }
    
    private static Map<String, Object> createIdProperty() {
        Map<String, Object> idProperty = new HashMap<>();
        idProperty.put("type", "String");
        idProperty.put("system", true);
        idProperty.put("required", false);
        idProperty.put("indexed", true);
        idProperty.put("unique", false);
        idProperty.put("multi", false);
        return idProperty;
    }

    static Document computeIndexKeySet(Map<String, Object> index) {
        // convert to the mongo form of the key structure
        Document keySet = new Document();
        //noinspection unchecked
        ((List<String>)index.get("keys")).forEach(k -> {
            if (k.startsWith("-")) {
                keySet.put(k.substring(1), -1);
            } else if (k.startsWith("+")) {
                keySet.put(k.substring(1), 1);
            } else if (k.startsWith("##")) {
                keySet.put(k.substring(2), "2d");
            } else if (k.startsWith("#")) {
                keySet.put(k.substring(1), "2dsphere");
            } else {
                keySet.put(k, 1);
            }
        });
        return keySet;
    }
    
    /**
     * Analyze the indexes in the existing and proposed types and add the indexes to the delete and add lists as
     * appropriate
     * @param existingType type prior to update
     * @param proposedType type after update
     * @param deleteList indexes to delete
     * @param addList indexes to add
     */
    @SuppressWarnings("unchecked")
    private void analyzeIndexes(Map<String, Object> existingType, Map<String, Object> proposedType,
                                List<Map<String,Object>> deleteList, List<Map<String, Object>> addList) {
        // if the index is not in the proposed type, add it to the delete list
        if (existingType.get("indexes") != null) {
            ((List<Map<String, Object>>) existingType.get("indexes")).stream().filter(index -> {
                // check if the index is in the proposed type
                return ((List<Map<String, Object>>) proposedType.get("indexes")).stream().noneMatch(proposedIndex ->
                        proposedIndex.get("keys").equals(index.get("keys"))
                );
            }).forEach(deleteList::add);
        }
        
        // if the index is not in the existing type, add it to the add list
        if (proposedType.get("indexes") != null) {
            ((List<Map<String, Object>>) proposedType.get("indexes")).stream().filter(index -> {
                // check if the index is in the existing type
                return ((List<Map<String, Object>>) existingType.get("indexes")).stream().noneMatch(existingIndex ->
                        existingIndex.get("keys").equals(index.get("keys"))
                );
            }).forEach(addList::add);
        }
    }
    
    @Override
    public Single<Map<String, Object>> insert(String storageName, Map<String, Object> storageManagerReference,
                                              Map<String, Object> values, Map<String, Object> options) {
        Document doc = new Document(values);
        if (doc.containsKey("_id") && !(doc.get("_id") instanceof ObjectId)) {
            if (!(doc.get("_id") instanceof String)) {
                return Single.error(new Exception("Found illegal value " + doc.get("_id") + " for ''_id''. Expecting " +
                        "either an ObjectId or the string representation of an ObjectId."));
            }
            ObjectId id;
            try {
                id = new ObjectId(doc.getString("_id"));
            } catch (IllegalArgumentException ignored) {
                return Single.error(new Exception("Found illegal value " + doc.get("_id") + " for ''_id''. Expecting " +
                        "either an ObjectId or the string representation of an ObjectId."));
            }
            doc.append("_id", id);
        }
        return collectionFromStorageName(storageName, options, (collection, session) ->
                collection.insertOne(session, doc)).firstOrError().map(insertOne -> externalizeId(doc));
    }

    static Map<String, Object> externalizeId(Map<String, Object> obj) {
        if (obj.containsKey("_id")) {
            obj.put("_id", obj.get("_id").toString());
        }
        return obj;
    }
    
    @Override
    public Flowable<Map<String, Object>>
    insertMany(String storageName, Map<String, Object> storageManagerReference, List<Map<String, Object>> values,
               Map<String, Object> options) {
        if (values.isEmpty()) {
            return Flowable.empty();
        }
        List<Document> docs = new ArrayList<>();
        values.forEach(value -> docs.add(new Document(value).append("_id", new ObjectId())));

        return collectionFromStorageName(storageName, options, (collection, session) -> {
            InsertManyOptions insertOpts = new InsertManyOptions().ordered(false);
            return collection.insertMany(session, docs, insertOpts);
        }).flatMap(result -> Flowable.fromIterable(docs))
          .map(AtlasStorageMgr::externalizeId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Maybe<Map<String, Object>>
    update(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> values,
           Map<String, Object> qual, Map<String, Object> options) {
        
        Map<String, Object> unsetVals = values.containsKey("$unset") ?
                (Map<String, Object>)values.remove("$unset") : new HashMap<>();
        Map<String, Object> setVals = values.containsKey("$set") ?
                (Map<String, Object>)values.remove("$set") : values;

        return collectionFromStorageName(storageName, options, (collection, session) -> {
            Document cleanQual = new Document((Map<String, Object>)cleanQualMap(qual));
            Document updateVals = new Document();
            if (!setVals.isEmpty()) {
                if (setVals.containsKey("$rename")) {
                    updateVals.put("$rename", setVals.get("$rename"));
                } else {
                    updateVals.put("$set", setVals);
                }
            }
            if (!unsetVals.isEmpty()) {
                updateVals.put("$unset", unsetVals);
            }
            UpdateOptions updateOptions = new UpdateOptions().upsert(false);
            return collection.updateMany(session, cleanQual, updateVals, updateOptions);
        }).singleElement().flatMap(updateResult -> {
            if (updateResult.getModifiedCount() == 0) {
                return Maybe.empty();
            }
            // not entirely sure what to return here... don't want to issue a find over the qual to get the updated
            // data.
            return Maybe.just(setVals);
        });
    }

    @Override
    public Single<Integer>
    count(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual,
          Map<String, Object> options) {
        if (options == null) {
            options = Collections.emptyMap();
        }
        CountOptions countOptions = new CountOptions().maxTime(QUERY_TIMEOUT, QUERY_TIMEOUT_TIMEUNIT);
        if (options.containsKey("limit")) {
            countOptions.limit((int)options.get("limit"));
        }
        if (options.containsKey("skip")) {
            countOptions.skip((int)options.get("skip"));
        }
        return collectionFromStorageName(storageName, options, (collection, session) -> {
            //noinspection unchecked
            Document query = new Document((Map<String, Object>)cleanQualMap(qual));
            return collection.countDocuments(session, query, countOptions);
        }).map(Math::toIntExact).firstOrError();
    }

    /**
     * Clean up the qual map prior to submitting to mongodb to handle the special case of the _id field.
     * If the _id field is a String, convert it to an ObjectId. Also, BigDecimal converts to Double.
     * <br/><br/>
     * Note quals / subquals can be maps or lists, so we need to recurse and the return type of this method is Object.
     * @param qual the qualification to clean up
     * @return the cleaned qual / subqual
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object cleanQualMap(Object qual) {
        if (qual instanceof Map) {
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) qual).entrySet()) {
                if (entry.getKey().equals("_id")) {
                    // check if we need to convert any String version of an id to an ObjectId
                    if (!(entry.getValue() instanceof ObjectId)) {
                        // Support a simple query as well as equality
                        if (entry.getValue() instanceof Map && ((Map)entry.getValue()).size() == 1) {
                            Map.Entry<String, Object> first =
                                    ((Map<String, Object>)entry.getValue()).entrySet().iterator().next();
                            // The key should be the operator and the value is the id
                            String idKey = first.getKey();
                            Object idVal = first.getValue();
                            // Handles {$in: [<id>...]}
                            if (idVal instanceof List) {
                                // Parse for each element in the list
                                List<Object> finalIdVal = new ArrayList<>();
                                ((List)idVal).forEach(value -> {
                                    Map obj = (Map)cleanQualMap(Collections.singletonMap("_id",value));
                                    finalIdVal.add(obj.get("_id"));
                                });
                                entry.setValue(Collections.singletonMap(idKey, finalIdVal));
                            } else {
                                Map idCheck = (Map)cleanQualMap(Collections.singletonMap("_id", idVal));
                                entry.setValue(Collections.singletonMap(idKey, idCheck.get("_id")));
                            }
                            continue;
                        } else if (entry.getValue() == null || !(entry.getValue() instanceof String) ||
                                !ObjectId.isValid((String)entry.getValue())) {
                            throw new RuntimeException(
                                "The value '" + entry.getValue() + 
                                        "' given as a document identifier is not legal. Please confirm " +
                                        "that you have supplied the contents of the '${ID_FIELD}' property.");
                        }
                        //noinspection GroovyAssignabilityCheck
                        entry.setValue(new ObjectId((String)entry.getValue()));
                    }
                } else if (entry.getValue() instanceof BigDecimal) { // If the value is a BigDecimal we know there is no reason to recurse
                    entry.setValue(((BigDecimal)entry.getValue()).doubleValue());
                } else {
                    entry.setValue(cleanQualMap(entry.getValue()));
                }
            }
        } else if (qual instanceof List) {
            ((List) qual).replaceAll(this::cleanQualMap);
        }
        return qual;
    }

    /**
     * clean the qualification and observe any request limits on producing results
     * @param collection the collection to query
     * @param props the properties to return
     * @param qual the qualification
     */
    private FindPublisher<Document>
    buildFind(MongoCollection<Document> collection, ClientSession session, List<String> props, Map<String, Object> qual,
              Map<String, Object> options) {
        // No options means empty map
        if (options == null) {
            options = new HashMap<>();
        }

        // Create cursor over find
        //noinspection unchecked,rawtypes
        FindPublisher<Document> crsr = collection.find(session, new Document((Map)cleanQualMap(qual)));

        // Set the max query time allowed
        long maxTime = QUERY_TIMEOUT;
        if (options.containsKey("maxTime")) {
            maxTime = (Long)options.get("maxTime");
        }
        crsr.maxTime(maxTime, QUERY_TIMEOUT_TIMEUNIT);

        // If we have a properties list, do a projection
        if (props != null && !props.isEmpty()) {
            if (options.containsKey("excludeProps") && (boolean) options.get("excludeProps")) {
                crsr = crsr.projection(exclude(props));
            } else {
                crsr = crsr.projection(include(props));
            }
        }

        // Apply other query options
        if (options.containsKey("limit")) {
            crsr.limit((int)options.get("limit"));
        }
        if (options.containsKey("skip")) {
            crsr.skip((int)options.get("skip"));
        }
        if (options.containsKey("sort")) {
            //noinspection unchecked,rawtypes
            crsr.sort(new Document((Map)options.get("sort")));
        }
        return crsr;
    }

    @Override
    public Flowable<Map<String, Object>>
    select(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> properties,
           Map<String, Object> qual, Map<String, Object> options) {

        return collectionFromStorageName(storageName, options, (collection, session) ->
            Flowable.fromPublisher(buildFind(collection, session, new ArrayList<>(properties.keySet()), qual, options))
                .map(AtlasStorageMgr::externalizeId));
    }

    /**
     * map the database name to an existing connection / database or create a new one
     * 
     * @param databaseName Atlas database name
     * @param cmdFunction function to execute with the session
     * @param <T> typically a Map, but generally the type of the observable expected
     */
    <T> Flowable<T> usingSession(String databaseName, String xid, BiFunction<MongoDatabase, ClientSession,
                                 Publisher<T>> cmdFunction) {
        Single<ImmutablePair<MongoDatabase, ClientSession>> dbSessionObs;
        if (xid != null) {
            // if we are in a transaction use its client and session to run the command
            val errorIfMissing = Single.<ImmutablePair<MongoClient, ClientSession>>error(new Exception(
                    "No transaction found for id " + xid));
            dbSessionObs = transactions.asMap().getOrDefault(xid, errorIfMissing).observeOn(rxScheduler).map(cliSess ->
                new ImmutablePair<>(cliSess.getLeft().getDatabase(databaseName), cliSess.getRight())
            );
        } else {
            dbSessionObs = dbSessions.asMap().computeIfAbsent(databaseName, name -> {
                Single<MongoClient> clientObs = connection.connect(config);
                return clientObs.flatMapPublisher(client -> {
                    Flowable<ClientSession> sessionObs = Flowable.fromPublisher(client.startSession());
                    return sessionObs.map(session -> new ImmutablePair<>(client.getDatabase(databaseName), session));
                }).compose(new CacheIfNotEmptyOrError<>()).firstOrError();
            }).observeOn(rxScheduler);
        }
        
        AtomicLong start = new AtomicLong();
        // mongodb always calls back on its own thread. switch back to the vertx event loop context via observeOn
        return dbSessionObs.flatMapPublisher(sessionDb -> cmdFunction.apply(sessionDb.getLeft(), sessionDb.getRight()))
                .observeOn(rxScheduler == null ? Schedulers.trampoline() : rxScheduler)
                .doOnSubscribe(s ->start.set(System.nanoTime()))
                .doOnComplete(() -> {
            long end = System.nanoTime();
            long duration = end - start.get();
            log.debug("Atlas round trip time: {} ms", duration / 1000000);
        });
    }
    
    String buildStorageName(String databaseName, String collectionName) {
        return databaseName + ":" + collectionName;
    }
    
    <T> Flowable<T>
    collectionFromStorageName(String storageName, Map<String, Object> options, BiFunction<MongoCollection<Document>,
                              ClientSession, Publisher<T>> cmdFunction) {
        if (storageName == null) {
            throw new IllegalArgumentException("No storage name specified type type");
        }
        String[] parts = storageName.split(":");
        String databaseName = parts.length < 2 ? config.obtainDefaultDatabase(): parts[0];
        String collectionName = parts.length < 2 ? parts[0]: parts[1];
        String transaction = options == null ? null : (String)options.get("transaction");
        return usingSession(databaseName, transaction, (db, session) ->
                cmdFunction.apply(db.getCollection(collectionName), session));
    }
    
    @Override
    public Maybe<Map<String, Object>>
    selectOne(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> properties,
              Map<String, Object> qual, Map<String, Object> options) {
        return select(storageName, storageManagerReference, properties, qual, options)
            .singleElement()
            .onErrorResumeNext(t -> {
                if (t instanceof IllegalArgumentException) {
                    return Maybe.error(new Exception(
                        MessageFormat.format(
                            "In selectOne, more than one instance of type: {0} was found for qual {1}.",
                            storageName, new JsonObject(qual).toString())));
                }
                return Maybe.error(t);
            });
    }

    @Override
    public Single<Integer>
    delete(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual,
           Map<String, Object> options) {
        //noinspection unchecked
        return collectionFromStorageName(storageName, options, (dbCol, sess) ->
            dbCol.deleteMany(sess, new Document((Map<String, Object>)cleanQualMap(qual)))
        ).singleOrError().map(dr ->
            Math.toIntExact(dr.getDeletedCount())
        );
    }

    @Override
    public Completable
    startTransaction(String vantiqTransactionId, Map<String, Object> options) {
        return Completable.fromAction(() ->
            transactions.asMap().computeIfAbsent(vantiqTransactionId, id -> 
                Flowable.defer(() -> {
                    Single<MongoClient> clientObs = connection.connect(config);
                    return clientObs.flatMapPublisher(client ->
                        Flowable.fromPublisher(client.startSession()).map(session -> {
                            session.startTransaction();
                            return new ImmutablePair<>(client, session);
                        }).switchIfEmpty(
                            // if we get here, startSession returned an empty publisher, should  never happen, but ...
                            Flowable.error(new Exception("Failed to create a session in which to start a transaction"))
                        )
                    );
                }).compose(new CacheIfNotEmptyOrError<>()).firstOrError()
            )
        );
    }

    @Override
    public Completable commitTransaction(String vantiqTransactionId, Map<String, Object> options) {
        Single<ImmutablePair<MongoClient, ClientSession>> cliSessObs = transactions.asMap().remove(vantiqTransactionId);
        if (cliSessObs == null) {
            return Completable.error(new Exception("No transaction found for id " + vantiqTransactionId));
        }
        // 1st observeOn ensures we switch from thread that cached the session, while the 2nd switches from the thread
        // that mongo the mongodb client calls back on
        return cliSessObs.observeOn(rxScheduler).flatMapPublisher(cliSess ->
            Flowable.fromPublisher(cliSess.getRight().commitTransaction()).observeOn(rxScheduler).doOnComplete(() ->
                log.debug("transaction {} committed", vantiqTransactionId)
            ).doOnError(t ->
                log.debug("transaction {} failed to commit with error: {}", vantiqTransactionId, t.getMessage())
            ).doOnComplete(() -> cliSess.getRight().close())
        ).ignoreElements();
    }

    @Override
    public Completable abortTransaction(String vantiqTransactionId, Map<String, Object> options) {
        Single<ImmutablePair<MongoClient, ClientSession>> cliSessObs = transactions.asMap().remove(vantiqTransactionId);
        if (cliSessObs == null) {
            return Completable.error(new Exception("No transaction found for id " + vantiqTransactionId));
        }
        // 1st observeOn ensures we switch from thread that cached the session, while the 2nd switches from the thread
        // that the mongodb client calls back on
        return cliSessObs.observeOn(rxScheduler).flatMapPublisher(cliSess ->
            Flowable.fromPublisher(cliSess.getRight().abortTransaction()).observeOn(rxScheduler).doOnComplete(() ->
                cliSess.getRight().close()
            )
        ).ignoreElements();
    }

    @Override
    public Flowable<Map<String, Object>> aggregate(String storageName, Map<String, Object> storageManagerReference,
                                                   List<Map<String, Object>> pipeline, Map<String, Object> options) {
        return this.collectionFromStorageName(storageName, options, (collection, session) -> {
            List<Document> mongoPipeline = pipeline.stream().map(Document::new).collect(Collectors.toList());
            return collection.aggregate(session, mongoPipeline).maxTime(QUERY_TIMEOUT, QUERY_TIMEOUT_TIMEUNIT);
        }).map(AtlasStorageMgr::externalizeId);
    }
}
