package io.vantiq.atlasConnector;

import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Projections.include;

import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import io.vantiq.svcconnector.InstanceConfigUtils;
import io.vantiq.svcconnector.VantiqStorageManager;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class AtlasStorageMgr implements VantiqStorageManager {
    static final long QUERY_TIMEOUT = 50L;
    static final TimeUnit QUERY_TIMEOUT_TIMEUNIT = TimeUnit.SECONDS;
    
    Connection connection;
    InstanceConfigUtils config = new InstanceConfigUtils();
    ConcurrentMap<String, Single<MongoDatabase>> sessions = new ConcurrentHashMap<>();
    
    @Override
    public Completable initialize() {
        config.loadServerConfig();
        connection = new Connection();
        // get a connection and run a ping command
        return connection.connect(config).flatMapPublisher(client -> {
            MongoDatabase adminDb = client.getDatabase("admin");
            return Flowable.fromPublisher(adminDb.runCommand(new Document("ping", 1))).map(doc -> {
                if (doc.get("ok", 0) != 1) {
                    throw new Exception("Failed to ping MongoDB Atlas");
                }
                return doc;
            });
        }).ignoreElements();
    }

    @Override
    public Single<Map<String, Object>> getTypeRestrictions() {
        // we support everything the VANTIQ type system supports
        return Single.just(new HashMap<>());
    }

    @Override
    public Single<Map<String, Object>>
    initializeTypeDefinition(Map<String, Object> proposedType, Map<String, Object> existingType) {
        boolean capped = false;
        int cappedSize = (Integer)proposedType.get("maxStorage");
        int maxDocs = 0;
        if (cappedSize > 0) {
            if (proposedType.containsKey("maxObjects")) {
                maxDocs = (Integer)proposedType.get("maxObjects");
            }
            capped = true;
        }
        if (existingType == null) {
            CreateCollectionOptions createOptions = new CreateCollectionOptions()
                    .capped(capped)
                    .sizeInBytes(cappedSize)
                    .maxDocuments(maxDocs);
            return usingSession(config.obtainDefaultDatabase(), db ->
                    db.createCollection((String) proposedType.get("name"), createOptions))
                .doOnComplete(() -> {
                    // track the database for collection in the storage name of the proposed type
                    proposedType.put("storageName", config.obtainDefaultDatabase() + "." + proposedType.get("name"));
                }).onErrorResumeNext(t -> {
                    // if the collection already exists, we're good
                    if (t.getMessage().contains("already exists")) {
                        proposedType.put("storageName", config.obtainDefaultDatabase() + "." + proposedType.get("name"));
                        return Flowable.empty();
                    } else {
                        return Flowable.error(t);
                    }
                }).ignoreElements().andThen(Single.just(proposedType));
        } else {
            // check if the definition of indexes has changed
            List<Map<String, Object>> delete = new ArrayList<>();
            List<Map<String, Object>> add = new ArrayList<>();
            analyzeIndexes(existingType, proposedType, delete, add);
            return usingSession(config.obtainDefaultDatabase(), db -> {
                MongoCollection<Document> collection = db.getCollection((String) proposedType.get("name"));
                return Flowable.concat(
                    // delete indexes that are no longer in the proposed type
                    Flowable.fromIterable(delete).flatMap(index ->
                        collection.dropIndex(computeKeySet(index))
                    ),
                    // add indexes that are in the proposed type but not in the existing type
                    Flowable.fromIterable(add).flatMap(addIndex -> {
                        //noinspection unchecked
                        Map<String,Object> options = (Map<String,Object>)addIndex.get("options");
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
                        return collection.createIndex(computeKeySet(addIndex), idxOptions);
                }));
            }).ignoreElements().andThen(Single.just(proposedType));
        }
    }

    static Document computeKeySet(Map<String, Object> index) {
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
        ((List<Map<String, Object>>)existingType.get("indexes")).stream().filter( index -> {
            // check if the index is in the proposed type
            return ((List<Map<String, Object>>)proposedType.get("indexes")).stream().noneMatch( proposedIndex -> 
                proposedIndex.get("keys").equals(index.get("keys"))
            );
        }).forEach(deleteList::add);
        
        // if the index is not in the existing type, add it to the add list
        ((List<Map<String, Object>>)proposedType.get("indexes")).stream().filter( index -> {
            // check if the index is in the existing type
            return ((List<Map<String, Object>>)existingType.get("indexes")).stream().noneMatch( existingIndex ->
                existingIndex.get("keys").equals(index.get("keys"))
            );
        }).forEach(addList::add);
    }
    
    @Override
    public Single<Map<String, Object>> insert(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> values) {
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
        return collectionFromStorageName(storageName, collection -> collection.insertOne(doc))
                .firstOrError()
                .map(insertOne -> externalizeId(doc));
    }

    static Map<String, Object> externalizeId(Map<String, Object> obj) {
        if (obj.containsKey("_id")) {
            obj.put("_id", obj.get("_id").toString());
        }
        return obj;
    }
    
    @Override
    public Flowable<Map<String, Object>> insertMany(String storageName, Map<String, Object> storageManagerReference, List<Map<String, Object>> values) {
        return null;
    }

    @Override
    public Maybe<Map<String, Object>> update(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> values, Map<String, Object> qual) {
        return null;
    }

    @Override
    public Single<Integer> count(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual, Map<String, Object> options) {
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
        return collectionFromStorageName(storageName, collection -> {
            //noinspection unchecked
            Document query = new Document((Map<String, Object>)cleanQualMap(qual));
            return collection.countDocuments(query, countOptions);
        }).map(Long::intValue).firstOrError();
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
    
    private FindPublisher<Document>
    buildFind(MongoCollection<Document> collection, List<String> props, Map<String, Object> qual,
              Map<String, Object> options) {
        // No options means empty map
        if (options == null) {
            options = new HashMap<>();
        }

        // Create cursor over find
        //noinspection unchecked,rawtypes
        FindPublisher<Document> crsr = collection.find(new Document((Map)cleanQualMap(qual)));

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
    select(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> properties, Map<String,
           Object> qual, Map<String, Object> options) {

        return collectionFromStorageName(storageName, collection ->
            Flowable.fromPublisher(buildFind(collection, new ArrayList<>(properties.keySet()), qual, options))
                .map(AtlasStorageMgr::externalizeId));
    }

    <T> Flowable<T> usingSession(String databaseName, Function<MongoDatabase, Publisher<T>> cmdFunction) {
        Single<MongoDatabase> sessionObs = sessions.computeIfAbsent(databaseName, name -> {
            Single<MongoClient> clientObs = connection.connect(config);
            return clientObs.map(client -> client.getDatabase(databaseName)).cache();
        });
        return sessionObs.flatMapPublisher(cmdFunction::apply);
    }
    
    <T> Flowable<T> collectionFromStorageName(String storageName, Function<MongoCollection<Document>, Publisher<T>> cmdFunction) {
        String[] parts = storageName.split("\\.");
        String databaseName = parts.length < 2 ? config.obtainDefaultDatabase(): parts[0];
        String collectionName = parts.length < 2 ? parts[0]: parts[1];
        return usingSession(databaseName, db -> cmdFunction.apply(db.getCollection(collectionName)));
    }
    
    @Override
    public Maybe<Map<String, Object>> selectOne(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> properties, Map<String, Object> qual, Map<String, Object> options) {
        return null;
    }

    @Override
    public Single<Integer> delete(String storageName, Map<String, Object> storageManagerReference, Map<String, Object> qual) {
        return null;
    }
}
