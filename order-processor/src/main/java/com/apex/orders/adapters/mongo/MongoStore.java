package com.apex.orders.adapters.mongo;

import com.apex.orders.application.Ports.Envelope;
import com.apex.orders.application.Ports.Store;
import com.apex.orders.domain.Order;
import com.apex.orders.domain.Result;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;

public final class MongoStore implements Store {

    private static final String COL_METADATA = "metadata";
    private static final String STORAGE_FORMAT_ID = "storage-format";
    private static final String STORAGE_FORMAT_VERSION = "separate-versions-v1";

    private final MongoOrderStore orderStore;
    private final MongoOutboxStore outboxStore;

    public MongoStore(MongoClient client, String database, ObjectMapper json) {
        MongoDatabase db = client.getDatabase(database)
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY);

        outboxStore = new MongoOutboxStore(db, json);
        orderStore = new MongoOrderStore(client, db,
                new BsonConverter(json), new EventDocuments(json), outboxStore);

        // Fail fast if this database was previously used with an incompatible
        // document/index layout (see ensureCompatibleStorage for details).
        ensureCompatibleStorage(db);

        // One revision row per (orderId, orderVersion): prevents storing two
        // different snapshots for the same order version (optimistic concurrency).
        db.getCollection(MongoOrderStore.COL_REVISIONS)
                .createIndex(
                        Indexes.compoundIndex(
                                Indexes.ascending("orderId"),
                                Indexes.ascending("orderVersion")),
                        new IndexOptions().unique(true));

        // Speeds up the outbox poller's query for unsent messages ordered by age.
        db.getCollection(MongoOutboxStore.COLLECTION)
                .createIndex(
                        Indexes.compoundIndex(
                                Indexes.ascending("sent"),
                                Indexes.ascending("createdAt")));

        // Record (once) which storage format this database uses, so future
        // instances can detect incompatible/legacy layouts via ensureCompatibleStorage.
        db.getCollection(COL_METADATA).updateOne(
                eq("_id", STORAGE_FORMAT_ID),
                Updates.setOnInsert("format", STORAGE_FORMAT_VERSION),
                new UpdateOptions().upsert(true));
    }

    // -------------------------------------------------------------------------
    // Inbound: delegate to MongoOrderStore
    // -------------------------------------------------------------------------

    private static void ensureCompatibleStorage(MongoDatabase db) {
        Document marker = db.getCollection(COL_METADATA)
                .find(eq("_id", STORAGE_FORMAT_ID))
                .first();
        boolean populated = Stream.of(
                        MongoOrderStore.COL_ORDERS, MongoOrderStore.COL_REVISIONS,
                        MongoOrderStore.COL_INBOX, MongoOutboxStore.COLLECTION)
                .anyMatch(name -> db.getCollection(name).find().limit(1).first() != null);
        boolean oldDocuments = Stream.of(MongoOrderStore.COL_ORDERS, MongoOrderStore.COL_REVISIONS)
                .anyMatch(name -> db.getCollection(name)
                        .find(exists("orderVersion", false))
                        .first() != null);
        boolean oldIndex = false;

        if (db.listCollectionNames().into(new ArrayList<>()).contains(MongoOrderStore.COL_REVISIONS))
            for (Document index : db.getCollection(MongoOrderStore.COL_REVISIONS).listIndexes())
                if (index.get("key", Document.class).containsKey("eventVersion")) oldIndex = true;

        if ((marker == null && populated)
                || oldDocuments
                || oldIndex
                || (marker != null && !STORAGE_FORMAT_VERSION.equals(marker.getString("format")))
        ) throw new IllegalStateException("INCOMPATIBLE_STORAGE_FORMAT: configure MONGODB_DATABASE with a new empty " +
                "database. Existing data and indexes were not changed.");
    }

    @Override
    public boolean seen(String eventId) {
        return orderStore.seen(eventId);
    }

    @Override
    public void save(Envelope env, Order order, Result result, int attempts) {
        orderStore.save(env, order, result, attempts);
    }

    // -------------------------------------------------------------------------
    // Outbound: delegate to MongoOutboxStore
    // -------------------------------------------------------------------------

    @Override
    public void invalid(Envelope env, String eventId, String orderId, String reason) {
        orderStore.invalid(env, eventId, orderId, reason);
    }

    public List<Document> pending() {
        return outboxStore.pending();
    }

    public void publicationFailed(String id, String errorType, boolean permanent, int maxPermanentFailures, long retryDelayMs) {
        outboxStore.publicationFailed(id, errorType, permanent, maxPermanentFailures, retryDelayMs);
    }

    // -------------------------------------------------------------------------
    // Private: storage compatibility check (startup only)
    // -------------------------------------------------------------------------

    public void sent(String id) {
        outboxStore.sent(id);
    }
}
