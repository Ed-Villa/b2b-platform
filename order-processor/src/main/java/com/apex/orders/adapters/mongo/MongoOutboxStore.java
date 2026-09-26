package com.apex.orders.adapters.mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.*;

class MongoOutboxStore {
    static final String COLLECTION = "outbox";
    private static final int BATCH_LIMIT = 100;

    private final MongoDatabase db;
    private final ObjectMapper json;

    MongoOutboxStore(MongoDatabase db, ObjectMapper json) {
        this.db = db;
        this.json = json;
    }

    void insert(ClientSession session, String key, String topic, Document payload) {
        db.getCollection(COLLECTION).insertOne(session,
                new Document("_id", payload.getString("eventId"))
                        .append("key", key)
                        .append("topic", topic)
                        .append("payload", encode(payload))
                        .append("sent", false)
                        .append("createdAt", Instant.now().toString()));
    }

    public List<Document> pending() {
        // createdAt is a scheduling hint, never the business ordering guarantee.
        return db.getCollection(COLLECTION)
                .find(and(eq("sent", false), ne("parked", true),
                        or(exists("nextAttemptAt", false), lte("nextAttemptAt", Date.from(Instant.now())))))
                .sort(Sorts.ascending("createdAt", "_id"))
                .limit(BATCH_LIMIT)
                .into(new ArrayList<>());
    }

    public void publicationFailed(String id, String errorType, boolean permanent, int maxPermanentFailures, long retryDelayMs) {
        var failures = db.getCollection(COLLECTION).findOneAndUpdate(
                and(eq("_id", id), eq("sent", false)),
                Updates.combine(
                        Updates.inc("publicationAttempts", 1),
                        Updates.inc("permanentFailures", permanent ? 1 : 0),
                        Updates.set("lastErrorType", errorType),
                        Updates.set("lastFailureAt", Instant.now().toString()),
                        Updates.set("nextAttemptAt", Date.from(Instant.now().plusMillis(retryDelayMs)))),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        if (failures != null && failures.getInteger("permanentFailures", 0) >= maxPermanentFailures) {
            db.getCollection(COLLECTION).updateOne(
                    and(eq("_id", id), eq("sent", false)),
                    Updates.set("parked", true));
            LoggerFactory.getLogger(MongoOutboxStore.class).error(
                    "transition=outbox_parked eventId={} errorType={}", id, errorType);
        }
    }

    public void sent(String id) {
        db.getCollection(COLLECTION).updateOne(
                eq("_id", id),
                Updates.combine(Updates.set("sent", true), Updates.set("sentAt", Instant.now().toString())));
    }

    private String encode(Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
