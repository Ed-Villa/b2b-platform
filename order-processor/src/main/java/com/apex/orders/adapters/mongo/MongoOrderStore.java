package com.apex.orders.adapters.mongo;

import com.apex.orders.application.Ports.Envelope;
import com.apex.orders.domain.Order;
import com.apex.orders.domain.Result;
import com.mongodb.MongoException;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

class MongoOrderStore {
    static final String COL_INBOX = "inbox";
    static final String COL_ORDERS = "orders";
    static final String COL_REVISIONS = "revisions";

    private static final String TOPIC_PROCESSED = "orders.processed.v1";
    private static final String TOPIC_DLT = "orders.processing.dlt";
    private static final int MAX_TRANSACTION_ATTEMPTS = 8;

    private final MongoClient client;
    private final MongoDatabase db;
    private final BsonConverter bson;
    private final EventDocuments events;
    private final MongoOutboxStore outbox;

    MongoOrderStore(MongoClient client, MongoDatabase db, BsonConverter bson, EventDocuments events, MongoOutboxStore outbox) {
        this.client = client;
        this.db = db;
        this.bson = bson;
        this.events = events;
        this.outbox = outbox;
    }

    private static void log(String transition, Order order) {
        LoggerFactory.getLogger(MongoOrderStore.class).info(
                "transactionDecision={} orderId={} eventId={} revision={}",
                transition, order.orderId(), order.eventId(), order.orderVersion());
    }

    boolean seen(String eventId) {
        return db.getCollection(COL_INBOX).find(eq("_id", "event:" + eventId)).first() != null;
    }

    void save(Envelope env, Order order, Result result, int attempts) {
        transact(session -> {
            String id = "event:" + order.eventId();
            if (isDuplicate(session, id)) {
                log("duplicate", order);
                return;
            }
            Disposition disposition = resolveDisposition(session, order);
            db.getCollection(COL_INBOX).insertOne(session,
                    EventDocuments.receipt(env, id)
                            .append("eventId", order.eventId())
                            .append("orderId", order.orderId())
                            .append("disposition", disposition.name()));

            if (disposition == Disposition.CONFLICT) {
                enqueueConflict(session, env, order);
            } else if (disposition == Disposition.PROCESSED) {
                persistOrder(session, env, order, result);
                enqueueResult(session, env, order, result, attempts);
            }
            log(disposition.name().toLowerCase(), order);
        });
    }

    void invalid(Envelope env, String eventId, String orderId, String reason) {
        transact(session -> {
            String id = "invalid:" + env.receiptId();
            if (isDuplicate(session, id)) return;
            db.getCollection(COL_INBOX).insertOne(session,
                    EventDocuments.receipt(env, id).append("disposition", Disposition.INVALID.name()));
            outbox.insert(session,
                    orderId == null ? env.receiptId() : orderId,
                    TOPIC_DLT,
                    events.dlt(env, eventId, orderId, DltCategory.VALIDATION, reason, 1));
        });
    }

    private boolean isDuplicate(ClientSession session, String inboxId) {
        return db.getCollection(COL_INBOX).find(session, eq("_id", inboxId)).first() != null;
    }

    private Disposition resolveDisposition(ClientSession session, Order order) {
        boolean conflictRevision = db.getCollection(COL_REVISIONS)
                .find(session, and(eq("orderId", order.orderId()), eq("orderVersion", order.orderVersion())))
                .first() != null;
        if (conflictRevision) return Disposition.CONFLICT;

        Document current = db.getCollection(COL_ORDERS).find(session, eq("_id", order.orderId())).first();
        if (current != null && current.getLong("orderVersion") > order.orderVersion()) return Disposition.OBSOLETE;

        return Disposition.PROCESSED;
    }

    private void enqueueConflict(ClientSession session, Envelope env, Order order) {
        outbox.insert(session, order.orderId(), TOPIC_DLT,
                events.dlt(env, order.eventId(), order.orderId(), DltCategory.VERSION_CONFLICT,
                        "Different event for committed revision", 1));
    }

    private void persistOrder(ClientSession session, Envelope env, Order order, Result result) {
        Document persisted = bson.toDocument(result)
                .append("_id", order.orderId())
                .append("orderId", order.orderId())
                .append("eventId", order.eventId())
                .append("eventVersion", EventDocuments.EVENT_VERSION)
                .append("orderVersion", order.orderVersion())
                .append("market", order.market())
                .append("currency", order.currency())
                .append("occurredAt", order.occurredAt().toString())
                .append("receivedAt", env.receivedAt().toString())
                .append("processedAt", Instant.now().toString())
                .append("source", new Document("topic", env.topic())
                        .append("partition", env.partition())
                        .append("offset", env.offset()))
                .append("input", bson.toDocument(order))
                .append("calculationPolicyVersion", EventDocuments.CALCULATION_POLICY);

        // Retain the winning inputs/result even when a later revision replaces the current order.
        db.getCollection(COL_REVISIONS).insertOne(session, new Document("orderId", order.orderId())
                .append("orderVersion", order.orderVersion())
                .append("eventId", order.eventId())
                .append("snapshot", persisted));

        Document current = db.getCollection(COL_ORDERS).find(session, eq("_id", order.orderId())).first();
        if (current == null) {
            db.getCollection(COL_ORDERS).insertOne(session, persisted);
        } else if (db.getCollection(COL_ORDERS)
                .replaceOne(session,
                        and(eq("_id", order.orderId()), eq("orderVersion", current.getLong("orderVersion"))),
                        persisted)
                .getModifiedCount() != 1) {
            throw new IllegalStateException("Concurrent revision changed");
        }
    }

    private void enqueueResult(ClientSession session, Envelope env, Order order, Result result, int attempts) {
        if ("TECHNICAL_FAILURE".equals(result.status())) {
            outbox.insert(session, order.orderId(), TOPIC_DLT,
                    events.dlt(env, order.eventId(), order.orderId(), DltCategory.EXTERNAL_TECHNICAL, result.reason(), attempts));
        } else {
            outbox.insert(session, order.orderId(), TOPIC_PROCESSED, EventDocuments.processedEvent(order, result));
        }
    }

    private void transact(Consumer<ClientSession> work) {
        for (int attempt = 0; attempt < MAX_TRANSACTION_ATTEMPTS; attempt++) {
            try (ClientSession session = client.startSession()) {
                session.withTransaction(() -> {
                    work.accept(session);
                    return true;
                }, TransactionOptions.builder()
                        .readConcern(ReadConcern.SNAPSHOT)
                        .writeConcern(WriteConcern.MAJORITY)
                        .build());
                return;
            } catch (MongoException ex) {
                if (attempt == MAX_TRANSACTION_ATTEMPTS - 1
                        || !(ex.getCode() == 11000 || ex.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)))
                    throw ex;
            }
        }
    }
}
