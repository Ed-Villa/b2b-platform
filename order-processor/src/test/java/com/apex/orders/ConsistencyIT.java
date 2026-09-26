package com.apex.orders;

import com.apex.orders.adapters.Json;
import com.apex.orders.adapters.mongo.MongoStore;
import com.apex.orders.adapters.OrderListener;
import com.apex.orders.adapters.OutboxPublisher;
import com.apex.orders.application.Ports.Catalog;
import com.apex.orders.application.Ports.Envelope;
import com.apex.orders.application.ProcessOrder;
import com.apex.orders.domain.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.util.backoff.FixedBackOff;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static com.mongodb.client.model.Filters.eq;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class ConsistencyIT {
    @Container
    static MongoDBContainer mongo = new MongoDBContainer("mongo:7.0.18");
    @Container
    static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.8.0"));
    MongoClient client;
    MongoDatabase db;
    MongoStore store;
    List<org.bson.BsonDocument> commands = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setup() {
        client = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                .applyConnectionString(new com.mongodb.ConnectionString(mongo.getReplicaSetUrl()))
                .addCommandListener(new com.mongodb.event.CommandListener() {
                    public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                        commands.add(event.getCommand().clone());
                    }
                }).build());
        String database = "test_" + UUID.randomUUID().toString().replace("-", "");
        db = client.getDatabase(database);
        store = new MongoStore(client, database, Json.mapper());
    }

    @AfterEach
    void close() {
        db.drop();
        client.close();
    }

    Envelope envelope(String raw) {
        return new Envelope(raw, "orders.created.v1", 0, 1, Instant.now());
    }

    void save(String event, long version) {
        Order o = RulesTest.order(event, version);
        store.save(envelope("original"), o, new Rules().calculate(o, RulesTest.client(), List.of(RulesTest.product())), 1);
    }

    @Test
    void outputPreservesOrderRevisionWithoutChangingSchemaTopic() throws Exception {
        save("REVISION-2", 2);
        var pending = store.pending().getFirst();
        assertEquals("orders.processed.v1", pending.getString("topic"));
        var event = Json.mapper().readTree(pending.getString("payload"));
        assertEquals(2L, event.path("orderVersion").asLong());
        assertEquals(1L, event.path("eventVersion").asLong());
        assertEquals(1L, event.path("sourceEventVersion").asLong());
        assertEquals(2L, db.getCollection("orders").find().first().getLong("orderVersion"));
    }

    @Test
    void refusesLegacyDataWithoutModifyingIt() {
        db.getCollection("orders").insertOne(new Document("_id", "legacy").append("eventVersion", 2));
        var error = assertThrows(IllegalStateException.class, () -> new MongoStore(client, db.getName(), Json.mapper()));
        assertTrue(error.getMessage().contains("MONGODB_DATABASE"));
        assertEquals(2, db.getCollection("orders").find(eq("_id", "legacy")).first().getInteger("eventVersion"));
    }

    @Test
    void dltPreservesRecoverableVersions() {
        store.invalid(envelope("{\"eventVersion\":9,\"orderVersion\":3}"), "E", "O", "UNSUPPORTED_SCHEMA_VERSION");
        var payload = Document.parse(store.pending().getFirst().getString("payload"));
        assertEquals(9, payload.getInteger("sourceEventVersion"));
        assertEquals(3, payload.getInteger("orderVersion"));
    }

    @Test
    void concurrentDuplicateHasOneEffect() throws Exception {
        try (var pool = Executors.newFixedThreadPool(8)) {
            var start = new CountDownLatch(1);
            var tasks = new ArrayList<Future<?>>();
            for (int i = 0; i < 8; i++)
                tasks.add(pool.submit(() -> {
                    start.await();
                    save("E", 1);
                    return null;
                }));
            start.countDown();
            for (var task : tasks) task.get(30, TimeUnit.SECONDS);
        }
        assertEquals(1, db.getCollection("orders").countDocuments());
        assertEquals(1, db.getCollection("revisions").countDocuments());
        assertEquals(1, store.pending().size());
    }

    @Test
    void sameRevisionDifferentEventsProducesConflict() throws Exception {
        try (var pool = Executors.newFixedThreadPool(2)) {
            var start = new CountDownLatch(1);
            var a = pool.submit(() -> {
                start.await();
                save("A", 1);
                return null;
            });
            var b = pool.submit(() -> {
                start.await();
                save("B", 1);
                return null;
            });
            start.countDown();
            a.get(30, TimeUnit.SECONDS);
            b.get(30, TimeUnit.SECONDS);
        }
        assertEquals(1, db.getCollection("orders").countDocuments());
        assertEquals(1, db.getCollection("inbox").countDocuments(eq("disposition", "CONFLICT")));
        assertEquals(2, store.pending().size());
        assertEquals(1, store.pending().stream().filter(d -> d.getString("topic").equals("orders.processing.dlt")).count());
    }

    @Test
    void obsoleteCannotOverwriteNewer() {
        save("NEW", 2);
        save("OLD", 1);
        assertEquals(2L, db.getCollection("orders").find().first().getLong("orderVersion"));
        assertEquals(1, store.pending().size());
    }

    @Test
    void moneyIsStoredAsDecimal128AndTechnicalFailureGoesToDlt() {
        save("E", 1);
        var totals = db.getCollection("orders").find().first().get("totals", Document.class);
        assertEquals(new java.math.BigDecimal("958.67"), totals.get("grandTotal", org.bson.types.Decimal128.class).bigDecimalValue());
        Order next = RulesTest.order("F", 2);
        store.save(envelope("original failure"), next, Result.failure("TECHNICAL_FAILURE", RulesTest.client(), "PRODUCT_HTTP_503"), 3);
        var dlt = store.pending().stream().filter(d -> d.getString("topic").equals("orders.processing.dlt")).findFirst().orElseThrow();
        assertTrue(dlt.getString("payload").contains("\"attempts\":3"));
        assertNull(db.getCollection("orders").find().first().get("totals"));
    }

    @Test
    void concurrentVersionsKeepNewest() throws Exception {
        try (var pool = Executors.newFixedThreadPool(2)) {
            var a = pool.submit(() -> save("OLD", 1));
            var b = pool.submit(() -> save("NEW", 2));
            a.get(30, TimeUnit.SECONDS);
            b.get(30, TimeUnit.SECONDS);
        }
        assertEquals(2L, db.getCollection("orders").find().first().getLong("orderVersion"));
    }

    @Test
    void invalidIsDurableWithoutOrder() {
        store.invalid(envelope("{broken"), null, null, "INVALID_INPUT");
        store.invalid(envelope("{broken"), null, null, "INVALID_INPUT");
        assertEquals(0, db.getCollection("orders").countDocuments());
        assertEquals(1, store.pending().size());
        assertTrue(store.pending().getFirst().getString("payload").contains("{broken"));
    }

    @Test
    void rollbackBeforeCommitLeavesNoPartialState() {
        // Force an outbox insert failure after inbox/revision/order writes inside the transaction.
        db.runCommand(new Document("collMod", "outbox").append("validator", new Document("forbiddenField", new Document("$exists", true))).append("validationLevel", "strict"));
        assertThrows(RuntimeException.class, () -> save("E", 1));
        assertEquals(0, db.getCollection("inbox").countDocuments());
        assertEquals(0, db.getCollection("orders").countDocuments());
        assertEquals(0, db.getCollection("revisions").countDocuments());
    }

    @Test
    void publishThenCrashRedeliversStableEventId() {
        save("E", 1);
        var delivered = new ArrayList<String>();
        new OutboxPublisher(store, (topic, key, payload) -> {
            delivered.add(payload);
            throw new IllegalStateException("crash after broker ack");
        }, 3, 0).publish();
        assertEquals(1, store.pending().size());
        new OutboxPublisher(store, (topic, key, payload) -> delivered.add(payload)).publish();
        assertEquals(0, store.pending().size());
        assertEquals(delivered.get(0), delivered.get(1));
        assertEquals(1, db.getCollection("orders").countDocuments());
    }

    @Test
    void majorityReadsAndSnapshotTransactionAreExplicit() {
        commands.clear();
        store.seen("E");
        store.pending();
        save("E", 1);
        var nonTransactionalReads = commands.stream().filter(c -> c.containsKey("find") && !c.containsKey("txnNumber")).toList();
        assertFalse(nonTransactionalReads.isEmpty());
        for (var command : nonTransactionalReads)
            assertEquals("majority", command.getDocument("readConcern").getString("level").getValue());
        assertTrue(commands.stream().anyMatch(c -> c.containsKey("startTransaction")
                && c.getDocument("readConcern").getString("level").getValue().equals("snapshot")));
        assertTrue(commands.stream().anyMatch(c -> c.containsKey("commitTransaction")
                && c.getDocument("writeConcern").getString("w").getValue().equals("majority")));
    }

    @Test
    void technicalFailureNeedsNewIdentityAndHigherSnapshotRevision() {
        Order failed = RulesTest.order("FAILED", 1);
        store.save(envelope("original"), failed, Result.failure("TECHNICAL_FAILURE", RulesTest.client(), "PRODUCT_HTTP_503"), 3);
        save("FAILED", 1); // A retry after an already confirmed commit must remain a duplicate.
        assertEquals("TECHNICAL_FAILURE", db.getCollection("orders").find().first().getString("status"));
        assertEquals(1, db.getCollection("inbox").countDocuments());
        save("RECOVERY", 3); // Snapshots allow gaps; no v2 is required.
        assertEquals("APPROVED", db.getCollection("orders").find().first().getString("status"));
        save("LATE", 2);
        assertEquals(3L, db.getCollection("orders").find().first().getLong("orderVersion"));
        assertEquals(1, db.getCollection("inbox").countDocuments(eq("disposition", "OBSOLETE")));
    }

    @Test
    void auditSnapshotSurvivesLaterCatalogValuesAndOrderRevision() {
        save("FIRST", 1);
        var newer = RulesTest.order("NEXT", 2);
        var changedClient = new Client("CLI-99821", "Client", "ACTIVE", "RETAIL", "EXEMPT", "MX");
        var changedProduct = new Product("PRD-001", "Updated", "NEW-SKU", "ACTIVE", "REDUCED");
        store.save(envelope("next"), newer, new Rules().calculate(newer, changedClient, List.of(changedProduct)), 1);
        var original = db.getCollection("revisions").find(eq("eventId", "FIRST")).first().get("snapshot", Document.class);
        assertEquals("GENERAL", original.get("client", Document.class).getString("taxRegime"));
        assertEquals("STANDARD", original.getList("lines", Document.class).getFirst().getString("taxCategory"));
        assertEquals("v1", original.getString("calculationPolicyVersion"));
        assertEquals(new java.math.BigDecimal("35.50"), original.get("input", Document.class)
                .getList("items", Document.class).getFirst().get("unitPrice", org.bson.types.Decimal128.class).bigDecimalValue());
        assertEquals("EXEMPT", db.getCollection("orders").find().first().get("client", Document.class).getString("taxRegime"));
    }

    @Test
    void poisonMessageDoesNotBlockOtherOrdersAndIsParkedWithoutLoss() {
        store.invalid(new Envelope("poison", "input", 0, 1, Instant.now()), null, "BAD", "INVALID_INPUT");
        store.invalid(new Envelope("good", "input", 0, 2, Instant.now()), null, "GOOD", "INVALID_INPUT");
        var delivered = new ArrayList<String>();
        var publisher = new OutboxPublisher(store, (topic, key, payload) -> {
            if (key.equals("BAD"))
                throw new ExecutionException(new org.apache.kafka.common.errors.RecordTooLargeException("too large"));
            delivered.add(key);
        }, 3, 0);
        publisher.publish();
        assertEquals(List.of("GOOD"), delivered);
        publisher.publish();
        publisher.publish();
        var poison = db.getCollection("outbox").find(eq("key", "BAD")).first();
        assertTrue(poison.getBoolean("parked"));
        assertFalse(poison.getBoolean("sent"));
        assertEquals(3, poison.getInteger("publicationAttempts"));
        assertNotNull(poison.getString("payload"));
        assertTrue(store.pending().isEmpty());
    }

    @Test
    void transientBrokerFailuresAreDelayedNotParked() {
        save("E", 1);
        String id = store.pending().getFirst().getString("_id");
        var publisher = new OutboxPublisher(store, (topic, key, payload) -> {
            throw new org.apache.kafka.common.errors.TimeoutException("broker unavailable");
        }, 1, 0);
        for (int i = 0; i < 4; i++) publisher.publish();
        assertEquals(1, store.pending().size());
        store.publicationFailed(id, "TimeoutException", false, 1, 60000);
        assertTrue(store.pending().isEmpty());
        var entry = db.getCollection("outbox").find(eq("_id", id)).first();
        assertFalse(entry.getBoolean("parked", false));
        assertFalse(entry.getBoolean("sent"));
        assertEquals(5, entry.getInteger("publicationAttempts"));
    }

    @Test
    void endToEndThroughKafkaAndMongo() throws Exception {
        String input = "input-" + UUID.randomUUID();

        try (var admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1), new NewTopic("orders.processed.v1", 1, (short) 1), new NewTopic("orders.processing.dlt", 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
        }
        var producerConfig = new HashMap<String, Object>();
        producerConfig.put("bootstrap.servers", kafka.getBootstrapServers());
        producerConfig.put("key.serializer", StringSerializer.class);
        producerConfig.put("value.serializer", StringSerializer.class);
        var factory = new DefaultKafkaProducerFactory<String, String>(producerConfig);
        var template = new KafkaTemplate<>(factory);
        var consumerConfig = new HashMap<String, Object>();
        consumerConfig.put("bootstrap.servers", kafka.getBootstrapServers());
        consumerConfig.put("group.id", "test-" + UUID.randomUUID());
        consumerConfig.put("auto.offset.reset", "earliest");
        consumerConfig.put("enable.auto.commit", false);
        consumerConfig.put("key.deserializer", StringDeserializer.class);
        consumerConfig.put("value.deserializer", StringDeserializer.class);
        var config = new ContainerProperties(input);
        config.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        Catalog catalog = new Catalog() {
            public Client client(Order o) {
                return RulesTest.client();
            }

            public Product product(Order o, String id) {
                return RulesTest.product();
            }
        };
        var listener = new OrderListener(Json.mapper(), new ProcessOrder(catalog, store, new Rules()), store);
        config.setMessageListener((AcknowledgingMessageListener<String, String>) listener::receive);
        var container = new KafkaMessageListenerContainer<>(new DefaultKafkaConsumerFactory<String, String>(consumerConfig), config);
        container.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(100, 3)));
        container.start();

        try (var output = new KafkaConsumer<String, String>(consumerConfig)) {
            output.subscribe(List.of("orders.processed.v1", "orders.processing.dlt"));
            template.send(input, "ORD-1", RulesTest.eventJson("E", 1)).get(10, TimeUnit.SECONDS);
            long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (store.pending().isEmpty() && System.nanoTime() < until) Thread.sleep(100);
            assertEquals(1, store.pending().size());
            new OutboxPublisher(store, OutboxPublisher.kafka(template)).publish();
            boolean received = false;
            while (!received && System.nanoTime() < until) for (var record : output.poll(Duration.ofMillis(500))) {
                if (record.topic().equals("orders.processed.v1")) {
                    assertEquals("APPROVED", Json.mapper().readTree(record.value()).path("status").asText());
                    received = true;
                }
            }
            assertTrue(received, "processed event must reach Kafka");
            assertEquals(1, db.getCollection("orders").countDocuments());
        } finally {
            container.stop();
            factory.destroy();
        }
    }
}
