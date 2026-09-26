package com.apex.orders.adapters;

import com.apex.orders.adapters.mongo.MongoStore;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.kafka.core.KafkaTemplate;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import java.util.concurrent.TimeUnit;

public final class OutboxPublisher {
    @FunctionalInterface public interface Sender { void send(String topic, String key, String payload) throws Exception; }
    private final MongoStore store;
    private final Sender sender;
    private final int maxPermanentFailures;
    private final long retryDelayMs;
    public OutboxPublisher(MongoStore store, Sender sender) { this(store, sender, 3, 5000); }
    public OutboxPublisher(MongoStore store, Sender sender, int maxPermanentFailures, long retryDelayMs) {
        if (maxPermanentFailures < 1 || retryDelayMs < 0) throw new IllegalArgumentException("Invalid outbox retry settings");
        this.store = store; this.sender = sender;
        this.maxPermanentFailures = maxPermanentFailures; this.retryDelayMs = retryDelayMs;
    }
    public static Sender kafka(KafkaTemplate<String, String> kafka) { return (topic, key, payload) -> kafka.send(topic, key, payload).get(10, TimeUnit.SECONDS); }
    private static boolean permanent(Throwable ex) {
        for (Throwable cause = ex; cause != null; cause = cause.getCause())
            if (cause instanceof RecordTooLargeException || cause instanceof SerializationException) return true;
        return false;
    }
    @Scheduled(fixedDelayString = "${outbox.delay-ms:1000}")
    public void publish() {
        try {
            for (var entry : store.pending()) {
                String id = entry.getString("_id");
                try {
                    sender.send(entry.getString("topic"), entry.getString("key"), entry.getString("payload"));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception ex) {
                    store.publicationFailed(id, ex.getClass().getSimpleName(), permanent(ex), maxPermanentFailures, retryDelayMs);
                    LoggerFactory.getLogger(OutboxPublisher.class).warn("transition=outbox_retry eventId={} errorType={}", id, ex.getClass().getSimpleName());
                    continue;
                }
                // A failure marking sent is not a poison message; leave it for redelivery.
                store.sent(id);
                LoggerFactory.getLogger(OutboxPublisher.class).info("transition=published orderId={} eventId={}", entry.getString("key"), id);
            }
        } catch (Exception ex) {
            LoggerFactory.getLogger(OutboxPublisher.class).warn("transition=outbox_pending errorType={}", ex.getClass().getSimpleName());
        }
    }
}
