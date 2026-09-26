package com.apex.orders.adapters.mongo;

import com.apex.orders.application.Ports.Envelope;
import com.apex.orders.domain.Order;
import com.apex.orders.domain.Result;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.bson.Document;

import java.time.Instant;
import java.util.UUID;

class EventDocuments {
    static final int EVENT_VERSION = 1;
    static final String CALCULATION_POLICY = "v1";
    static final String COMPONENT = "order-processor";

    private final ObjectMapper json;

    EventDocuments(ObjectMapper json) {
        this.json = json;
    }

    static Document receipt(Envelope e, String id) {
        return new Document("_id", id)
                .append("receivedAt", e.receivedAt().toString())
                .append("topic", e.topic())
                .append("partition", e.partition())
                .append("offset", e.offset());
    }

    Document dlt(Envelope e, String eventId, String orderId, DltCategory category, String cause, int attempts) {
        return new Document("eventId", UUID.randomUUID().toString())
                .append("sourceEventId", eventId)
                .append("orderId", orderId)
                .append("sourceEventVersion", recoverVersion(e.raw(), "eventVersion"))
                .append("orderVersion", recoverVersion(e.raw(), "orderVersion"))
                .append("category", category.name())
                .append("cause", cause)
                .append("attempts", attempts)
                .append("timestamp", Instant.now().toString())
                .append("component", COMPONENT)
                .append("originalMessage", e.raw())
                .append("sourceTopic", e.topic())
                .append("sourcePartition", e.partition())
                .append("sourceOffset", e.offset());
    }

    static Document processedEvent(Order order, Result result) {
        return new Document("eventId", UUID.randomUUID().toString())
                .append("eventVersion", EVENT_VERSION)
                .append("orderVersion", order.orderVersion())
                .append("occurredAt", Instant.now().toString())
                .append("sourceEventId", order.eventId())
                .append("sourceEventVersion", EVENT_VERSION)
                .append("orderId", order.orderId())
                .append("status", result.status())
                .append("market", order.market())
                .append("currency", order.currency())
                .append("totals", result.totals())
                .append("reason", result.reason());
    }

    private Long recoverVersion(String raw, String field) {
        try {
            var value = json.readTree(raw).path(field);
            return value.isIntegralNumber() && value.canConvertToLong() ? value.longValue() : null;
        } catch (Exception ignored) {
            return null;
        }
    }
}
