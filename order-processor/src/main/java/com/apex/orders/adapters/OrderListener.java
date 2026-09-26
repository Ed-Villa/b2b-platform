package com.apex.orders.adapters;

import com.apex.orders.application.ProcessOrder;
import com.apex.orders.application.Ports.*;
import com.apex.orders.domain.Order;
import com.apex.orders.domain.Rules;
import com.fasterxml.jackson.databind.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.slf4j.LoggerFactory;
import java.time.Instant;

public final class OrderListener {
    private final ObjectMapper json;
    private final ProcessOrder processor;
    private final Store store;
    public OrderListener(ObjectMapper json, ProcessOrder processor, Store store) { this.json = json; this.processor = processor; this.store = store; }
    @KafkaListener(topics = "orders.created.v1")
    public void receive(ConsumerRecord<String, String> record, Acknowledgment ack) {
        Envelope env = new Envelope(record.value(), record.topic(), record.partition(), record.offset(), Instant.now());
        Order order;
        JsonNode tree = null;
        try {
            tree = json.readTree(record.value() == null ? "null" : record.value());
            CreatedEvent event = json.treeToValue(tree, CreatedEvent.class);
            order = event == null ? null : event.toDomain();
            new Rules().validate(order);
        } catch (Exception ex) {
            store.invalid(env, identifier(tree, "eventId"), identifier(tree, "orderId"), "UNSUPPORTED_SCHEMA_VERSION".equals(ex.getMessage()) ? "UNSUPPORTED_SCHEMA_VERSION" : "INVALID_INPUT");
            ack.acknowledge();
            LoggerFactory.getLogger(OrderListener.class).info("transition=invalid source={}", env.receiptId());
            return;
        }
        LoggerFactory.getLogger(OrderListener.class).info("transition=received orderId={} eventId={} source={}", order.orderId(), order.eventId(), env.receiptId());
        processor.process(env, order);
        LoggerFactory.getLogger(OrderListener.class).info("transition=durably_handled orderId={} eventId={}", order.orderId(), order.eventId());
        ack.acknowledge();
    }
    private String identifier(JsonNode tree, String field) {
        String value = tree == null ? null : tree.path(field).asText(null);
        return Rules.id(value) ? value : null;
    }
}
